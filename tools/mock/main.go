// ArcGate 离线验收模拟器：从零构建并驱动真实 arcgate 二进制跑通全链路验收。
//
// 用法（在仓库根目录执行）：
//
//	go run ./tools/mock
//
// 流程：把根目录 arcgate 构建到临时目录 → 启动 mock 站点与 mock NapCat →
// 启动被测进程并按场景注入 OneBot 11 退群事件 → 断言站点请求、NapCat 播报、
// pending 队列文件与日志内容 → 按序打印 [PASS]/[FAIL] 清单，任一 FAIL 退出码 1。
// 全程离线，只访问 127.0.0.1，只使用 Go 标准库。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// 常量
// ============================================================================

const (
	botToken      = "test-token-123" // 注入被测进程的站点令牌（OBSIDIAN_ARC_BOT_TOKEN）
	napcatToken   = "napcat-token"   // Phase C 的 OneBot 访问令牌（ARC_ONEBOT_ACCESS_TOKEN）
	healthTimeout = 5 * time.Second  // /healthz 轮询总超时
	waitShort     = 6 * time.Second  // 一般播报/站点调用等待
	waitLong      = 8 * time.Second  // 重试/排水类场景等待
)

// managedEnvKeys：注入被测进程前先从父环境清除的键，避免外部环境污染串场。
var managedEnvKeys = []string{
	"OBSIDIAN_ARC_URL", "OBSIDIAN_ARC_BOT_TOKEN", "ARC_LISTEN_ADDR",
	"ARC_ONEBOT_API_URL", "ARC_ONEBOT_ACCESS_TOKEN", "OBSIDIAN_ARC_WATCH_GROUPS",
	"ARC_HTTP_TIMEOUT_MS", "ARC_RETRY_BACKOFF_MS", "ARC_RATE_LIMIT_WAIT_MS",
	"ARC_DRAIN_INTERVAL_MS", "ARC_DRAIN_GAP_MS", "ARC_LOG_DIR", "ARC_PENDING_DIR",
	"ARC_LOG_LEVEL",
}

// ============================================================================
// 结果收集与汇总输出
// ============================================================================

type checkResult struct {
	name   string
	ok     bool
	detail string
}

type checker struct {
	mu    sync.Mutex
	items []checkResult
}

func (c *checker) check(name string, ok bool, detail string) {
	c.mu.Lock()
	c.items = append(c.items, checkResult{name: name, ok: ok, detail: detail})
	c.mu.Unlock()
	if !ok {
		// 实时进度走 stderr，正式清单最后统一打到 stdout。
		fmt.Fprintf(os.Stderr, "[FAIL] %s — %s\n", name, trunc(detail, 400))
	}
}

func (c *checker) failCount() int {
	n := 0
	for _, it := range c.items {
		if !it.ok {
			n++
		}
	}
	return n
}

func (c *checker) printSummary() {
	pass := 0
	for _, it := range c.items {
		if it.ok {
			pass++
			fmt.Printf("[PASS] %s\n", it.name)
		} else {
			d := it.detail
			if d == "" {
				d = "(无明细)"
			}
			fmt.Printf("[FAIL] %s — %s\n", it.name, trunc(d, 600))
		}
	}
	fmt.Printf("PASS %d/%d\n", pass, len(c.items))
}

func trunc(s string, n int) string {
	s = strings.ReplaceAll(s, "\r", "")
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

func mark(name string) { fmt.Fprintln(os.Stderr, "--- "+name) }

// ============================================================================
// 通用工具
// ============================================================================

// waitFor 每 100ms 轮询一次 cond，直到成立或超时。
func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// pickPort 用 net.Listen("tcp","127.0.0.1:0") 取一个空闲端口后立即关闭。
func pickPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

// readNonEmptyLines 读文件的非空行；文件不存在时 exists=false。
func readNonEmptyLines(path string) (lines []string, exists bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	for _, ln := range strings.Split(string(b), "\n") {
		ln = strings.TrimSpace(ln)
		if ln != "" {
			lines = append(lines, ln)
		}
	}
	return lines, true
}

// dirContainsToken 递归扫描目录下所有文件是否包含 token（用于令牌不落日志断言）。
func dirContainsToken(dir, token string) (bool, string) {
	found := false
	where := ""
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		if strings.Contains(string(b), token) {
			found = true
			where = path
		}
		return nil
	})
	return found, where
}

// syncBuf 并发安全的输出缓冲（被测进程 stdout/stderr 合流写入）。
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// ============================================================================
// mock 站点：POST /api/bot/departure，行为由可热切换的 handler 决定
// ============================================================================

type siteCall struct {
	at     time.Time
	path   string
	auth   string
	ctype  string
	origin string
	body   string
}

func (c siteCall) qq() string {
	var m struct {
		QQ string `json:"qq"`
	}
	_ = json.Unmarshal([]byte(c.body), &m)
	return m.QQ
}

type mockSite struct {
	mu      sync.Mutex
	handler func(http.ResponseWriter, *http.Request)
	calls   []siteCall
}

func (s *mockSite) setHandler(h func(http.ResponseWriter, *http.Request)) {
	s.mu.Lock()
	s.handler = h
	s.mu.Unlock()
}

func (s *mockSite) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	call := siteCall{
		at:     time.Now(),
		path:   r.URL.Path,
		auth:   r.Header.Get("Authorization"),
		ctype:  r.Header.Get("Content-Type"),
		origin: r.Header.Get("Origin"),
		body:   string(raw),
	}
	s.mu.Lock()
	s.calls = append(s.calls, call)
	h := s.handler
	s.mu.Unlock()
	// 把已读出的 body 放回去，方便场景 handler 二次解析。
	r.Body = io.NopCloser(bytes.NewReader(raw))
	if h == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	h(w, r)
}

// callsFor 返回请求体 qq 字段等于给定值的全部调用（各场景 qq 互不重叠）。
func (s *mockSite) callsFor(qq string) []siteCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []siteCall
	for _, c := range s.calls {
		if c.qq() == qq {
			out = append(out, c)
		}
	}
	return out
}

// statusHandler 固定 status 的站点响应；result 按契约带全字段（含隐私字段，用于 S9）。
func statusHandler(status string, revoked int) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var m map[string]any
		_ = json.NewDecoder(r.Body).Decode(&m)
		qq, _ := m["qq"].(string)
		if qq == "" {
			qq = "0"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": status,
			"result": map[string]any{
				"username":      "siteuser",
				"user_id":       "u-777",
				"qq":            qq,
				"inviter_id":    "u-1",
				"mode":          "disable",
				"cards_due":     3,
				"cards_revoked": revoked,
			},
		})
	}
}

// hijackHandler 接受请求后立即裸关连接，模拟网络故障（客户端表现为传输错误）。
func hijackHandler(w http.ResponseWriter, _ *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_ = conn.Close()
}

// ============================================================================
// mock NapCat：/get_group_member_info、/get_stranger_info、/send_group_msg
// ============================================================================

type napName struct {
	card     string
	nickname string
}

type napMsg struct {
	group int64
	text  string
	auth  string
	at    time.Time
}

type napReq struct {
	path string
	auth string
	at   time.Time
}

type mockNapcat struct {
	mu    sync.Mutex
	names map[int64]napName
	msgs  []napMsg // send_group_msg 记录 (group_id, text, Authorization)
	reqs  []napReq // 全部请求记录（path + Authorization）
}

func newMockNapcat() *mockNapcat {
	return &mockNapcat{names: map[int64]napName{
		10001: {card: "唐三", nickname: "唐三本体"},
		10007: {card: "", nickname: "糖三"}, // card 为空 → 被测方应回退用 nickname
	}}
}

// lookup：表中没有的用户统一返回 card=唐三（满足 S5/S6 场景的精确文案断言）。
func (n *mockNapcat) lookup(id int64) napName {
	if v, ok := n.names[id]; ok {
		return v
	}
	return napName{card: "唐三", nickname: "唐三本体"}
}

func (n *mockNapcat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	n.mu.Lock()
	n.reqs = append(n.reqs, napReq{path: r.URL.Path, auth: r.Header.Get("Authorization"), at: time.Now()})
	n.mu.Unlock()

	switch r.URL.Path {
	case "/get_group_member_info":
		var req struct {
			GroupID float64 `json:"group_id"`
			UserID  float64 `json:"user_id"`
		}
		_ = json.Unmarshal(raw, &req)
		name := n.lookup(int64(req.UserID))
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"retcode": 0,
			"data":    map[string]any{"nickname": name.nickname, "card": name.card},
		})
	case "/get_stranger_info":
		var req struct {
			UserID float64 `json:"user_id"`
		}
		_ = json.Unmarshal(raw, &req)
		name := n.lookup(int64(req.UserID))
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"retcode": 0,
			"data":    map[string]any{"nickname": name.nickname},
		})
	case "/send_group_msg":
		var req struct {
			GroupID float64         `json:"group_id"`
			Message json.RawMessage `json:"message"`
		}
		_ = json.Unmarshal(raw, &req)
		text := ""
		var segs []struct {
			Type string `json:"type"`
			Data struct {
				Text string `json:"text"`
			} `json:"data"`
		}
		if err := json.Unmarshal(req.Message, &segs); err == nil {
			var parts []string
			for _, sg := range segs {
				if sg.Type == "text" {
					parts = append(parts, sg.Data.Text)
				}
			}
			text = strings.Join(parts, "")
		} else {
			var s string
			if json.Unmarshal(req.Message, &s) == nil {
				text = s
			}
		}
		n.mu.Lock()
		n.msgs = append(n.msgs, napMsg{
			group: int64(req.GroupID),
			text:  text,
			auth:  r.Header.Get("Authorization"),
			at:    time.Now(),
		})
		n.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"retcode": 0,
			"data":    map[string]any{"message_id": 1},
		})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (n *mockNapcat) msgsFor(group int64) []napMsg {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []napMsg
	for _, m := range n.msgs {
		if m.group == group {
			out = append(out, m)
		}
	}
	return out
}

func (n *mockNapcat) allMsgs() []napMsg {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]napMsg, len(n.msgs))
	copy(out, n.msgs)
	return out
}

func (n *mockNapcat) reqCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.reqs)
}

func (n *mockNapcat) reqsFrom(idx int) []napReq {
	n.mu.Lock()
	defer n.mu.Unlock()
	if idx > len(n.reqs) {
		return nil
	}
	out := make([]napReq, len(n.reqs)-idx)
	copy(out, n.reqs[idx:])
	return out
}

// ============================================================================
// 事件注入与被测进程管理
// ============================================================================

// obEvent OneBot 11 退群通知事件（数字字段）。
type obEvent struct {
	PostType   string `json:"post_type"`
	SelfID     int64  `json:"self_id"`
	Time       int64  `json:"time"`
	NoticeType string `json:"notice_type"`
	SubType    string `json:"sub_type"`
	GroupID    int64  `json:"group_id"`
	OperatorID int64  `json:"operator_id"`
	UserID     int64  `json:"user_id"`
}

func makeEvent(group, user int64, sub string) obEvent {
	return obEvent{
		PostType:   "notice",
		SelfID:     11111,
		Time:       time.Now().Unix(),
		NoticeType: "group_decrease",
		SubType:    sub,
		GroupID:    group,
		OperatorID: group,
		UserID:     user,
	}
}

var injHTTP = &http.Client{Timeout: 5 * time.Second}

// injectEvent 向被测进程上报端口任意路径 POST 事件 JSON，返回状态码与响应体。
func injectEvent(listen string, ev obEvent, headers map[string]string) (int, string, error) {
	b, err := json.Marshal(ev)
	if err != nil {
		return 0, "", err
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+listen+"/onebot/event", bytes.NewReader(b))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := injHTTP.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return resp.StatusCode, string(body), nil
}

type botConfig struct {
	exe         string
	listen      string // ARC_LISTEN_ADDR
	siteURL     string // OBSIDIAN_ARC_URL
	napURL      string // ARC_ONEBOT_API_URL
	logDir      string // ARC_LOG_DIR
	pendingDir  string // ARC_PENDING_DIR
	watchGroups string // OBSIDIAN_ARC_WATCH_GROUPS（可空）
	onebotToken string // ARC_ONEBOT_ACCESS_TOKEN（可空）
}

type botProc struct {
	cmd  *exec.Cmd
	out  *syncBuf
	done chan struct{}
}

// kill 终止被测进程并等待 Wait 回收（Windows 下删临时目录前必须做）。
func (b *botProc) kill() {
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
	}
}

// startBot 启动被测二进制：清掉父环境里的相关键后注入固定 env，stdout+stderr 收进 buffer。
func startBot(cfg botConfig) (*botProc, error) {
	env := make([]string, 0, len(os.Environ())+16)
	for _, kv := range os.Environ() {
		skip := false
		for _, k := range managedEnvKeys {
			if strings.HasPrefix(kv, k+"=") {
				skip = true
				break
			}
		}
		if !skip {
			env = append(env, kv)
		}
	}
	add := func(k, v string) {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	add("OBSIDIAN_ARC_URL", cfg.siteURL)
	add("OBSIDIAN_ARC_BOT_TOKEN", botToken)
	add("ARC_LISTEN_ADDR", cfg.listen)
	add("ARC_ONEBOT_API_URL", cfg.napURL)
	add("ARC_ONEBOT_ACCESS_TOKEN", cfg.onebotToken)
	add("OBSIDIAN_ARC_WATCH_GROUPS", cfg.watchGroups)
	add("ARC_HTTP_TIMEOUT_MS", "2000")
	add("ARC_RETRY_BACKOFF_MS", "200")
	add("ARC_RATE_LIMIT_WAIT_MS", "400")
	add("ARC_DRAIN_INTERVAL_MS", "1000")
	add("ARC_DRAIN_GAP_MS", "100")
	add("ARC_LOG_DIR", cfg.logDir)
	add("ARC_PENDING_DIR", cfg.pendingDir)
	add("ARC_LOG_LEVEL", "debug")

	cmd := exec.Command(cfg.exe)
	cmd.Env = env
	out := &syncBuf{}
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &botProc{cmd: cmd, out: out, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

var healthClient = &http.Client{Timeout: 1500 * time.Millisecond}

// waitHealth 轮询被测进程 /healthz 直到 200 或超时；进程提前退出立即报错。
func waitHealth(p *botProc, listen string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		resp, err := healthClient.Get("http://" + listen + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-p.done:
			return fmt.Errorf("被测进程提前退出，输出：%s", trunc(p.out.String(), 400))
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("健康检查超时（%s），最近输出：%s", timeout, trunc(p.out.String(), 400))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// buildBot 在仓库根目录执行 go build -o <exe> . 构建被测程序。
func buildBot(exe string) (string, error) {
	cmd := exec.Command("go", "build", "-o", exe, ".")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return buf.String(), err
}

// ============================================================================
// 场景上下文
// ============================================================================

type harness struct {
	c       *checker
	site    *mockSite
	nap     *mockNapcat
	exe     string
	siteURL string
	napURL  string
}

// ============================================================================
// Phase A 场景（一次启动跑完 S1-S10）
// ============================================================================

func phaseA(h *harness, listen, logDir, pendDir string) {
	mark("Phase A 启动")
	_ = os.MkdirAll(logDir, 0o755)
	_ = os.MkdirAll(pendDir, 0o755)
	bot, err := startBot(botConfig{
		exe: h.exe, listen: listen, siteURL: h.siteURL, napURL: h.napURL,
		logDir: logDir, pendingDir: pendDir,
	})
	if err != nil {
		h.c.check("Phase A 启动被测进程", false, err.Error())
		return
	}
	defer bot.kill()
	if err := waitHealth(bot, listen, healthTimeout); err != nil {
		h.c.check("Phase A 健康检查 /healthz", false, err.Error())
		return
	}
	h.c.check("Phase A 启动与健康检查", true, "")

	s1(h, listen)
	silentScenario(h, listen, "S2 already_departed", 1002, 10002, "already_departed")
	silentScenario(h, listen, "S3 unknown", 1003, 10003, "unknown")
	s4(h, listen)
	s5(h, listen)
	s6(h, listen, pendDir)
	s7(h, listen)
	s8(h, listen)
	s9(h)
	s10(h, bot, logDir, pendDir)
}

// s1 正确请求与播报：退群（leave）→ 站点 departed + cards_revoked=2 → 精确文案。
func s1(h *harness, listen string) {
	mark("S1 正确请求与播报")
	h.site.setHandler(statusHandler("departed", 2))
	code, _, err := injectEvent(listen, makeEvent(1001, 10001, "leave"), nil)
	if err != nil {
		h.c.check("S1 注入事件", false, err.Error())
		return
	}
	h.c.check("S1 上报入口秒回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))

	ok := waitFor(func() bool { return len(h.site.callsFor("10001")) >= 1 }, waitShort)
	h.c.check("S1 站点被调用", ok, "6s 内未收到 /api/bot/departure 调用")
	time.Sleep(800 * time.Millisecond)
	calls := h.site.callsFor("10001")
	h.c.check("S1 站点恰被调用 1 次", len(calls) == 1, fmt.Sprintf("实际 %d 次", len(calls)))
	if len(calls) == 0 {
		return
	}
	c := calls[0]
	h.c.check("S1 请求路径为 /api/bot/departure", c.path == "/api/bot/departure", "实际 "+c.path)
	h.c.check("S1 Authorization 为 Bearer 站点令牌", c.auth == "Bearer "+botToken, "实际 "+c.auth)
	h.c.check("S1 Content-Type 含 application/json", strings.Contains(c.ctype, "application/json"), "实际 "+c.ctype)
	h.c.check("S1 无 Origin 头", c.origin == "", "实际 "+c.origin)
	var body map[string]any
	jerr := json.Unmarshal([]byte(c.body), &body)
	_, hasMode := body["mode"]
	h.c.check(`S1 请求体严格 {"qq":"10001"}`, jerr == nil && len(body) == 1 && body["qq"] == "10001", "实际 "+c.body)
	h.c.check("S1 请求体无 mode 字段", jerr == nil && !hasMode, "实际 "+c.body)

	msgOK := waitFor(func() bool { return len(h.nap.msgsFor(1001)) >= 1 }, waitShort)
	h.c.check("S1 NapCat 收到群 1001 播报", msgOK, "6s 内未收到 send_group_msg")
	if msgs := h.nap.msgsFor(1001); len(msgs) > 0 {
		want := "「唐三」 已退群，已收回 2 张重置卡"
		h.c.check("S1 播报文案精确", msgs[0].text == want, "实际 "+msgs[0].text)
	}
}

// silentScenario 静默类场景：站点返回指定 status，断言只调用一次且不播报。
func silentScenario(h *harness, listen, tag string, group, user int64, status string) {
	mark(tag + " 静默")
	h.site.setHandler(statusHandler(status, 0))
	qq := fmt.Sprint(user)
	code, _, err := injectEvent(listen, makeEvent(group, user, "leave"), nil)
	if err != nil {
		h.c.check(tag+" 注入事件", false, err.Error())
		return
	}
	h.c.check(tag+" 上报入口回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))
	ok := waitFor(func() bool { return len(h.site.callsFor(qq)) >= 1 }, waitShort)
	h.c.check(tag+" 站点被调用", ok, "6s 内未收到 /api/bot/departure 调用")
	time.Sleep(800 * time.Millisecond)
	calls := h.site.callsFor(qq)
	h.c.check(tag+" 站点恰被调用 1 次", len(calls) == 1, fmt.Sprintf("实际 %d 次", len(calls)))
	h.c.check(tag+" 静默（无群播报）", len(h.nap.msgsFor(group)) == 0, fmt.Sprintf("实际 %d 条", len(h.nap.msgsFor(group))))
}

// s4 refused：必须播报固定文案。
func s4(h *harness, listen string) {
	mark("S4 refused 播报")
	h.site.setHandler(statusHandler("refused", 0))
	code, _, err := injectEvent(listen, makeEvent(1004, 10004, "leave"), nil)
	if err != nil {
		h.c.check("S4 注入事件", false, err.Error())
		return
	}
	h.c.check("S4 上报入口回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))
	ok := waitFor(func() bool { return len(h.nap.msgsFor(1004)) >= 1 }, waitShort)
	h.c.check("S4 群 1004 收到播报", ok, "6s 内未收到 send_group_msg")
	if msgs := h.nap.msgsFor(1004); len(msgs) > 0 {
		want := "该账号需管理员在站点后台处理"
		h.c.check("S4 播报文案精确", msgs[0].text == want, "实际 "+msgs[0].text)
	}
	time.Sleep(500 * time.Millisecond)
	h.c.check("S4 站点恰被调用 1 次", len(h.site.callsFor("10004")) == 1, fmt.Sprintf("实际 %d 次", len(h.site.callsFor("10004"))))
}

// s5 429 限流：前两次 429，第三次成功；断言调用次数、退避间隔与最终文案。
func s5(h *harness, listen string) {
	mark("S5 429 限流退避重试")
	var n int32
	h.site.setHandler(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]any
		_ = json.NewDecoder(r.Body).Decode(&m)
		if qq, _ := m["qq"].(string); qq == "10005" {
			if atomic.AddInt32(&n, 1) <= 2 {
				writeJSON(w, http.StatusTooManyRequests, map[string]any{
					"error": map[string]any{"code": "rate_limited", "message": "请求过于频繁，请稍后再试"},
				})
				return
			}
		}
		statusHandler("departed", 0)(w, r)
	})
	code, _, err := injectEvent(listen, makeEvent(1005, 10005, "leave"), nil)
	if err != nil {
		h.c.check("S5 注入事件", false, err.Error())
		return
	}
	h.c.check("S5 上报入口回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))

	msgOK := waitFor(func() bool { return len(h.nap.msgsFor(1005)) >= 1 }, waitShort)
	h.c.check("S5 两次 429 后重试成功并播报", msgOK, "6s 内未收到群 1005 播报")
	time.Sleep(800 * time.Millisecond)
	calls := h.site.callsFor("10005")
	h.c.check("S5 站点共被调 3 次", len(calls) == 3, fmt.Sprintf("实际 %d 次", len(calls)))
	if len(calls) >= 2 {
		gap := calls[1].at.Sub(calls[0].at)
		h.c.check("S5 第 1→2 次间隔 ≥350ms（限流等待 400ms）", gap >= 350*time.Millisecond,
			fmt.Sprintf("实际 %dms", gap.Milliseconds()))
	}
	if msgs := h.nap.msgsFor(1005); len(msgs) > 0 {
		want := "「唐三」 已退群"
		h.c.check("S5 播报文案精确（cards_revoked=0 无追加）", msgs[0].text == want, "实际 "+msgs[0].text)
	}
}

// s6 网络故障（裸关连接）：4 次退避重试 → 入队 pending.jsonl → 切回正常 handler → 排水成功。
//
// 说明：pending 入队后排水协程每 ARC_DRAIN_INTERVAL_MS 就会重报一次，因此在失败
// handler 期间"调用数稳定在 4"与排水行为互斥；本场景改为断言前 4 次调用的退避
// 间隔正确（即报告器自身按 1+3 次后退避停止），随后验证排水重报、清队与播报。
func s6(h *harness, listen, pendDir string) {
	mark("S6 网络故障重试+入队+排水")
	pendPath := filepath.Join(pendDir, "pending.jsonl")
	h.site.setHandler(hijackHandler)
	code, _, err := injectEvent(listen, makeEvent(1006, 10006, "leave"), nil)
	if err != nil {
		h.c.check("S6 注入事件", false, err.Error())
		return
	}
	h.c.check("S6 上报入口回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))

	ok := waitFor(func() bool { return len(h.site.callsFor("10006")) >= 4 }, waitLong)
	h.c.check("S6 站点被调 4 次（1 初始+3 重试）", ok,
		fmt.Sprintf("8s 内实际 %d 次", len(h.site.callsFor("10006"))))
	calls := h.site.callsFor("10006")
	if len(calls) >= 4 {
		// 前 4 次一定是报告器自身的尝试（pending 在第 4 次失败后才写入，排水只会更晚）。
		first4 := calls[:4]
		g1 := first4[1].at.Sub(first4[0].at)
		g2 := first4[2].at.Sub(first4[1].at)
		g3 := first4[3].at.Sub(first4[2].at)
		h.c.check("S6 重试间隔 ≥150ms（退避 200ms）", g1 >= 150*time.Millisecond, fmt.Sprintf("实际 %dms", g1.Milliseconds()))
		h.c.check("S6 重试间隔 ≥450ms（退避 500ms）", g2 >= 450*time.Millisecond, fmt.Sprintf("实际 %dms", g2.Milliseconds()))
		h.c.check("S6 重试间隔 ≥950ms（退避 1000ms）", g3 >= 950*time.Millisecond, fmt.Sprintf("实际 %dms", g3.Milliseconds()))
	} else {
		h.c.check("S6 重试间隔断言", false, fmt.Sprintf("调用不足 4 次（%d），无法测量间隔", len(calls)))
	}

	pendOK := waitFor(func() bool {
		lines, exists := readNonEmptyLines(pendPath)
		if !exists || len(lines) == 0 {
			return false
		}
		for _, ln := range lines {
			if strings.Contains(ln, "10006") {
				return true
			}
		}
		return false
	}, 5*time.Second)
	h.c.check("S6 pending.jsonl 入队且含 10006", pendOK, "5s 内未出现含 10006 的非空行")

	// 切成正常 handler，等待排水协程重报成功。
	before := len(h.site.callsFor("10006"))
	h.site.setHandler(statusHandler("departed", 0))
	drainOK := waitFor(func() bool { return len(h.site.callsFor("10006")) > before }, waitLong)
	h.c.check("S6 排水重报：站点再次收到 qq=10006", drainOK,
		fmt.Sprintf("8s 内调用数未从 %d 增加", before))
	emptyOK := waitFor(func() bool {
		lines, _ := readNonEmptyLines(pendPath)
		return len(lines) == 0
	}, waitLong)
	h.c.check("S6 pending.jsonl 已清空", emptyOK, "8s 后仍有非空行")
	msgOK := waitFor(func() bool { return len(h.nap.msgsFor(1006)) >= 1 }, waitShort)
	h.c.check("S6 排水后群 1006 收到播报", msgOK, "6s 内未收到 send_group_msg")
	if msgs := h.nap.msgsFor(1006); len(msgs) > 0 {
		want := "「唐三」 已退群"
		h.c.check("S6 播报文案精确", msgs[0].text == want, "实际 "+msgs[0].text)
	}
}

// s7 kick 文案 + 昵称回退：NapCat card 为空 → 用 nickname，kick 用"已被移出群"。
func s7(h *harness, listen string) {
	mark("S7 kick 文案与昵称回退")
	h.site.setHandler(statusHandler("departed", 1))
	code, _, err := injectEvent(listen, makeEvent(1007, 10007, "kick"), nil)
	if err != nil {
		h.c.check("S7 注入事件", false, err.Error())
		return
	}
	h.c.check("S7 上报入口回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))
	ok := waitFor(func() bool { return len(h.nap.msgsFor(1007)) >= 1 }, waitShort)
	h.c.check("S7 群 1007 收到播报", ok, "6s 内未收到 send_group_msg")
	if msgs := h.nap.msgsFor(1007); len(msgs) > 0 {
		want := "「糖三」 已被移出群，已收回 1 张重置卡"
		h.c.check("S7 播报文案精确（card 空→昵称回退）", msgs[0].text == want, "实际 "+msgs[0].text)
	}
}

// s8 去重：同群同人连续两次相同事件（隔 300ms）只处理一次。
func s8(h *harness, listen string) {
	mark("S8 重复事件去重")
	h.site.setHandler(statusHandler("departed", 0))
	ev := makeEvent(1008, 10008, "leave")
	code, _, err := injectEvent(listen, ev, nil)
	if err != nil {
		h.c.check("S8 注入事件", false, err.Error())
		return
	}
	h.c.check("S8 首次上报回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))
	time.Sleep(300 * time.Millisecond)
	if _, _, err := injectEvent(listen, ev, nil); err != nil {
		h.c.check("S8 重复注入", false, err.Error())
	}
	ok := waitFor(func() bool { return len(h.site.callsFor("10008")) >= 1 }, waitShort)
	h.c.check("S8 站点被调用", ok, "6s 内未收到调用")
	time.Sleep(1500 * time.Millisecond)
	h.c.check("S8 重复事件站点只调 1 次", len(h.site.callsFor("10008")) == 1,
		fmt.Sprintf("实际 %d 次", len(h.site.callsFor("10008"))))
	h.c.check("S8 群 1008 只播报 1 条", len(h.nap.msgsFor(1008)) == 1,
		fmt.Sprintf("实际 %d 条", len(h.nap.msgsFor(1008))))
}

// s9 隐私（跨 Phase A）：所有播报文本不得包含站点 result 里的内部字段。
func s9(h *harness) {
	mark("S9 播报隐私（跨 Phase A）")
	var bad []string
	for _, m := range h.nap.allMsgs() {
		for _, secret := range []string{"siteuser", "u-777", "u-1"} {
			if strings.Contains(m.text, secret) {
				bad = append(bad, fmt.Sprintf("群 %d 文本含 %q：%q", m.group, secret, m.text))
			}
		}
	}
	h.c.check("S9 播报不含 siteuser/u-777/u-1", len(bad) == 0, strings.Join(bad, "；"))
}

// s10 令牌不落日志（跨 Phase A）：日志目录 + 进程输出 + pending/dead 文件均不含站点令牌。
func s10(h *harness, bot *botProc, logDir, pendDir string) {
	mark("S10 令牌不落日志（跨 Phase A）")
	var where []string
	if ok, p := dirContainsToken(logDir, botToken); ok {
		where = append(where, "日志文件 "+p)
	}
	if ok, p := dirContainsToken(pendDir, botToken); ok {
		where = append(where, "pending/dead 文件 "+p)
	}
	if strings.Contains(bot.out.String(), botToken) {
		where = append(where, "被测进程 stdout/stderr")
	}
	h.c.check("S10 站点令牌不出现在日志/输出/队列文件", len(where) == 0, strings.Join(where, "；"))
}

// ============================================================================
// Phase B：OBSIDIAN_ARC_WATCH_GROUPS 白名单过滤
// ============================================================================

func phaseB(h *harness, listen, logDir, pendDir string) {
	mark("Phase B 启动（WATCH_GROUPS=111）")
	_ = os.MkdirAll(logDir, 0o755)
	_ = os.MkdirAll(pendDir, 0o755)
	bot, err := startBot(botConfig{
		exe: h.exe, listen: listen, siteURL: h.siteURL, napURL: h.napURL,
		logDir: logDir, pendingDir: pendDir, watchGroups: "111",
	})
	if err != nil {
		h.c.check("Phase B 启动被测进程", false, err.Error())
		return
	}
	defer bot.kill()
	if err := waitHealth(bot, listen, healthTimeout); err != nil {
		h.c.check("Phase B 健康检查 /healthz", false, err.Error())
		return
	}
	h.c.check("Phase B 启动与健康检查", true, "")
	h.site.setHandler(statusHandler("departed", 0))

	// B1：未监听群 999 注入后完全静默。
	code, _, err := injectEvent(listen, makeEvent(999, 20001, "leave"), nil)
	if err != nil {
		h.c.check("B1 注入事件", false, err.Error())
	} else {
		h.c.check("B1 上报入口回 204", code == http.StatusNoContent, fmt.Sprintf("得到 %d", code))
	}
	time.Sleep(2 * time.Second)
	h.c.check("B1 未监听群 999 不调站点", len(h.site.callsFor("20001")) == 0,
		fmt.Sprintf("实际 %d 次", len(h.site.callsFor("20001"))))
	h.c.check("B1 未监听群 999 无播报", len(h.nap.msgsFor(999)) == 0,
		fmt.Sprintf("实际 %d 条", len(h.nap.msgsFor(999))))

	// B2：监听群 111 正常走全流程。
	_, _, err = injectEvent(listen, makeEvent(111, 20002, "leave"), nil)
	if err != nil {
		h.c.check("B2 注入事件", false, err.Error())
		return
	}
	ok := waitFor(func() bool { return len(h.site.callsFor("20002")) >= 1 }, waitShort)
	time.Sleep(800 * time.Millisecond)
	h.c.check("B2 监听群 111 站点恰被调 1 次", ok && len(h.site.callsFor("20002")) == 1,
		fmt.Sprintf("实际 %d 次", len(h.site.callsFor("20002"))))
	h.c.check("B2 群 111 收到播报", len(h.nap.msgsFor(111)) >= 1,
		fmt.Sprintf("实际 %d 条", len(h.nap.msgsFor(111))))
}

// ============================================================================
// Phase C：ARC_ONEBOT_ACCESS_TOKEN 上报鉴权（双向）
// ============================================================================

func phaseC(h *harness, listen, logDir, pendDir string) {
	mark("Phase C 启动（ARC_ONEBOT_ACCESS_TOKEN）")
	_ = os.MkdirAll(logDir, 0o755)
	_ = os.MkdirAll(pendDir, 0o755)
	bot, err := startBot(botConfig{
		exe: h.exe, listen: listen, siteURL: h.siteURL, napURL: h.napURL,
		logDir: logDir, pendingDir: pendDir, onebotToken: napcatToken,
	})
	if err != nil {
		h.c.check("Phase C 启动被测进程", false, err.Error())
		return
	}
	defer bot.kill()
	if err := waitHealth(bot, listen, healthTimeout); err != nil {
		h.c.check("Phase C 健康检查 /healthz", false, err.Error())
		return
	}
	h.c.check("Phase C 启动与健康检查", true, "")
	h.site.setHandler(statusHandler("departed", 0))
	napBefore := h.nap.reqCount()

	// C1：无鉴权头 → 上报被拒（非 2xx）且不处理。
	code, body, err := injectEvent(listen, makeEvent(3001, 30001, "leave"), nil)
	if err != nil {
		h.c.check("C1 注入事件（无令牌）", false, err.Error())
	} else {
		h.c.check("C1 无令牌上报被拒（非 2xx）", code < 200 || code >= 300,
			fmt.Sprintf("得到 %d，响应体 %s", code, trunc(body, 120)))
	}
	time.Sleep(1500 * time.Millisecond)
	h.c.check("C1 被拒事件不处理（站点 0 次调用）", len(h.site.callsFor("30001")) == 0,
		fmt.Sprintf("实际 %d 次", len(h.site.callsFor("30001"))))

	// C2：带 Bearer 令牌 → 正常处理。
	_, _, err = injectEvent(listen, makeEvent(3002, 30002, "leave"),
		map[string]string{"Authorization": "Bearer " + napcatToken})
	if err != nil {
		h.c.check("C2 注入事件（带令牌）", false, err.Error())
		return
	}
	ok := waitFor(func() bool { return len(h.site.callsFor("30002")) >= 1 }, waitShort)
	time.Sleep(800 * time.Millisecond)
	h.c.check("C2 带令牌上报处理成功（站点 1 次调用）", ok && len(h.site.callsFor("30002")) == 1,
		fmt.Sprintf("实际 %d 次", len(h.site.callsFor("30002"))))
	h.c.check("C2 群 3002 收到播报", len(h.nap.msgsFor(3002)) >= 1,
		fmt.Sprintf("实际 %d 条", len(h.nap.msgsFor(3002))))

	// C3：本阶段 arcgate 发往 NapCat 的 API 请求全部带 Bearer napcat-token。
	bad := 0
	total := 0
	var offenders []string
	for _, rq := range h.nap.reqsFrom(napBefore) {
		switch rq.path {
		case "/get_group_member_info", "/get_stranger_info", "/send_group_msg":
			total++
			if rq.auth != "Bearer "+napcatToken {
				bad++
				offenders = append(offenders, fmt.Sprintf("%s（Authorization=%q）", rq.path, rq.auth))
			}
		}
	}
	h.c.check("C3 NapCat 请求均带 Bearer napcat-token", total >= 1 && bad == 0,
		fmt.Sprintf("共 %d 个请求，异常 %d 个：%s", total, bad, strings.Join(offenders, "；")))

	// C4：napcat-token 不落日志/输出/队列文件。
	var where []string
	if ok2, p := dirContainsToken(logDir, napcatToken); ok2 {
		where = append(where, "日志文件 "+p)
	}
	if ok2, p := dirContainsToken(pendDir, napcatToken); ok2 {
		where = append(where, "pending/dead 文件 "+p)
	}
	if strings.Contains(bot.out.String(), napcatToken) {
		where = append(where, "被测进程 stdout/stderr")
	}
	h.c.check("C4 napcat-token 不落日志", len(where) == 0, strings.Join(where, "；"))
}

// ============================================================================
// 主流程
// ============================================================================

func main() {
	os.Exit(runMain())
}

func runMain() int {
	fmt.Println("== ArcGate 离线验收模拟器 ==")

	// 前置：必须在仓库根目录（存在 go.mod）。
	if _, err := os.Stat("go.mod"); err != nil {
		fmt.Fprintln(os.Stderr, "错误：当前目录没有 go.mod，请在仓库根目录执行 go run ./tools/mock")
		return 1
	}

	c := &checker{}

	// 临时目录：被测二进制、日志、pending 队列都放这里，结束时整体删除。
	tmp, err := os.MkdirTemp("", "arcgate-mock-*")
	if err != nil {
		c.check("创建临时目录", false, err.Error())
		c.printSummary()
		return 1
	}
	// 注意 Windows：先 kill 进程、关服务，最后才删目录（defer LIFO 顺序已保证）。
	defer os.RemoveAll(tmp)

	// ---- 构建被测二进制 ----
	exe := filepath.Join(tmp, "arcgate-test.exe")
	buildLog, err := buildBot(exe)
	if err != nil {
		c.check("构建被测程序（go build -o arcgate-test.exe .）", false,
			trunc(buildLog+"\n"+err.Error(), 800))
		c.printSummary()
		return 1
	}
	c.check("构建被测程序", true, "")

	// ---- 起 mock 站点 ----
	sitePort, err := pickPort()
	if err != nil {
		c.check("分配 mock 站点端口", false, err.Error())
		c.printSummary()
		return 1
	}
	siteLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", sitePort))
	if err != nil {
		c.check("监听 mock 站点端口", false, err.Error())
		c.printSummary()
		return 1
	}
	site := &mockSite{}
	siteSrv := &http.Server{Handler: site}
	go func() { _ = siteSrv.Serve(siteLn) }()
	defer siteSrv.Close()

	// ---- 起 mock NapCat ----
	napPort, err := pickPort()
	if err != nil {
		c.check("分配 mock NapCat 端口", false, err.Error())
		c.printSummary()
		return 1
	}
	napLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", napPort))
	if err != nil {
		c.check("监听 mock NapCat 端口", false, err.Error())
		c.printSummary()
		return 1
	}
	nap := newMockNapcat()
	napSrv := &http.Server{Handler: nap}
	go func() { _ = napSrv.Serve(napLn) }()
	defer napSrv.Close()

	h := &harness{
		c:       c,
		site:    site,
		nap:     nap,
		exe:     exe,
		siteURL: fmt.Sprintf("http://127.0.0.1:%d", sitePort),
		napURL:  fmt.Sprintf("http://127.0.0.1:%d", napPort),
	}

	// 三个阶段各用一个上报端口，避免端口复用串扰。
	listenA, err := pickPort()
	if err != nil {
		c.check("分配上报端口 A", false, err.Error())
		c.printSummary()
		return 1
	}
	listenB, err := pickPort()
	if err != nil {
		c.check("分配上报端口 B", false, err.Error())
		c.printSummary()
		return 1
	}
	listenC, err := pickPort()
	if err != nil {
		c.check("分配上报端口 C", false, err.Error())
		c.printSummary()
		return 1
	}

	// Phase A：一次启动跑 S1-S10。
	phaseA(h, fmt.Sprintf("127.0.0.1:%d", listenA),
		filepath.Join(tmp, "logs"), filepath.Join(tmp, "data"))
	// Phase B：重启，校验 OBSIDIAN_ARC_WATCH_GROUPS 白名单。
	phaseB(h, fmt.Sprintf("127.0.0.1:%d", listenB),
		filepath.Join(tmp, "logs-b"), filepath.Join(tmp, "data-b"))
	// Phase C：重启，校验 ARC_ONEBOT_ACCESS_TOKEN 双向鉴权。
	phaseC(h, fmt.Sprintf("127.0.0.1:%d", listenC),
		filepath.Join(tmp, "logs-c"), filepath.Join(tmp, "data-c"))

	c.printSummary()
	if c.failCount() > 0 {
		return 1
	}
	return 0
}
