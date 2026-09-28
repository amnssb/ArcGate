// ArcGate —— QQ 群退群上报机器人（OneBot 11 HTTP 上报模式，单文件实现）。
//
// 链路：NapCat（OneBot 11 实现）通过 HTTP POST 把事件上报到本程序内置 HTTP 服务，
// 程序过滤退群事件 → POST 站点 Webhook → 按 status 播报回群（调 NapCat API 发群消息）。
//
// 仅使用 Go 标准库，零第三方依赖。
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// 常量与全局
// ---------------------------------------------------------------------------

const (
	// 站点 Webhook 路径
	webhookPath = "/api/bot/departure"
	// 入站上报请求体上限：1MB
	maxReportBodyBytes = 1 << 20
	// 单个日志文件上限：10MB，写入前 stat 超过即轮转
	logRotateBytes = 10 << 20
	// Webhook 重试上限：初始 1 次 + 最多 3 次重试（共 ≤4 次请求）
	maxWebhookRetries = 3
	// 重试队列条目最大处理次数，达到即移入死信 dead.jsonl
	maxQueueTries = 5
)

// 网络类失败的退避阶梯：ARC_RETRY_BACKOFF_MS × 因子（默认 2s/5s/10s，越界取最大）。
// 429/网络失败重试是安全的：站点按 QQ 幂等处理，不会重复收卡。
var backoffFactors = []float64{1, 2.5, 5}

// QQ 号格式：5-15 位、首位非 0
var qqPattern = regexp.MustCompile(`^[1-9][0-9]{4,14}$`)

// 日志级别
const (
	levelDebug = iota
	levelInfo
	levelWarn
	levelError
)

var levelNames = [...]string{"DEBUG", "INFO", "WARN", "ERROR"}

// ---------------------------------------------------------------------------
// 类型
// ---------------------------------------------------------------------------

// config 汇总全部环境变量配置。
type config struct {
	ArcURL        string         // 站点基址（使用时已去尾部 /）
	BotToken      string         // 站点 Webhook 令牌（绝不写入日志）
	WatchGroups   map[int64]bool // 群白名单；空 = 监听所有群
	ListenAddr    string         // 接收上报的监听地址
	OnebotAPIURL  string         // NapCat HTTP API 基址（已去尾部 /）
	OnebotToken   string         // NapCat access token（绝不写入日志）
	LogLevel      string
	HTTPTimeout   time.Duration // 所有出站 HTTP 超时
	RetryBackoff  time.Duration // 网络类失败退避基数
	RateLimitWait time.Duration // 收到 429 后的等待时长
	DedupTTL      time.Duration // 去重时间窗
	DrainInterval time.Duration // 重试队列排水周期
	DrainGap      time.Duration // 排水时条目间隔
	LogDir        string
	PendingDir    string
}

// reportEvent 是 OneBot 11 上报事件中本程序关心的字段子集。
type reportEvent struct {
	PostType   string `json:"post_type"`
	NoticeType string `json:"notice_type"`
	SubType    string `json:"sub_type"`
	GroupID    int64  `json:"group_id"`
	UserID     int64  `json:"user_id"`
	OperatorID int64  `json:"operator_id"`
	SelfID     int64  `json:"self_id"`
}

// app 聚合全部运行期依赖。
type app struct {
	cfg          *config
	log          *logger
	httpClient   *http.Client
	dedup        *deduper
	queue        *retryQueue
	dryRun       bool
	queueEnabled bool // 仅服务器模式且非 dry-run 时启用（dry-run 禁用队列读写）
}

func newApp(cfg *config, log *logger, dryRun bool) *app {
	return &app{
		cfg:        cfg,
		log:        log,
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
		dedup:      newDeduper(cfg.DedupTTL),
		queue:      &retryQueue{dir: cfg.PendingDir, log: log},
		dryRun:     dryRun,
	}
}

// ---------------------------------------------------------------------------
// env 加载：先读进程环境，再解析工作目录下 .env 仅填充未设置的键
// ---------------------------------------------------------------------------

// readDotEnv 解析 .env 文件：KEY=VALUE，忽略空行和 # 注释行，
// 去掉两侧空白与成对引号。文件不存在属正常情况。
func readDotEnv(path string) map[string]string {
	out := make(map[string]string)
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if key != "" {
			out[key] = val
		}
	}
	return out
}

// loadConfig 进程环境优先（空值视为未设置），.env 兜底，最后应用内置默认值。
func loadConfig() *config {
	fileEnv := readDotEnv(".env")
	get := func(key string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fileEnv[key]
	}
	def := func(key, def string) string {
		if v := get(key); v != "" {
			return v
		}
		return def
	}
	ms := func(key string, def int64) time.Duration {
		raw := get(key)
		if raw == "" {
			return time.Duration(def) * time.Millisecond
		}
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil || n <= 0 {
			return time.Duration(def) * time.Millisecond
		}
		return time.Duration(n) * time.Millisecond
	}
	minutes := func(key string, def int64) time.Duration {
		raw := get(key)
		if raw == "" {
			return time.Duration(def) * time.Minute
		}
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil || n <= 0 {
			return time.Duration(def) * time.Minute
		}
		return time.Duration(n) * time.Minute
	}
	return &config{
		ArcURL:        strings.TrimRight(strings.TrimSpace(get("OBSIDIAN_ARC_URL")), "/"),
		BotToken:      get("OBSIDIAN_ARC_BOT_TOKEN"),
		WatchGroups:   parseWatchGroups(get("OBSIDIAN_ARC_WATCH_GROUPS")),
		ListenAddr:    def("ARC_LISTEN_ADDR", "127.0.0.1:3002"),
		OnebotAPIURL:  strings.TrimRight(def("ARC_ONEBOT_API_URL", "http://127.0.0.1:3000"), "/"),
		OnebotToken:   get("ARC_ONEBOT_ACCESS_TOKEN"),
		LogLevel:      strings.ToLower(def("ARC_LOG_LEVEL", "info")),
		HTTPTimeout:   ms("ARC_HTTP_TIMEOUT_MS", 10000),
		RetryBackoff:  ms("ARC_RETRY_BACKOFF_MS", 2000),
		RateLimitWait: ms("ARC_RATE_LIMIT_WAIT_MS", 5000),
		DedupTTL:      minutes("ARC_DEDUP_TTL_MINUTES", 15),
		DrainInterval: ms("ARC_DRAIN_INTERVAL_MS", 30000),
		DrainGap:      ms("ARC_DRAIN_GAP_MS", 6000),
		LogDir:        def("ARC_LOG_DIR", "logs"),
		PendingDir:    def("ARC_PENDING_DIR", "data"),
	}
}

// parseWatchGroups 解析逗号分隔的群号白名单；空串 = 监听所有群（返回空 map）。
func parseWatchGroups(s string) map[int64]bool {
	set := make(map[int64]bool)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if n, err := strconv.ParseInt(part, 10, 64); err == nil && n > 0 {
			set[n] = true
		}
	}
	return set
}

// ---------------------------------------------------------------------------
// logger：双写 stdout 与 {ARC_LOG_DIR}/arcgate.log，级别过滤 + 令牌脱敏 + 轮转
// ---------------------------------------------------------------------------

type logger struct {
	mu       sync.Mutex
	minLevel int
	dir      string
	file     *os.File
	secrets  []string // 需要从每行日志中抹掉的敏感串（两类令牌），兜底防线
}

func newLogger(dir, levelName string, secrets ...string) *logger {
	lvl := levelInfo
	switch levelName {
	case "debug":
		lvl = levelDebug
	case "info":
		lvl = levelInfo
	case "warn":
		lvl = levelWarn
	case "error":
		lvl = levelError
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "无法创建日志目录 %s：%v（将只输出到 stdout）\n", dir, err)
		dir = ""
	}
	return &logger{minLevel: lvl, dir: dir, secrets: secrets}
}

func (l *logger) debugf(format string, args ...any) { l.logf(levelDebug, format, args...) }
func (l *logger) infof(format string, args ...any)  { l.logf(levelInfo, format, args...) }
func (l *logger) warnf(format string, args ...any)  { l.logf(levelWarn, format, args...) }
func (l *logger) errorf(format string, args ...any) { l.logf(levelError, format, args...) }

// logf 输出一行：<RFC3339 本地时间> [LEVEL] 消息 key=value ...
// 写入前对整行做令牌替换（出现即替换为 ***），令牌绝不以任何形式进日志。
func (l *logger) logf(level int, format string, args ...any) {
	if level < l.minLevel {
		return
	}
	line := time.Now().Format(time.RFC3339) + " [" + levelNames[level] + "] " + fmt.Sprintf(format, args...)
	for _, s := range l.secrets {
		if s != "" {
			line = strings.ReplaceAll(line, s, "***")
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintln(os.Stdout, line)
	l.writeToFile(line)
}

// writeToFile 追加写日志文件；写入前 stat，超过 10MB 即轮转：
// arcgate.1.log → arcgate.2.log、arcgate.log → arcgate.1.log（丢最旧），共 3 个文件。
// Windows 下直接以 UTF-8 字节写入。
func (l *logger) writeToFile(line string) {
	if l.dir == "" {
		return
	}
	path := filepath.Join(l.dir, "arcgate.log")
	if l.file == nil {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "无法打开日志文件 %s：%v\n", path, err)
			return
		}
		l.file = f
	}
	if st, err := l.file.Stat(); err == nil && st.Size() > logRotateBytes {
		l.rotateLocked()
	}
	if l.file != nil {
		fmt.Fprintln(l.file, line)
	}
}

func (l *logger) rotateLocked() {
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
	p0 := filepath.Join(l.dir, "arcgate.log")
	p1 := filepath.Join(l.dir, "arcgate.1.log")
	p2 := filepath.Join(l.dir, "arcgate.2.log")
	_ = os.Remove(p2) // 丢最旧
	_ = os.Rename(p1, p2)
	_ = os.Rename(p0, p1)
	f, err := os.OpenFile(p0, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "日志轮转后无法重新打开 %s：%v\n", p0, err)
		return
	}
	l.file = f
}

// ---------------------------------------------------------------------------
// 去重器：防 NapCat 重发风暴导致重复上报/播报
// ---------------------------------------------------------------------------

// deduper 以 "groupID:userID" 为键做时间窗去重。
type deduper struct {
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
}

func newDeduper(ttl time.Duration) *deduper {
	return &deduper{seen: make(map[string]time.Time), ttl: ttl}
}

// allow check-and-set：键不存在或已过 TTL 时记录当前时间并放行；否则拒绝。
// 插入时顺带清理过期项，避免 map 无限增长。
func (d *deduper) allow(key string) bool {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, t := range d.seen {
		if now.Sub(t) > d.ttl {
			delete(d.seen, k)
		}
	}
	if t, ok := d.seen[key]; ok && now.Sub(t) <= d.ttl {
		return false
	}
	d.seen[key] = now
	return true
}

// ---------------------------------------------------------------------------
// 重试队列：{ARC_PENDING_DIR}/pending.jsonl（JSONL，一行一条）
// ---------------------------------------------------------------------------

// pendingEntry 是重试队列条目。
type pendingEntry struct {
	QQ       string `json:"qq"`
	GroupID  int64  `json:"group_id"`
	SubType  string `json:"sub_type"`
	Tries    int    `json:"tries"`
	QueuedAt string `json:"queued_at"` // RFC3339
}

// retryQueue 基于 JSONL 文件的本地重试队列，读写都受互斥锁保护。
type retryQueue struct {
	mu  sync.Mutex
	dir string
	log *logger
}

func (q *retryQueue) pendingPath() string { return filepath.Join(q.dir, "pending.jsonl") }
func (q *retryQueue) deadPath() string    { return filepath.Join(q.dir, "dead.jsonl") }

// append 以 O_APPEND|O_CREATE 追加一条到 pending.jsonl（互斥锁保护）。
func (q *retryQueue) append(entry pendingEntry) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.appendLocked(entry, q.pendingPath())
}

// dead 把条目移入死信文件 dead.jsonl（tries 达上限后调用）。
func (q *retryQueue) dead(entry pendingEntry) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.appendLocked(entry, q.deadPath())
}

func (q *retryQueue) appendLocked(entry pendingEntry, path string) error {
	if err := os.MkdirAll(q.dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	_, err = f.Write(line)
	return err
}

// takeAll 持锁读入全部条目并截断文件；排水期间新追加的条目留给下一轮。
func (q *retryQueue) takeAll() []pendingEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	path := q.pendingPath()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil // 文件不存在视为空队列
	}
	if err := os.Truncate(path, 0); err != nil {
		q.log.warnf("重试队列：截断 %s 失败：%v（本轮跳过，条目保留待下轮）", path, err)
		return nil
	}
	var entries []pendingEntry
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e pendingEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			q.log.warnf("重试队列：跳过无法解析的行 %q：%v", line, err)
			continue
		}
		if e.QQ == "" || (e.SubType != "leave" && e.SubType != "kick") {
			q.log.warnf("重试队列：跳过非法条目 qq=%q sub_type=%q", e.QQ, e.SubType)
			continue
		}
		entries = append(entries, e)
	}
	return entries
}

// ---------------------------------------------------------------------------
// 站点 Webhook：POST {OBSIDIAN_ARC_URL}/api/bot/departure（带重试）
// ---------------------------------------------------------------------------

// departureRequest 是站点 Webhook 请求体：永远只带 qq，绝不含 mode 字段。
type departureRequest struct {
	QQ string `json:"qq"`
}

// webhookResult 是站点成功响应的 result 字段（容错解析，缺字段不报错）。
// 注意：username/user_id/inviter_id 属站点侧数据，只解析不使用，绝不写入日志或群消息。
type webhookResult struct {
	UserID       string `json:"user_id"`
	Username     string `json:"username"`
	QQ           string `json:"qq"`
	InviterID    string `json:"inviter_id"`
	Mode         string `json:"mode"`
	CardsDue     int    `json:"cards_due"`
	CardsRevoked int    `json:"cards_revoked"`
}

// webhookResponse 是站点 Webhook 成功响应（2xx）。already_departed/unknown/refused 时 result 通常为 {}。
type webhookResponse struct {
	Status string        `json:"status"`
	Result webhookResult `json:"result"`
}

// webhookError 区分 Webhook 调用失败类型，决定是否重试及等待时长。
type webhookError struct {
	kind        string // network / http_401 / http_400 / http_429 / http_5xx / http_other / exhausted
	retryable   bool
	rateLimited bool // true = 本次失败因 429，等待 ARC_RATE_LIMIT_WAIT_MS
	status      int
	code        string // 站点错误码（400 响应体 error.code）
	message     string // 站点错误描述（400 响应体 error.message）
	cause       error
}

func (e *webhookError) Error() string {
	switch e.kind {
	case "network":
		return fmt.Sprintf("网络错误：%v", e.cause)
	case "http_401":
		return "HTTP 401"
	case "http_400":
		return fmt.Sprintf("HTTP 400 code=%s message=%s", e.code, e.message)
	case "http_429":
		return "HTTP 429（站点限流）"
	case "http_5xx":
		return fmt.Sprintf("HTTP %d（站点服务端错误）", e.status)
	case "exhausted":
		return fmt.Sprintf("重试耗尽：%v", e.cause)
	default:
		return fmt.Sprintf("HTTP %d", e.status)
	}
}

// Unwrap 保留根因，便于上层排查。
func (e *webhookError) Unwrap() error { return e.cause }

// postDeparture 带重试地上报退群事件：
//   - 初始 1 次 + 最多 3 次重试（共 ≤4 次请求）；
//   - 429：每次等待 ARC_RATE_LIMIT_WAIT_MS；
//   - 网络错误/超时/5xx：按 ARC_RETRY_BACKOFF_MS × {1, 2.5, 5} 退避（越界取最大）；
//   - 混合失败时每次可重试失败消耗一个重试名额，等待时长按本次失败类型定。
//
// 429/网络失败重试是安全的（站点按 QQ 幂等，不会重复收卡）；
// unknown/refused/401/400 属业务性结果，不重试。
func (a *app) postDeparture(qq string) (*webhookResponse, error) {
	if a.dryRun {
		a.log.infof("DRY-RUN：POST %s", a.cfg.ArcURL+webhookPath)
		a.log.infof("DRY-RUN：Authorization: Bearer ***")
		a.log.infof("DRY-RUN：Content-Type: application/json")
		a.log.infof("DRY-RUN：体：{\"qq\":\"%s\"}", qq)
		// 合成成功响应：departed + cards_revoked=0
		return &webhookResponse{Status: "departed", Result: webhookResult{CardsRevoked: 0}}, nil
	}
	retries := 0
	for {
		resp, err := a.postDepartureOnce(qq)
		if err == nil {
			return resp, nil
		}
		we, ok := err.(*webhookError)
		if !ok || !we.retryable {
			// 401/400/其他状态：不重试，已在 postDepartureOnce 内按级别记录
			return nil, err
		}
		if retries >= maxWebhookRetries {
			return nil, &webhookError{kind: "exhausted", cause: we}
		}
		var wait time.Duration
		if we.rateLimited {
			wait = a.cfg.RateLimitWait
		} else {
			idx := retries
			if idx >= len(backoffFactors) {
				idx = len(backoffFactors) - 1
			}
			wait = time.Duration(float64(a.cfg.RetryBackoff) * backoffFactors[idx])
		}
		a.log.warnf("Webhook 上报失败（%s），%v 后进行第 %d/%d 次重试 qq=%s", we.Error(), wait, retries+1, maxWebhookRetries, qq)
		time.Sleep(wait)
		retries++
	}
}

// postDepartureOnce 发起一次站点 Webhook 请求。
// 头部只带 Authorization 与 Content-Type，绝不发 Origin（站点同源校验会拒绝带 Origin 的请求）。
func (a *app) postDepartureOnce(qq string) (*webhookResponse, error) {
	body, err := json.Marshal(departureRequest{QQ: qq})
	if err != nil {
		a.log.errorf("Webhook 请求体序列化失败：%v", err)
		return nil, &webhookError{kind: "network", cause: err}
	}
	req, err := http.NewRequest(http.MethodPost, a.cfg.ArcURL+webhookPath, bytes.NewReader(body))
	if err != nil {
		a.log.errorf("Webhook 请求构造失败（检查 OBSIDIAN_ARC_URL）：%v", err)
		return nil, &webhookError{kind: "network", cause: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.cfg.BotToken)
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, &webhookError{kind: "network", retryable: true, cause: err}
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxReportBodyBytes))
	if err != nil {
		return nil, &webhookError{kind: "network", retryable: true, cause: err}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// 容错解析：缺字段不报错；类型不符保持零值，交由上层按未知 status 处理
		var wr webhookResponse
		_ = json.Unmarshal(respBody, &wr)
		return &wr, nil
	}
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		// 401 不重试：令牌问题重试也不会好
		a.log.errorf("Webhook 鉴权失败(401)：令牌无效或站点接口未启用")
		return nil, &webhookError{kind: "http_401", status: resp.StatusCode}
	case resp.StatusCode == http.StatusBadRequest:
		var eb struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(respBody, &eb)
		a.log.warnf("Webhook 请求被拒绝(400)：code=%s message=%s", eb.Error.Code, eb.Error.Message)
		return nil, &webhookError{kind: "http_400", status: resp.StatusCode, code: eb.Error.Code, message: eb.Error.Message}
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, &webhookError{kind: "http_429", retryable: true, rateLimited: true, status: resp.StatusCode}
	case resp.StatusCode >= 500:
		return nil, &webhookError{kind: "http_5xx", retryable: true, status: resp.StatusCode}
	default:
		a.log.warnf("Webhook 返回意外状态码 %d", resp.StatusCode)
		return nil, &webhookError{kind: "http_other", status: resp.StatusCode}
	}
}

// ---------------------------------------------------------------------------
// OneBot 客户端：昵称获取与群消息播报
// ---------------------------------------------------------------------------

// onebotResponse 是 OneBot 11 HTTP API 的通用响应外壳。
type onebotResponse struct {
	Status  string          `json:"status"`
	Retcode int             `json:"retcode"`
	Data    json.RawMessage `json:"data"`
}

// onebotPost 调 NapCat HTTP API。任何失败都以 error 返回，绝不 panic/崩溃。
func (a *app) onebotPost(action string, payload any) (*onebotResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, a.cfg.OnebotAPIURL+action, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.OnebotToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.OnebotToken)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxReportBodyBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var envelope onebotResponse
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("响应 JSON 解析失败：%w", err)
	}
	return &envelope, nil
}

// fetchNickname 只用 QQ 侧数据获取昵称（绝不使用站点返回的 username/user_id）：
//  1. /get_group_member_info：retcode==0 且 card（去空白）非空 → card，否则 nickname 非空 → nickname；
//  2. /get_stranger_info：retcode==0 且 nickname 非空；
//  3. 兜底：QQ 号字符串。
//
// 退群后 1) 可能失败，属预期；NapCat 不可达绝不 panic/崩溃，只 debug 记日志。
func (a *app) fetchNickname(groupID, userID int64) string {
	qq := strconv.FormatInt(userID, 10)
	if resp, err := a.onebotPost("/get_group_member_info", map[string]any{
		"group_id": groupID,
		"user_id":  userID,
		"no_cache": false,
	}); err != nil {
		a.log.debugf("get_group_member_info 失败 user_id=%s group_id=%d：%v", qq, groupID, err)
	} else if resp.Retcode != 0 {
		a.log.debugf("get_group_member_info retcode=%d user_id=%s group_id=%d", resp.Retcode, qq, groupID)
	} else {
		var info struct {
			Nickname string `json:"nickname"`
			Card     string `json:"card"`
		}
		if err := json.Unmarshal(resp.Data, &info); err != nil {
			a.log.debugf("get_group_member_info data 解析失败 user_id=%s：%v", qq, err)
		} else {
			if card := strings.TrimSpace(info.Card); card != "" {
				return card
			}
			if nick := strings.TrimSpace(info.Nickname); nick != "" {
				return nick
			}
		}
	}
	if resp, err := a.onebotPost("/get_stranger_info", map[string]any{"user_id": userID}); err != nil {
		a.log.debugf("get_stranger_info 失败 user_id=%s：%v", qq, err)
	} else if resp.Retcode != 0 {
		a.log.debugf("get_stranger_info retcode=%d user_id=%s", resp.Retcode, qq)
	} else {
		var info struct {
			Nickname string `json:"nickname"`
		}
		if err := json.Unmarshal(resp.Data, &info); err != nil {
			a.log.debugf("get_stranger_info data 解析失败 user_id=%s：%v", qq, err)
		} else if nick := strings.TrimSpace(info.Nickname); nick != "" {
			return nick
		}
	}
	a.log.debugf("昵称获取全部失败，使用 QQ 号兜底 user_id=%s group_id=%d", qq, groupID)
	return qq
}

// sendGroupMsg 通过 NapCat /send_group_msg 发群消息。
// message 用消息段数组（type=text），天然防 CQ 码注入；文案由代码拼接，昵称来自 QQ 侧。
// retcode!=0 记 warn，网络失败记 warn，绝不崩溃。
func (a *app) sendGroupMsg(groupID int64, text string) {
	if a.dryRun {
		a.log.infof("DRY-RUN：send_group_msg group_id=%d 文案=%s", groupID, text)
		return
	}
	resp, err := a.onebotPost("/send_group_msg", map[string]any{
		"group_id": groupID,
		"message": []map[string]any{
			{"type": "text", "data": map[string]any{"text": text}},
		},
	})
	if err != nil {
		a.log.warnf("send_group_msg 请求失败 group_id=%d：%v", groupID, err)
		return
	}
	if resp.Retcode != 0 {
		a.log.warnf("send_group_msg retcode=%d status=%s group_id=%d", resp.Retcode, resp.Status, groupID)
		return
	}
	a.log.infof("播报成功 group_id=%d 文案=%s", groupID, text)
}

// ---------------------------------------------------------------------------
// 事件处理：去重 → 取昵称 → 打 Webhook → 按 status 播报
// ---------------------------------------------------------------------------

// handleEvent 处理一条已通过同步过滤的退群事件（服务器模式下在 goroutine 中异步执行，
// simulate 模式下同步执行）。去重 check-and-set 在处理开始时做，防 NapCat 重发风暴。
func (a *app) handleEvent(ev reportEvent) {
	key := strconv.FormatInt(ev.GroupID, 10) + ":" + strconv.FormatInt(ev.UserID, 10)
	if !a.dedup.allow(key) {
		a.log.debugf("去重命中，忽略重复上报 group_id=%d user_id=%d", ev.GroupID, ev.UserID)
		return
	}
	qq := strconv.FormatInt(ev.UserID, 10)
	a.log.infof("处理退群事件 group_id=%d user_id=%s sub_type=%s", ev.GroupID, qq, ev.SubType)
	nickname := a.fetchNickname(ev.GroupID, ev.UserID)
	resp, err := a.postDeparture(qq)
	if err != nil {
		if we, ok := err.(*webhookError); ok && we.kind == "exhausted" {
			a.log.errorf("Webhook 上报重试耗尽 group_id=%d user_id=%s：%v", ev.GroupID, qq, err)
			if a.queueEnabled {
				if qerr := a.queue.append(pendingEntry{
					QQ:       qq,
					GroupID:  ev.GroupID,
					SubType:  ev.SubType,
					Tries:    0,
					QueuedAt: time.Now().Format(time.RFC3339),
				}); qerr != nil {
					a.log.errorf("写入重试队列失败 group_id=%d user_id=%s：%v", ev.GroupID, qq, qerr)
				} else {
					a.log.warnf("事件已写入重试队列 group_id=%d user_id=%s", ev.GroupID, qq)
				}
			} else {
				a.log.debugf("重试队列未启用（dry-run/simulate），事件不落盘 group_id=%d user_id=%s", ev.GroupID, qq)
			}
		}
		// 非重试类失败（401/400/其他状态）已在 postDepartureOnce 内按级别记录，不重复
		return
	}
	a.announce(ev.GroupID, nickname, ev.SubType, resp)
}

// announce 按站点返回的 status 播报回群。
// 注意：站点返回的 username/user_id/inviter_id 不写日志、不发群消息。
func (a *app) announce(groupID int64, nickname, subType string, resp *webhookResponse) {
	var text string
	switch resp.Status {
	case "departed":
		if subType == "kick" {
			text = fmt.Sprintf("「%s」 已被移出群", nickname)
		} else {
			text = fmt.Sprintf("「%s」 已退群", nickname)
		}
		if resp.Result.CardsRevoked > 0 {
			text += fmt.Sprintf("，已收回 %d 张重置卡", resp.Result.CardsRevoked)
		}
	case "refused":
		text = "该账号需管理员在站点后台处理"
	case "already_departed", "unknown":
		a.log.infof("站点返回 status=%s，静默不播报 group_id=%d", resp.Status, groupID)
		return
	default:
		a.log.warnf("站点返回未知 status=%q，不播报 group_id=%d", resp.Status, groupID)
		return
	}
	a.sendGroupMsg(groupID, text)
}

// ---------------------------------------------------------------------------
// 重试队列排水
// ---------------------------------------------------------------------------

// drainLoop 排水协程：启动时立即跑一次，之后每 ARC_DRAIN_INTERVAL_MS 一轮。
func (a *app) drainLoop() {
	a.drainOnce()
	ticker := time.NewTicker(a.cfg.DrainInterval)
	defer ticker.Stop()
	for range ticker.C {
		a.drainOnce()
	}
}

// drainOnce 排水一轮：持锁读入全部条目并截断文件（排水期间新追加的留给下一轮），
// 逐条重新上报（间隔 ≥ARC_DRAIN_GAP_MS）。
// 拿到任何 2xx 业务 status（含 already_departed/unknown/refused）→ 成功出队（记 info 日志）；
// 仍失败 → tries+1 写回；tries>=5 → 移入 dead.jsonl 死信并 error 日志。
func (a *app) drainOnce() {
	entries := a.queue.takeAll()
	for i, e := range entries {
		if i > 0 {
			time.Sleep(a.cfg.DrainGap)
		}
		resp, err := a.postDeparture(e.QQ)
		if err != nil {
			e.Tries++
			if e.Tries >= maxQueueTries {
				if derr := a.queue.dead(e); derr != nil {
					a.log.errorf("重试队列：写入死信失败 qq=%s group_id=%d：%v", e.QQ, e.GroupID, derr)
				}
				a.log.errorf("重试队列：条目处理 %d 次仍失败，移入死信 dead.jsonl qq=%s group_id=%d sub_type=%s：%v",
					e.Tries, e.QQ, e.GroupID, e.SubType, err)
				continue
			}
			if werr := a.queue.append(e); werr != nil {
				a.log.errorf("重试队列：条目写回失败 qq=%s group_id=%d：%v", e.QQ, e.GroupID, werr)
				continue
			}
			a.log.warnf("重试队列：条目上报失败，tries=%d 已写回 qq=%s group_id=%d sub_type=%s：%v",
				e.Tries, e.QQ, e.GroupID, e.SubType, err)
			continue
		}
		a.log.infof("重试队列：条目出队成功 status=%s qq=%s group_id=%d", resp.Status, e.QQ, e.GroupID)
		// 排水时重新走昵称获取（退群后 get_group_member_info 可能失败 → get_stranger_info → QQ 号兜底）；
		// 播报 sub_type 用条目里存的
		nickname := ""
		if resp.Status == "departed" {
			if uid, perr := strconv.ParseInt(e.QQ, 10, 64); perr == nil {
				nickname = a.fetchNickname(e.GroupID, uid)
			}
		}
		a.announce(e.GroupID, nickname, e.SubType, resp)
	}
}

// ---------------------------------------------------------------------------
// 内置 HTTP 服务：接收 NapCat 上报
// ---------------------------------------------------------------------------

// serveHTTP 路由：POST 任意路径都当作 OneBot 上报处理（NapCat 上报路径可配置，不挑路径）；
// GET /healthz 返回 200 "ok"；其他 GET 404。
func (a *app) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
		return
	}
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	a.handleReport(w, r)
}

// checkOnebotToken 校验入站上报鉴权：Authorization: Bearer <token> 头或 ?access_token= 查询参数。
// 用常量时间比较，避免时序侧信道。
func checkOnebotToken(r *http.Request, token string) bool {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) == 1 {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(r.URL.Query().Get("access_token")), []byte(token)) == 1
}

// handleReport 处理一条 OneBot 上报：鉴权 → 限 1MB → 解析 JSON → 同步过滤 →
// 尽快 204，再 goroutine 异步干活（handleEvent 里才取昵称、打 Webhook，保证上报响应快）。
func (a *app) handleReport(w http.ResponseWriter, r *http.Request) {
	if a.cfg.OnebotToken != "" && !checkOnebotToken(r, a.cfg.OnebotToken) {
		a.log.warnf("上报鉴权失败，返回 403 remote=%s", r.RemoteAddr)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxReportBodyBytes))
	if err != nil {
		a.log.warnf("读取上报体失败（上限 1MB）remote=%s：%v", r.RemoteAddr, err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var ev reportEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		a.log.warnf("上报 JSON 解析失败 remote=%s：%v", r.RemoteAddr, err)
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// ---- 同步过滤：不相关事件一律快速 204 忽略 ----
	if ev.PostType != "notice" {
		a.log.debugf("忽略 post_type=%s", ev.PostType)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if ev.NoticeType != "group_decrease" {
		a.log.debugf("忽略 notice_type=%s（心跳/消息等）", ev.NoticeType)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if ev.SubType != "leave" && ev.SubType != "kick" {
		a.log.debugf("忽略 sub_type=%s group_id=%d user_id=%d", ev.SubType, ev.GroupID, ev.UserID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if ev.UserID == ev.SelfID {
		a.log.debugf("机器人自身变动事件，忽略 group_id=%d user_id=%d", ev.GroupID, ev.UserID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(a.cfg.WatchGroups) > 0 && !a.cfg.WatchGroups[ev.GroupID] {
		a.log.debugf("群号不在白名单，忽略 group_id=%d user_id=%d", ev.GroupID, ev.UserID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !qqPattern.MatchString(strconv.FormatInt(ev.UserID, 10)) {
		a.log.warnf("user_id 不符合 QQ 号格式，忽略 group_id=%d user_id=%d", ev.GroupID, ev.UserID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// 尽快 204，异步处理
	w.WriteHeader(http.StatusNoContent)
	go a.handleEvent(ev)
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

func main() {
	dryRun := flag.Bool("dry-run", false, "演练模式：Webhook 与播报只打日志，禁用重试队列")
	simulate := flag.Bool("simulate", false, "模拟退群事件：-simulate <群号> <QQ号> [leave|kick]")
	flag.Usage = printUsage
	flag.Parse()
	args := flag.Args()

	cfg := loadConfig()
	log := newLogger(cfg.LogDir, cfg.LogLevel, cfg.BotToken, cfg.OnebotToken)

	if *simulate {
		runSimulateMode(cfg, log, *dryRun, args)
		return
	}
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "无法识别的位置参数：%s\n", strings.Join(args, " "))
		printUsage()
		os.Exit(2)
	}
	runServerMode(cfg, log, *dryRun)
}

func printUsage() {
	fmt.Fprint(os.Stderr, `ArcGate —— QQ 群退群上报机器人（OneBot 11 HTTP 上报模式）

用法：
  arcgate
      服务器模式（默认）：接收 NapCat 上报 → 过滤退群事件 → POST 站点 Webhook → 播报回群。
  arcgate -dry-run
      演练模式：可与服务器模式或 -simulate 组合；出站 HTTP 只打日志，禁用重试队列。
  arcgate -simulate <群号> <QQ号> [leave|kick]
      模拟一次退群事件并同步走完整处理流程，完成后退出（sub_type 默认 leave）。

环境变量见 .env.example。
`)
	flag.PrintDefaults()
}

// requireSiteEnv 校验必填环境变量，缺失时打印中文提示引导复制 .env.example，退出码 1。
func requireSiteEnv(cfg *config) {
	var missing []string
	if cfg.ArcURL == "" {
		missing = append(missing, "OBSIDIAN_ARC_URL")
	}
	if cfg.BotToken == "" {
		missing = append(missing, "OBSIDIAN_ARC_BOT_TOKEN")
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr,
			"缺少必填环境变量：%s。\n请复制 .env.example 为 .env 并填写后重试（或在进程环境中直接设置）。\n",
			strings.Join(missing, "、"))
		os.Exit(1)
	}
	if !strings.HasPrefix(cfg.ArcURL, "http://") && !strings.HasPrefix(cfg.ArcURL, "https://") {
		fmt.Fprintf(os.Stderr, "OBSIDIAN_ARC_URL 应为 http(s):// 开头的站点基址，当前为：%s\n", cfg.ArcURL)
		os.Exit(1)
	}
}

// runServerMode 无参数默认模式：校验必填环境变量 → 打印启动配置摘要（令牌只打长度）→
// 启动排水协程 + HTTP 服务 → SIGINT/SIGTERM 优雅退出。
func runServerMode(cfg *config, log *logger, dryRun bool) {
	if !dryRun {
		requireSiteEnv(cfg)
	}
	a := newApp(cfg, log, dryRun)
	a.queueEnabled = !dryRun // dry-run 模式下禁用重试队列读写

	log.infof("ArcGate 启动（模式=服务器 dry-run=%v）", dryRun)
	log.infof("监听地址=%s", cfg.ListenAddr)
	log.infof("OneBot API=%s 入站鉴权=%v OneBot 令牌长度=%d", cfg.OnebotAPIURL, cfg.OnebotToken != "", len(cfg.OnebotToken))
	arcLabel := cfg.ArcURL
	if arcLabel == "" {
		arcLabel = "(未设置)"
	}
	log.infof("站点地址=%s 站点令牌长度=%d（仅记录长度，不记录内容）", arcLabel, len(cfg.BotToken))
	watch := "所有群"
	if len(cfg.WatchGroups) > 0 {
		ids := make([]string, 0, len(cfg.WatchGroups))
		for id := range cfg.WatchGroups {
			ids = append(ids, strconv.FormatInt(id, 10))
		}
		sort.Strings(ids)
		watch = strings.Join(ids, ",")
	}
	log.infof("监听群=%s", watch)
	log.infof("HTTP 超时=%s 退避基数=%s 429 等待=%s 去重 TTL=%s",
		cfg.HTTPTimeout, cfg.RetryBackoff, cfg.RateLimitWait, cfg.DedupTTL)
	log.infof("排水周期=%s 排水间隔=%s 日志目录=%s 队列目录=%s",
		cfg.DrainInterval, cfg.DrainGap, cfg.LogDir, cfg.PendingDir)

	if a.queueEnabled {
		go a.drainLoop()
	} else {
		log.infof("重试队列已禁用（dry-run）")
	}

	srv := &http.Server{Addr: cfg.ListenAddr, Handler: http.HandlerFunc(a.serveHTTP)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.errorf("HTTP 服务异常退出：%v", err)
			os.Exit(1)
		}
	}()
	log.infof("HTTP 服务已监听 %s，等待 NapCat 上报", cfg.ListenAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	log.infof("收到信号 %v，正在优雅退出……", sig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.warnf("HTTP 服务关闭超时或出错：%v", err)
	}
	log.infof("ArcGate 已退出")
}

// runSimulateMode -simulate 模式：构造合成事件同步走完整 handleEvent
// （真实 NapCat 昵称获取、真实 Webhook、真实播报），完成后退出。
// 与 -dry-run 组合时不打任何 HTTP，昵称直接用 QQ 号，打印计划发出的 Webhook 请求（头/体）
// 与两种文案模板及收回卡片时的追加片段。
func runSimulateMode(cfg *config, log *logger, dryRun bool, args []string) {
	if len(args) < 2 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, "用法：arcgate -simulate <群号> <QQ号> [leave|kick]（sub_type 默认 leave）")
		os.Exit(2)
	}
	groupID, err := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64)
	if err != nil || groupID <= 0 {
		fmt.Fprintf(os.Stderr, "群号无效：%s\n用法：arcgate -simulate <群号> <QQ号> [leave|kick]\n", args[0])
		os.Exit(2)
	}
	userID, err := strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QQ 号无效：%s\n用法：arcgate -simulate <群号> <QQ号> [leave|kick]\n", args[1])
		os.Exit(2)
	}
	qq := strconv.FormatInt(userID, 10)
	if !qqPattern.MatchString(qq) {
		fmt.Fprintf(os.Stderr, "QQ 号不符合格式（5-15 位、首位非 0）：%s\n用法：arcgate -simulate <群号> <QQ号> [leave|kick]\n", qq)
		os.Exit(2)
	}
	subType := "leave"
	if len(args) == 3 {
		subType = strings.TrimSpace(args[2])
		if subType != "leave" && subType != "kick" {
			fmt.Fprintf(os.Stderr, "sub_type 只支持 leave 或 kick，收到：%s\n用法：arcgate -simulate <群号> <QQ号> [leave|kick]\n", args[2])
			os.Exit(2)
		}
	}
	if !dryRun {
		// 非演练的 simulate 会真实调用站点 Webhook，必填项同样要校验
		requireSiteEnv(cfg)
	}
	a := newApp(cfg, log, dryRun)
	log.infof("模拟退群事件 group_id=%d user_id=%s sub_type=%s dry-run=%v", groupID, qq, subType, dryRun)
	if dryRun {
		a.logSimulatePlan(qq)
		log.infof("模拟完成（dry-run，未发出任何 HTTP 请求）")
		return
	}
	a.handleEvent(reportEvent{
		PostType:   "notice",
		NoticeType: "group_decrease",
		SubType:    subType,
		GroupID:    groupID,
		UserID:     userID,
		OperatorID: userID,
		SelfID:     0,
	})
	log.infof("模拟完成")
}

// logSimulatePlan dry-run 模拟时打印计划发出的 Webhook 请求与两种文案模板（不打任何 HTTP）。
func (a *app) logSimulatePlan(qq string) {
	a.log.infof("DRY-RUN：昵称直接使用 QQ 号 %s", qq)
	a.log.infof("DRY-RUN：将 POST %s", a.cfg.ArcURL+webhookPath)
	a.log.infof("DRY-RUN：Authorization: Bearer ***")
	a.log.infof("DRY-RUN：Content-Type: application/json")
	a.log.infof("DRY-RUN：体：{\"qq\":\"%s\"}", qq)
	a.log.infof("DRY-RUN：文案模板（leave）：「%s」 已退群", qq)
	a.log.infof("DRY-RUN：文案模板（kick）：「%s」 已被移出群", qq)
	a.log.infof("DRY-RUN：收回卡片时的追加片段：，已收回 1 张重置卡")
}
