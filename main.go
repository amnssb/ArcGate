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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
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
	// 面板最近事件环形容量
	panelRecentCap = 50
	// 程序版本（启动日志与面板展示）
	version = "1.1.0"
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
	ArcURL         string         // 站点基址（使用时已去尾部 /）
	BotToken       string         // 站点 Webhook 令牌（绝不写入日志）
	WatchGroups    map[int64]bool // 群白名单；空 = 监听所有群
	WatchGroupsRaw string         // 白名单原始配置串（面板编辑用）
	ListenAddr     string         // 接收上报的监听地址
	OnebotAPIURL   string         // NapCat HTTP API 基址（已去尾部 /）
	OnebotToken    string         // NapCat access token（绝不写入日志）
	LogLevel       string
	HTTPTimeout    time.Duration // 所有出站 HTTP 超时
	RetryBackoff   time.Duration // 网络类失败退避基数
	RateLimitWait  time.Duration // 收到 429 后的等待时长
	DedupTTL       time.Duration // 去重时间窗
	DrainInterval  time.Duration // 重试队列排水周期
	DrainGap       time.Duration // 排水时条目间隔
	LogDir         string
	PendingDir     string
	PanelUser      string // 面板登录账号（默认 admin）
	PanelPassword  string // 面板登录口令（空 = 未初始化，首次打开面板时自己设置）
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
	stats        *statsCollector
	panel        *panelAuth
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
		stats:      newStatsCollector(),
		panel:      newPanelAuth(cfg.PanelUser, cfg.PanelPassword),
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

// saveDotEnv 把 updates 写入 .env：已有键原位替换（保留行序与注释），缺失键追加到末尾。
// 文件不存在则创建（权限 0600，因为含令牌）。
func saveDotEnv(path string, updates map[string]string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	lines := strings.Split(string(data), "\n")
	remaining := make(map[string]string, len(updates))
	for k, v := range updates {
		remaining[k] = v
	}
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		eq := strings.IndexByte(trimmed, '=')
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(trimmed[:eq])
		if v, ok := remaining[key]; ok {
			lines[i] = key + "=" + v
			delete(remaining, key)
		}
	}
	for k, v := range remaining {
		lines = append(lines, k+"="+v)
	}
	out := strings.TrimRight(strings.Join(lines, "\n"), "\r\n") + "\n"
	if err := os.WriteFile(path, []byte(out), 0o600); err != nil {
		return 0, err
	}
	return len(updates), nil
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
		ArcURL:         strings.TrimRight(strings.TrimSpace(get("OBSIDIAN_ARC_URL")), "/"),
		BotToken:       get("OBSIDIAN_ARC_BOT_TOKEN"),
		WatchGroups:    parseWatchGroups(get("OBSIDIAN_ARC_WATCH_GROUPS")),
		WatchGroupsRaw: strings.TrimSpace(get("OBSIDIAN_ARC_WATCH_GROUPS")),
		ListenAddr:     def("ARC_LISTEN_ADDR", "127.0.0.1:3002"),
		OnebotAPIURL:   strings.TrimRight(def("ARC_ONEBOT_API_URL", "http://127.0.0.1:3000"), "/"),
		OnebotToken:    get("ARC_ONEBOT_ACCESS_TOKEN"),
		LogLevel:       strings.ToLower(def("ARC_LOG_LEVEL", "info")),
		HTTPTimeout:    ms("ARC_HTTP_TIMEOUT_MS", 10000),
		RetryBackoff:   ms("ARC_RETRY_BACKOFF_MS", 2000),
		RateLimitWait:  ms("ARC_RATE_LIMIT_WAIT_MS", 5000),
		DedupTTL:       minutes("ARC_DEDUP_TTL_MINUTES", 15),
		DrainInterval:  ms("ARC_DRAIN_INTERVAL_MS", 30000),
		DrainGap:       ms("ARC_DRAIN_GAP_MS", 6000),
		LogDir:         def("ARC_LOG_DIR", "logs"),
		PendingDir:     def("ARC_PENDING_DIR", "data"),
		PanelUser:      def("ARC_PANEL_USER", "admin"),
		PanelPassword:  get("ARC_PANEL_PASSWORD"),
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

// addSecret 运行期追加脱敏串（如面板初始化时设置的口令）。
func (l *logger) addSecret(s string) {
	if s == "" {
		return
	}
	l.mu.Lock()
	l.secrets = append(l.secrets, s)
	l.mu.Unlock()
}

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
// 统计收集器：内存计数 + 最近事件环，供内置面板展示（进程内数据，重启归零）
// ---------------------------------------------------------------------------

// eventRecord 是面板「最近事件」表的一行。只含 QQ 侧信息（群号/QQ/昵称/播报文案），
// 站点返回的 username/user_id 等账号信息从不进入本结构。
type eventRecord struct {
	Time     string `json:"time"`   // 处理完成时刻（本地，MM-DD HH:MM:SS）
	Source   string `json:"source"` // event=实时上报 / drain=重试队列排水
	GroupID  int64  `json:"group_id"`
	QQ       string `json:"qq"`
	SubType  string `json:"sub_type"`
	Nickname string `json:"nickname"`
	Status   string `json:"status"`  // departed/already_departed/unknown/refused/queued/dead/error:<kind>
	Message  string `json:"message"` // 播报文案或失败说明
}

// statsCollector 汇总运行期计数。所有字段由互斥锁保护，读多写少，量级极小。
type statsCollector struct {
	mu         sync.Mutex
	startedAt  time.Time
	received   int            // 通过同步过滤、进入处理的退群事件数
	ignored    int            // 上报中被过滤忽略的事件数（心跳/无关 notice/白名单外等）
	deduped    int            // 去重命中数
	byStatus   map[string]int // 处理结果分布（含 queued/dead/error:*）
	webhookOK  int            // Webhook 请求成功次数（按请求计，重试成功也计）
	webhookErr int            // Webhook 请求失败次数（按请求计）
	retries    int            // 自动重试总次数
	msgSent    int            // 群播报成功数
	msgFailed  int            // 群播报失败数
	recent     []eventRecord  // 最近事件，新事件在前，容量 panelRecentCap
}

func newStatsCollector() *statsCollector {
	return &statsCollector{startedAt: time.Now(), byStatus: make(map[string]int)}
}

func (s *statsCollector) recordIgnored() {
	s.mu.Lock()
	s.ignored++
	s.mu.Unlock()
}

func (s *statsCollector) recordReceived() {
	s.mu.Lock()
	s.received++
	s.mu.Unlock()
}

func (s *statsCollector) recordDeduped() {
	s.mu.Lock()
	s.deduped++
	s.mu.Unlock()
}

func (s *statsCollector) recordWebhook(ok bool) {
	s.mu.Lock()
	if ok {
		s.webhookOK++
	} else {
		s.webhookErr++
	}
	s.mu.Unlock()
}

func (s *statsCollector) recordRetry() {
	s.mu.Lock()
	s.retries++
	s.mu.Unlock()
}

func (s *statsCollector) recordMsg(ok bool) {
	s.mu.Lock()
	if ok {
		s.msgSent++
	} else {
		s.msgFailed++
	}
	s.mu.Unlock()
}

// recordOutcome 记录一条事件的最终处理结果并推入最近事件环。
func (s *statsCollector) recordOutcome(source string, groupID int64, qq, subType, nickname, status, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byStatus[status]++
	s.recent = append([]eventRecord{{
		Time:     time.Now().Format("01-02 15:04:05"),
		Source:   source,
		GroupID:  groupID,
		QQ:       qq,
		SubType:  subType,
		Nickname: nickname,
		Status:   status,
		Message:  message,
	}}, s.recent...)
	if len(s.recent) > panelRecentCap {
		s.recent = s.recent[:panelRecentCap]
	}
}

// reset 清零全部计数与最近事件（面板「重置统计」用）。
func (s *statsCollector) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received, s.ignored, s.deduped = 0, 0, 0
	s.webhookOK, s.webhookErr, s.retries = 0, 0, 0
	s.msgSent, s.msgFailed = 0, 0
	s.byStatus = make(map[string]int)
	s.recent = nil
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

// depth 返回 pending.jsonl 当前条目数（文件不存在视为 0）。仅供面板展示。
func (q *retryQueue) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.parsePendingFile())
}

// list 返回 pending.jsonl 当前全部条目（不截断，仅供面板展示）。
func (q *retryQueue) list() []pendingEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.parsePendingFile()
}

// clear 清空 pending.jsonl，返回被清除的条目数。
func (q *retryQueue) clear() (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := len(q.parsePendingFile())
	if err := os.Truncate(q.pendingPath(), 0); err != nil {
		return 0, err
	}
	return n, nil
}

// parsePendingFile 读取并解析 pending 文件（调用方需持锁）。文件不存在视为空。
func (q *retryQueue) parsePendingFile() []pendingEntry {
	data, err := os.ReadFile(q.pendingPath())
	if err != nil {
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
			continue
		}
		if e.QQ == "" || (e.SubType != "leave" && e.SubType != "kick") {
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
			a.stats.recordWebhook(true)
			return resp, nil
		}
		a.stats.recordWebhook(false)
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
		a.stats.recordRetry()
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
		a.stats.recordMsg(false)
		return
	}
	if resp.Retcode != 0 {
		a.log.warnf("send_group_msg retcode=%d status=%s group_id=%d", resp.Retcode, resp.Status, groupID)
		a.stats.recordMsg(false)
		return
	}
	a.stats.recordMsg(true)
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
		a.stats.recordDeduped()
		return
	}
	a.processEvent(ev)
}

// processEvent 不带去重地完整处理一条退群事件（昵称→Webhook→播报→统计）。
// 面板手动注入走本方法（便于反复测试同一 QQ），NapCat 路径走 handleEvent。
func (a *app) processEvent(ev reportEvent) {
	qq := strconv.FormatInt(ev.UserID, 10)
	a.log.infof("处理退群事件 group_id=%d user_id=%s sub_type=%s", ev.GroupID, qq, ev.SubType)
	nickname := a.fetchNickname(ev.GroupID, ev.UserID)
	resp, err := a.postDeparture(qq)
	if err != nil {
		we, _ := err.(*webhookError)
		status, msg := "error", err.Error()
		if we != nil {
			status, msg = "error:"+we.kind, we.Error()
		}
		if we != nil && we.kind == "exhausted" {
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
					msg = "重试耗尽且入队失败：" + qerr.Error()
				} else {
					a.log.warnf("事件已写入重试队列 group_id=%d user_id=%s", ev.GroupID, qq)
					status, msg = "queued", "重试耗尽，已写入重试队列待排水"
				}
			} else {
				a.log.debugf("重试队列未启用（dry-run/simulate），事件不落盘 group_id=%d user_id=%s", ev.GroupID, qq)
			}
		}
		// 非重试类失败（401/400/其他状态）已在 postDepartureOnce 内按级别记录，不重复
		a.stats.recordOutcome("event", ev.GroupID, qq, ev.SubType, nickname, status, msg)
		return
	}
	text := a.announce(ev.GroupID, nickname, ev.SubType, resp)
	a.stats.recordOutcome("event", ev.GroupID, qq, ev.SubType, nickname, resp.Status, text)
}

// announce 按站点返回的 status 播报回群，返回实际播报的文案（静默时返回空串）。
// 注意：站点返回的 username/user_id/inviter_id 不写日志、不发群消息。
func (a *app) announce(groupID int64, nickname, subType string, resp *webhookResponse) string {
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
		return ""
	default:
		a.log.warnf("站点返回未知 status=%q，不播报 group_id=%d", resp.Status, groupID)
		return ""
	}
	a.sendGroupMsg(groupID, text)
	return text
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
				a.stats.recordOutcome("drain", e.GroupID, e.QQ, e.SubType, "", "dead",
					fmt.Sprintf("处理 %d 次仍失败，移入死信", e.Tries))
				continue
			}
			if werr := a.queue.append(e); werr != nil {
				a.log.errorf("重试队列：条目写回失败 qq=%s group_id=%d：%v", e.QQ, e.GroupID, werr)
				a.stats.recordOutcome("drain", e.GroupID, e.QQ, e.SubType, "", "error",
					"条目写回失败："+werr.Error())
				continue
			}
			a.log.warnf("重试队列：条目上报失败，tries=%d 已写回 qq=%s group_id=%d sub_type=%s：%v",
				e.Tries, e.QQ, e.GroupID, e.SubType, err)
			a.stats.recordOutcome("drain", e.GroupID, e.QQ, e.SubType, "", "queued",
				fmt.Sprintf("上报失败已写回（tries=%d）", e.Tries))
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
		text := a.announce(e.GroupID, nickname, e.SubType, resp)
		a.stats.recordOutcome("drain", e.GroupID, e.QQ, e.SubType, nickname, resp.Status, text)
	}
}

// ---------------------------------------------------------------------------
// 内置 HTTP 服务：接收 NapCat 上报
// ---------------------------------------------------------------------------

// serveHTTP 路由：
//
//	POST 任意路径 → OneBot 上报处理（NapCat 上报路径可配置，不挑路径）；
//	GET /healthz → 200 "ok"；
//	GET / 或 /panel → 只读状态面板（HTML）；
//	GET /api/stats → 面板数据（JSON）；
//	其他 GET → 404。
func (a *app) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/healthz":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok")
		case "/", "/panel", "/panel/":
			a.servePanel(w, r)
		case "/api/stats":
			a.serveStats(w, r)
		case "/api/state":
			a.serveState(w, r)
		case "/api/queue":
			if a.panelBearerAuth(w, r) {
				a.serveQueue(w, r)
			}
		case "/api/logs":
			if a.panelBearerAuth(w, r) {
				a.serveLogs(w, r)
			}
		case "/api/config":
			if a.panelBearerAuth(w, r) {
				a.serveConfigGet(w, r)
			}
		default:
			http.NotFound(w, r)
		}
		return
	}
	if r.Method == http.MethodPost {
		switch r.URL.Path {
		case "/api/setup", "/api/login":
			// 首次初始化与登录本身不要求 token；来源头校验保留（跨站请求带自定义头会被浏览器预检拦截）
			if r.Header.Get("X-ArcGate-Panel") != "1" {
				http.Error(w, "缺少面板请求头", http.StatusForbidden)
				return
			}
			a.servePanelPost(w, r)
		case "/api/test-announce", "/api/test-departure", "/api/queue/drain", "/api/queue/clear", "/api/stats/reset", "/api/config":
			if !a.panelBearerAuth(w, r) {
				return
			}
			// 简易 CSRF 防线：跨站 fetch 带自定义头会触发预检而被浏览器拦截
			if r.Header.Get("X-ArcGate-Panel") != "1" {
				http.Error(w, "缺少面板请求头", http.StatusForbidden)
				return
			}
			a.servePanelPost(w, r)
		default:
			a.handleReport(w, r) // 其余任意 POST 路径 = NapCat 上报
		}
		return
	}
	http.NotFound(w, r)
}

// panelAuth 面板登录凭据与 token 签发（.env 是持久层，这里是在运行期的热状态：
// 首次初始化与后续改密都立即生效，无需重启；凭据变更轮换签名密钥使旧登录态全部失效）。
type panelAuth struct {
	mu     sync.Mutex
	user   string
	pass   string // 空 = 未初始化（首次打开面板需自己设置账号密码）
	secret []byte // HMAC-SHA256 签名密钥，仅存内存，重启后所有登录态自然失效
}

func newPanelAuth(user, pass string) *panelAuth {
	if strings.TrimSpace(user) == "" {
		user = "admin"
	}
	return &panelAuth{user: user, pass: pass, secret: randomSecret()}
}

// randomSecret 生成 32 字节签名密钥；crypto/rand 失败时退化为时间+进程号摘要（不 panic）。
func randomSecret() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err == nil {
		return b
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%d", time.Now().UnixNano(), os.Getpid())))
	return sum[:]
}

func (p *panelAuth) initialized() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pass != ""
}

// verify 常量时间比对账号与密码。
func (p *panelAuth) verify(user, pass string) bool {
	p.mu.Lock()
	u, pw := p.user, p.pass
	p.mu.Unlock()
	okUser := subtle.ConstantTimeCompare([]byte(user), []byte(u)) == 1
	okPass := subtle.ConstantTimeCompare([]byte(pass), []byte(pw)) == 1
	return okUser && okPass && pw != ""
}

// set 更新凭据并轮换签名密钥（旧 token 全部失效）。
func (p *panelAuth) set(user, pass string) {
	if strings.TrimSpace(user) == "" {
		user = "admin"
	}
	p.mu.Lock()
	p.user, p.pass, p.secret = user, pass, randomSecret()
	p.mu.Unlock()
}

func (p *panelAuth) values() (user, pass string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.user, p.pass
}

// sign 签发登录 token：payload 为 "user|过期Unix秒"，HMAC-SHA256 签名，7 天有效。
func (p *panelAuth) sign(user string, exp time.Time) string {
	payload := user + "|" + strconv.FormatInt(exp.Unix(), 10)
	mac := hmac.New(sha256.New, p.hmacKey())
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) +
		"." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (p *panelAuth) hmacKey() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.secret
}

// checkToken 校验 Bearer token：签名、有效期、账号一致性（改账号后旧 token 失效）。
func (p *panelAuth) checkToken(tok string) bool {
	dot := strings.LastIndexByte(tok, '.')
	if dot <= 0 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(tok[:dot])
	if err != nil {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(tok[dot+1:])
	if err != nil {
		return false
	}
	expected := hmac.New(sha256.New, p.hmacKey())
	expected.Write(payload)
	if subtle.ConstantTimeCompare(sig, expected.Sum(nil)) != 1 {
		return false
	}
	s := string(payload)
	idx := strings.LastIndexByte(s, '|')
	if idx < 0 {
		return false
	}
	exp, err := strconv.ParseInt(s[idx+1:], 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return false
	}
	user, _ := p.values()
	return s[:idx] == user
}

// serveState 面板状态（无需鉴权）：是否已初始化，前端据此显示初始化页或登录页。
func (a *app) serveState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"initialized": a.panel.initialized()})
}

// serveSetup 首次初始化：设置面板账号密码，写入 .env 并立即热生效（仅未初始化时可用）。
// 未初始化状态下此接口开放——与「首个注册者成为管理员」同模式，公网部署请先收紧监听或加反代鉴权。
func (a *app) serveSetup(w http.ResponseWriter, r *http.Request) {
	if a.panel.initialized() {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "已初始化；如需修改请登录后在配置区操作"})
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		http.Error(w, "请求体应为 JSON", http.StatusBadRequest)
		return
	}
	user := strings.TrimSpace(body.Username)
	if user == "" {
		user = "admin"
	}
	if len(body.Password) < 8 {
		http.Error(w, "密码至少 8 位", http.StatusBadRequest)
		return
	}
	if _, err := saveDotEnv(".env", map[string]string{
		"ARC_PANEL_USER":     user,
		"ARC_PANEL_PASSWORD": body.Password,
	}); err != nil {
		http.Error(w, "写入 .env 失败："+err.Error(), http.StatusInternalServerError)
		return
	}
	a.panel.set(user, body.Password)
	a.log.addSecret(body.Password) // 口令加入脱敏名单，绝不落日志
	a.log.infof("面板：初始化完成，账号=%s（密码不记录）", user)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// serveLogin 校验账号密码并签发 7 天有效的 Bearer token；失败延迟 300ms 防爆破。
func (a *app) serveLogin(w http.ResponseWriter, r *http.Request) {
	if !a.panel.initialized() {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "未初始化"})
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		http.Error(w, "请求体应为 JSON", http.StatusBadRequest)
		return
	}
	if !a.panel.verify(strings.TrimSpace(body.Username), body.Password) {
		time.Sleep(300 * time.Millisecond)
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "账号或密码错误"})
		return
	}
	exp := time.Now().Add(7 * 24 * time.Hour)
	writeJSON(w, http.StatusOK, map[string]any{
		"token":      a.panel.sign(strings.TrimSpace(body.Username), exp),
		"expires_at": exp.Format(time.RFC3339),
	})
}

// panelBearerAuth 面板数据/操作鉴权：已初始化且持有效 Bearer token 才放行，否则 401 JSON
// （前端据此切换登录视图）。面板 HTML 外壳本身无敏感数据，不设门槛。
func (a *app) panelBearerAuth(w http.ResponseWriter, r *http.Request) bool {
	if !a.panel.initialized() {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "uninitialized"})
		return false
	}
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !a.panel.checkToken(tok) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return false
	}
	return true
}

// servePanel 输出面板 HTML 外壳（无敏感数据；数据由前端携 token 轮询 /api/*，401 时切换登录视图）。
func (a *app) servePanel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = io.WriteString(w, panelHTML)
}

// panelStats 是 /api/stats 的 JSON 快照。只含 QQ 侧信息与聚合计数，
// 令牌只回掩码，站点账号数据（username 等）从不出现。
type panelStats struct {
	Version      string         `json:"version"`
	StartedAt    string         `json:"started_at"`
	UptimeSec    int64          `json:"uptime_sec"`
	DryRun       bool           `json:"dry_run"`
	Site         string         `json:"site"`
	SiteToken    string         `json:"site_token"`
	ListenAddr   string         `json:"listen_addr"`
	OnebotAPI    string         `json:"onebot_api"`
	OnebotAuth   bool           `json:"onebot_auth"`
	WatchGroups  string         `json:"watch_groups"`
	SiteHealth   string         `json:"site_health"`
	SiteOK       bool           `json:"site_ok"`
	NapcatHealth string         `json:"napcat_health"`
	NapcatOK     bool           `json:"napcat_ok"`
	QueueEnabled bool           `json:"queue_enabled"`
	QueuePending int            `json:"queue_pending"`
	Received     int            `json:"received"`
	Ignored      int            `json:"ignored"`
	Deduped      int            `json:"deduped"`
	ByStatus     map[string]int `json:"by_status"`
	WebhookOK    int            `json:"webhook_ok"`
	WebhookErr   int            `json:"webhook_err"`
	Retries      int            `json:"retries"`
	MsgSent      int            `json:"msg_sent"`
	MsgFailed    int            `json:"msg_failed"`
	Recent       []eventRecord  `json:"recent"`
}

// serveStats 输出面板 JSON 快照。每次请求对站点与 NapCat 各做一次 2.5s 超时探活，
// 面板 5 秒轮询一次，开销可忽略。
func (a *app) serveStats(w http.ResponseWriter, r *http.Request) {
	if !a.panelBearerAuth(w, r) {
		return
	}
	s := a.stats
	s.mu.Lock()
	byStatus := make(map[string]int, len(s.byStatus))
	for k, v := range s.byStatus {
		byStatus[k] = v
	}
	snap := panelStats{
		Version:      version,
		StartedAt:    s.startedAt.Format("2006-01-02 15:04:05"),
		UptimeSec:    int64(time.Since(s.startedAt).Seconds()),
		DryRun:       a.dryRun,
		QueueEnabled: a.queueEnabled,
		Received:     s.received,
		Ignored:      s.ignored,
		Deduped:      s.deduped,
		ByStatus:     byStatus,
		WebhookOK:    s.webhookOK,
		WebhookErr:   s.webhookErr,
		Retries:      s.retries,
		MsgSent:      s.msgSent,
		MsgFailed:    s.msgFailed,
		Recent:       append([]eventRecord(nil), s.recent...),
	}
	s.mu.Unlock()
	snap.Site = a.cfg.ArcURL
	snap.SiteToken = maskedToken(a.cfg.BotToken)
	snap.ListenAddr = a.cfg.ListenAddr
	snap.OnebotAPI = a.cfg.OnebotAPIURL
	snap.OnebotAuth = a.cfg.OnebotToken != ""
	snap.WatchGroups = watchGroupsLabel(a.cfg.WatchGroups)
	snap.QueuePending = a.queue.depth()
	snap.SiteHealth, snap.SiteOK = a.probeSite()
	snap.NapcatHealth, snap.NapcatOK = a.probeNapcat()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(snap)
}

// maskedToken 令牌展示掩码：不泄露内容，只给长度便于核对配置。
func maskedToken(tok string) string {
	if tok == "" {
		return "(未设置)"
	}
	return fmt.Sprintf("***（长度 %d）", len(tok))
}

// watchGroupsLabel 群白名单展示文案。
func watchGroupsLabel(set map[int64]bool) string {
	if len(set) == 0 {
		return "所有群"
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, strconv.FormatInt(id, 10))
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// probeHTTPClient 仅供面板探活：短超时，与业务 HTTP 客户端隔离。
var probeHTTPClient = &http.Client{Timeout: 2500 * time.Millisecond}

// probeSite 探活站点健康检查接口 /api/health。
func (a *app) probeSite() (string, bool) {
	if a.cfg.ArcURL == "" {
		return "未配置", false
	}
	resp, err := probeHTTPClient.Get(a.cfg.ArcURL + "/api/health")
	if err != nil {
		return "不可达：" + err.Error(), false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "在线（HTTP " + strconv.Itoa(resp.StatusCode) + "）", true
	}
	return "异常（HTTP " + strconv.Itoa(resp.StatusCode) + "）", false
}

// probeNapcat 探活 NapCat /get_version_info（OneBot 11 标准动作）。
func (a *app) probeNapcat() (string, bool) {
	req, err := http.NewRequest(http.MethodPost, a.cfg.OnebotAPIURL+"/get_version_info", strings.NewReader("{}"))
	if err != nil {
		return "不可达：" + err.Error(), false
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.OnebotToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.OnebotToken)
	}
	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		return "离线：" + err.Error(), false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "在线", true
	}
	return "异常（HTTP " + strconv.Itoa(resp.StatusCode) + "）", false
}

// ---------------------------------------------------------------------------
// 面板操作端点：测试播报 / 手动注入退群 / 队列维护 / 统计重置 / 配置查看与保存
// ---------------------------------------------------------------------------

// panelConfigKeys 是面板可查看/修改的配置键（其余键不通过面板暴露）。
var panelConfigKeys = []string{
	"OBSIDIAN_ARC_URL",
	"OBSIDIAN_ARC_BOT_TOKEN",
	"OBSIDIAN_ARC_WATCH_GROUPS",
	"ARC_LISTEN_ADDR",
	"ARC_ONEBOT_API_URL",
	"ARC_ONEBOT_ACCESS_TOKEN",
	"ARC_PANEL_USER",
	"ARC_PANEL_PASSWORD",
	"ARC_LOG_LEVEL",
}

// writeJSON 输出 JSON 响应的小工具。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// serveConfigGet 返回当前生效配置（令牌与口令仅掩码；白名单给原始串便于编辑）。
// 面板账号/口令读热状态（初始化与改密即时生效，无需重启）。
func (a *app) serveConfigGet(w http.ResponseWriter, _ *http.Request) {
	panelUser, panelPass := a.panel.values()
	vals := map[string]string{
		"OBSIDIAN_ARC_URL":          a.cfg.ArcURL,
		"OBSIDIAN_ARC_BOT_TOKEN":    maskedToken(a.cfg.BotToken),
		"OBSIDIAN_ARC_WATCH_GROUPS": a.cfg.WatchGroupsRaw,
		"ARC_LISTEN_ADDR":           a.cfg.ListenAddr,
		"ARC_ONEBOT_API_URL":        a.cfg.OnebotAPIURL,
		"ARC_ONEBOT_ACCESS_TOKEN":   maskedToken(a.cfg.OnebotToken),
		"ARC_PANEL_USER":            panelUser,
		"ARC_PANEL_PASSWORD":        maskedToken(panelPass),
		"ARC_LOG_LEVEL":             a.cfg.LogLevel,
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": vals})
}

// serveConfigSave 校验并写入 .env（留空/掩码值 = 保持不变），重启进程后生效。
func (a *app) serveConfigSave(w http.ResponseWriter, r *http.Request) {
	var body map[string]string
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		http.Error(w, "请求体应为 JSON 对象", http.StatusBadRequest)
		return
	}
	allowed := make(map[string]bool, len(panelConfigKeys))
	for _, k := range panelConfigKeys {
		allowed[k] = true
	}
	updates := make(map[string]string)
	for k, v := range body {
		if !allowed[k] {
			http.Error(w, "不支持的配置键："+k, http.StatusBadRequest)
			return
		}
		v = strings.TrimSpace(v)
		if v == "" || strings.HasPrefix(v, "***") { // 留空/掩码 = 保持不变
			continue
		}
		updates[k] = v
	}
	for k, v := range updates {
		switch k {
		case "OBSIDIAN_ARC_URL", "ARC_ONEBOT_API_URL":
			if !strings.HasPrefix(v, "http://") && !strings.HasPrefix(v, "https://") {
				http.Error(w, k+" 必须以 http(s):// 开头", http.StatusBadRequest)
				return
			}
		case "ARC_LISTEN_ADDR":
			if !strings.Contains(v, ":") {
				http.Error(w, "ARC_LISTEN_ADDR 形如 127.0.0.1:3002", http.StatusBadRequest)
				return
			}
		case "ARC_LOG_LEVEL":
			switch v {
			case "debug", "info", "warn", "error":
			default:
				http.Error(w, "ARC_LOG_LEVEL 只支持 debug/info/warn/error", http.StatusBadRequest)
				return
			}
		case "ARC_PANEL_PASSWORD":
			if len(v) < 8 {
				http.Error(w, "ARC_PANEL_PASSWORD 至少 8 位", http.StatusBadRequest)
				return
			}
		case "OBSIDIAN_ARC_WATCH_GROUPS":
			for _, part := range strings.Split(v, ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				if _, err := strconv.ParseInt(part, 10, 64); err != nil {
					http.Error(w, "OBSIDIAN_ARC_WATCH_GROUPS 应为逗号分隔的群号", http.StatusBadRequest)
					return
				}
			}
		}
	}
	if len(updates) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"saved": 0, "detail": "没有修改（留空 = 保持不变）"})
		return
	}
	n, err := saveDotEnv(".env", updates)
	if err != nil {
		http.Error(w, "写入 .env 失败："+err.Error(), http.StatusInternalServerError)
		return
	}
	detail := "已写入 .env，重启 ArcGate 后生效"
	// 面板账号/口令无需重启：立即热生效并轮换签名密钥（旧登录态全部失效）
	newUser, changePass := updates["ARC_PANEL_USER"], updates["ARC_PANEL_PASSWORD"]
	if newUser != "" || changePass != "" {
		curUser, curPass := a.panel.values()
		if newUser == "" {
			newUser = curUser
		}
		if changePass == "" {
			changePass = curPass
		}
		a.panel.set(newUser, changePass)
		if updates["ARC_PANEL_PASSWORD"] != "" {
			a.log.addSecret(updates["ARC_PANEL_PASSWORD"])
		}
		a.log.infof("面板：登录凭据已修改并立即生效（账号=%s，密码不记录）", newUser)
		detail = "已保存并立即生效（下次登录用新凭据）"
	}
	a.log.infof("面板：已更新 .env 共 %d 项", n)
	writeJSON(w, http.StatusOK, map[string]any{"saved": n, "detail": detail})
}

// serveQueue 返回重试队列与死信概览。
func (a *app) serveQueue(w http.ResponseWriter, _ *http.Request) {
	pending := a.queue.list()
	dead := 0
	if data, err := os.ReadFile(a.queue.deadPath()); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) != "" {
				dead++
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"pending": pending, "dead": dead})
}

// servePanelPost 分发面板写操作（均已通过面板鉴权与来源头校验）。
func (a *app) servePanelPost(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/test-announce":
		var body struct {
			GroupID int64  `json:"group_id"`
			Text    string `json:"text"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil || body.GroupID <= 0 || strings.TrimSpace(body.Text) == "" {
			http.Error(w, "需要 group_id（正整数）与 text（非空）", http.StatusBadRequest)
			return
		}
		a.log.infof("面板：手动测试播报 group_id=%d", body.GroupID)
		go a.sendGroupMsg(body.GroupID, body.Text)
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "detail": "已提交发送，结果见计数与日志"})

	case "/api/test-departure":
		var body struct {
			GroupID int64  `json:"group_id"`
			QQ      string `json:"qq"`
			SubType string `json:"sub_type"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
			http.Error(w, "请求体应为 JSON", http.StatusBadRequest)
			return
		}
		subType := body.SubType
		if subType == "" {
			subType = "leave"
		}
		if body.GroupID <= 0 || !qqPattern.MatchString(body.QQ) || (subType != "leave" && subType != "kick") {
			http.Error(w, "需要 group_id（正整数）、qq（5-15 位数字）、sub_type（leave/kick，默认 leave）", http.StatusBadRequest)
			return
		}
		uid, _ := strconv.ParseInt(body.QQ, 10, 64)
		a.log.infof("面板：手动注入退群事件 group_id=%d qq=%s sub_type=%s", body.GroupID, body.QQ, subType)
		go a.processEvent(reportEvent{PostType: "notice", NoticeType: "group_decrease", SubType: subType, GroupID: body.GroupID, UserID: uid, OperatorID: uid})
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "detail": "已提交处理，结果见最近事件"})

	case "/api/queue/drain":
		a.log.infof("面板：手动触发重试队列排水")
		go a.drainOnce()
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "detail": "排水已触发"})

	case "/api/queue/clear":
		n, err := a.queue.clear()
		if err != nil {
			http.Error(w, "清空队列失败："+err.Error(), http.StatusInternalServerError)
			return
		}
		a.log.warnf("面板：已清空重试队列 %d 条", n)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "cleared": n})

	case "/api/stats/reset":
		a.stats.reset()
		a.log.infof("面板：统计与最近事件已重置")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case "/api/config":
		a.serveConfigSave(w, r)

	case "/api/setup":
		a.serveSetup(w, r)

	case "/api/login":
		a.serveLogin(w, r)

	default:
		http.NotFound(w, r)
	}
}

// serveLogs 返回 arcgate.log 尾部 n 行（?n=200，上限 1000）。日志本身已经过令牌脱敏。
func (a *app) serveLogs(w http.ResponseWriter, r *http.Request) {
	n := int64(200)
	if raw := r.URL.Query().Get("n"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			n = v
		}
	}
	if n > 1000 {
		n = 1000
	}
	lines := []string{}
	if data, err := os.ReadFile(filepath.Join(a.cfg.LogDir, "arcgate.log")); err == nil {
		all := strings.Split(strings.TrimRight(string(data), "\r\n"), "\n")
		if int64(len(all)) > n {
			all = all[len(all)-int(n):]
		}
		lines = all
	}
	writeJSON(w, http.StatusOK, map[string]any{"lines": lines})
}

// panelHTML 是只读状态面板（内联单页，无外部资源；数据经 /api/stats 轮询）。
const panelHTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ArcGate 控制台</title>
<style>
  /* 设计 token 对齐 ObsidianArc（_chat.scss 主题变量），跟随系统深浅色 */
  :root {
    color-scheme: light dark;
    --bg: #fafafa; --card: #f4f4f5; --field: #e8e8eb; --text: #1d1d1d; --text2: #71717a;
    --primary: #18181b; --on-primary: #ffffff; --border: #e4e4e7;
    --ok: #16a34a; --bad: #dc2626; --warn: #d97706; --info: #2563eb;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #1b1b1d; --card: #242426; --field: #343437; --text: #ededef; --text2: #a1a1aa;
      --primary: #ffffff; --on-primary: #18181b; --border: #3f3f46;
      --ok: #4ade80; --bad: #f87171; --warn: #fbbf24; --info: #93c5fd;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 24px; font: 14px/1.6 system-ui, "Microsoft YaHei", sans-serif; background: var(--bg); color: var(--text); }
  h1 { font-size: 22px; margin: 0; }
  .sub { color: var(--text2); font-size: 12px; margin: 2px 0 0; }
  header.top { display: flex; justify-content: space-between; align-items: flex-end; gap: 12px; flex-wrap: wrap; margin-bottom: 18px; }
  .top-right { display: flex; align-items: center; gap: 10px; }
  .who { color: var(--text2); font-size: 13px; }
  .ver { color: var(--text2); font-size: 14px; font-weight: 400; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px; margin-bottom: 14px; }
  .card { background: var(--card); border-radius: 18px; padding: 16px 18px; }
  .card h2 { font-size: 14px; font-weight: 600; margin: 0 0 12px; display: flex; align-items: center; gap: 8px; }
  .ico { width: 26px; height: 26px; border-radius: 8px; background: var(--field); display: inline-flex; align-items: center; justify-content: center; font-size: 13px; flex: none; }
  table.rows { width: 100%; border-collapse: collapse; }
  table.rows td { padding: 3px 8px 3px 0; vertical-align: top; }
  table.rows td.k { color: var(--text2); white-space: nowrap; width: 1%; }
  table.rows td.v { font-family: ui-monospace, Consolas, monospace; word-break: break-all; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
  .dot.ok { background: var(--ok); } .dot.bad { background: var(--bad); } .dot.warn { background: var(--warn); }
  .chips { display: flex; flex-wrap: wrap; gap: 8px; }
  .chip { background: var(--field); border-radius: 999px; padding: 2px 10px; font-size: 12px; }
  .chip b { font-weight: 600; }
  table.evt { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.evt th { text-align: left; color: var(--text2); font-weight: 500; padding: 4px 10px 4px 0; border-bottom: 1px solid var(--border); }
  table.evt td { padding: 4px 10px 4px 0; border-bottom: 1px solid var(--border); font-family: ui-monospace, Consolas, monospace; }
  .st { padding: 0 8px; border-radius: 999px; font-size: 12px; white-space: nowrap; }
  .st.ok { background: rgba(22,163,74,.14); color: var(--ok); }
  .st.info { background: rgba(37,99,235,.12); color: var(--info); }
  .st.warn { background: rgba(217,119,6,.14); color: var(--warn); }
  .st.refused { background: rgba(234,88,12,.14); color: #ea580c; }
  .st.bad { background: rgba(220,38,38,.12); color: var(--bad); }
  .frow { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 10px; }
  .frow label { color: var(--text2); width: 64px; flex: none; }
  input, select {
    background: var(--field); color: var(--text); border: 1px solid transparent; border-radius: 10px;
    padding: 6px 10px; font: inherit;
  }
  input:focus, select:focus { outline: 2px solid var(--primary); outline-offset: -1px; }
  button {
    background: var(--primary); color: var(--on-primary); border: none; border-radius: 10px;
    padding: 6px 14px; font: inherit; cursor: pointer;
  }
  button:hover { opacity: .88; }
  .op { min-height: 18px; font-size: 12px; margin-top: 6px; }
  .op.good { color: var(--ok); } .op.bad { color: var(--bad); }
  .hint { color: var(--text2); font-size: 12px; }
  pre.log { background: var(--field); border-radius: 10px; padding: 10px; max-height: 260px; overflow: auto;
    font: 12px/1.5 ui-monospace, Consolas, monospace; margin: 0; }
  .foot { color: var(--text2); font-size: 12px; margin-top: 18px; }
  #err { color: var(--bad); font-size: 13px; min-height: 18px; }
  /* 登录 / 初始化覆盖层 */
  .auth-wrap { position: fixed; inset: 0; display: flex; align-items: center; justify-content: center; background: var(--bg); z-index: 10; }
  .auth-card { width: 340px; max-width: calc(100vw - 32px); background: var(--card); border-radius: 18px; padding: 26px; }
  .auth-card h1 { margin-bottom: 2px; }
  .auth-card label { display: block; color: var(--text2); font-size: 12px; margin: 14px 0 4px; }
  .auth-card input { width: 100%; }
  .auth-card button { width: 100%; margin-top: 18px; }
</style>
</head>
<body>

<div id="auth" class="auth-wrap" style="display:none">
  <div class="auth-card">
    <h1>ArcGate</h1>
    <p class="sub">QQ 群退群上报机器人控制台</p>
    <div id="view-setup" style="display:none">
      <p class="hint" style="margin-top:14px">首次使用：自己设置面板的账号与密码（保存到 .env，之后可在面板「配置」里修改）。</p>
      <label>账号</label>
      <input id="s-user" value="admin">
      <label>密码（至少 8 位）</label>
      <input id="s-pass" type="password">
      <label>确认密码</label>
      <input id="s-pass2" type="password">
      <button onclick="doSetup()">完成初始化</button>
    </div>
    <div id="view-login" style="display:none">
      <label>账号</label>
      <input id="l-user">
      <label>密码</label>
      <input id="l-pass" type="password" onkeydown="if(event.key==='Enter')doLogin()">
      <button onclick="doLogin()">登录</button>
    </div>
    <div id="auth-msg" class="op"></div>
  </div>
</div>

<div id="main" style="display:none">
  <header class="top">
    <div>
      <h1>ArcGate <span id="ver" class="ver"></span></h1>
      <p class="sub">QQ 群退群上报机器人 · 每 5 秒自动刷新</p>
    </div>
    <div class="top-right">
      <span id="who" class="who"></span>
      <button onclick="doLogout()">登出</button>
    </div>
  </header>
  <div id="err"></div>
  <div class="grid">
    <div class="card"><h2><span class="ico">▣</span>运行</h2><table class="rows" id="t-run"></table></div>
    <div class="card"><h2><span class="ico">⚙</span>配置</h2><table class="rows" id="t-cfg"></table></div>
    <div class="card"><h2><span class="ico">⇄</span>连通性</h2><table class="rows" id="t-health"></table></div>
    <div class="card"><h2><span class="ico">∑</span>计数</h2><table class="rows" id="t-count"></table>
      <h2 style="margin-top:12px"><span class="ico">%</span>处理结果分布</h2><div class="chips" id="chips"></div></div>
  </div>
  <div class="grid">
    <div class="card"><h2><span class="ico">⚡</span>操作</h2>
      <div class="frow"><label>模拟退群</label>
        <input id="d-group" placeholder="群号" size="10">
        <input id="d-qq" placeholder="QQ号" size="12">
        <select id="d-type"><option value="leave">主动退群</option><option value="kick">被移出群</option></select>
        <button onclick="testDeparture()">注入处理</button>
      </div>
      <div class="frow"><label>测试播报</label>
        <input id="a-group" placeholder="群号" size="10">
        <input id="a-text" placeholder="文案" size="22">
        <button onclick="testAnnounce()">直接发到群</button>
      </div>
      <div class="frow"><label>维护</label>
        <button onclick="drainNow()">立即排水</button>
        <button onclick="clearQueue()">清空队列</button>
        <button onclick="resetStats()">重置统计</button>
        <button onclick="loadLogs()">刷新日志</button>
      </div>
      <div id="op-result" class="op"></div>
      <div class="hint">模拟退群走完整链路（查昵称 → 打站点 Webhook → 按状态播报）；测试播报直接经 NapCat 发送。</div>
    </div>
    <div class="card"><h2><span class="ico">✎</span>配置（令牌留空 = 保持不变）</h2>
      <table class="rows" id="t-cfgedit"></table>
      <div class="frow" style="margin-top:10px">
        <button onclick="saveConfig()">保存配置</button>
        <button onclick="loadConfig()">重新载入</button>
        <span id="cfg-result" class="op"></span>
      </div>
      <div class="hint">面板账号/口令保存后立即生效；其余项写入 .env，重启 ArcGate 后生效。</div>
    </div>
  </div>
  <div class="card"><h2><span class="ico">≣</span>最近事件（最多 50 条）</h2>
    <table class="evt"><thead><tr><th>时间</th><th>来源</th><th>群</th><th>QQ</th><th>类型</th><th>状态</th><th>说明 / 播报</th></tr></thead>
    <tbody id="tbody"></tbody></table>
    <div class="hint" id="empty" style="display:none">还没有处理过任何退群事件。</div>
  </div>
  <div class="card" style="margin-top:14px"><h2><span class="ico">▤</span>运行日志（尾部 200 行，已脱敏）</h2><pre id="log" class="log"></pre></div>
  <div class="foot">令牌与站点账号信息（username 等）永不展示 · 默认仅监听 127.0.0.1；公网部署请用反向代理加访问控制。</div>
</div>

<script>
var TOKEN = localStorage.getItem('ag_tok') || '';
function saveTok(t) { TOKEN = t || ''; if (t) localStorage.setItem('ag_tok', t); else localStorage.removeItem('ag_tok'); }

function show(which) {
  document.getElementById('auth').style.display = which === 'main' ? 'none' : 'flex';
  document.getElementById('view-setup').style.display = which === 'setup' ? '' : 'none';
  document.getElementById('view-login').style.display = which === 'login' ? '' : 'none';
  document.getElementById('main').style.display = which === 'main' ? '' : 'none';
  if (which === 'main') {
    document.getElementById('who').textContent = localStorage.getItem('ag_user') || '';
    refresh(); loadConfig(); loadLogs();
  }
}
function authMsg(msg, isErr) {
  var el = document.getElementById('auth-msg');
  el.textContent = msg; el.className = isErr ? 'op bad' : 'op good';
}
function boot() {
  fetch('/api/state').then(function(r) { return r.json(); }).then(function(s) {
    if (!s.initialized) { show('setup'); return; }
    if (!TOKEN) { show('login'); return; }
    fetch('/api/stats', { headers: { 'Authorization': 'Bearer ' + TOKEN } }).then(function(r) {
      show(r.status === 401 ? 'login' : 'main');
    }).catch(function() { show('login'); });
  }).catch(function(e) { authMsg('无法连接面板后端：' + e.message, true); });
}
function doSetup() {
  var u = document.getElementById('s-user').value.trim() || 'admin';
  var p = document.getElementById('s-pass').value;
  var p2 = document.getElementById('s-pass2').value;
  if (p.length < 8) { authMsg('密码至少 8 位', true); return; }
  if (p !== p2) { authMsg('两次输入的密码不一致', true); return; }
  postJSON('/api/setup', { username: u, password: p }).then(function() {
    authMsg('初始化完成，正在登录…');
    return loginCore(u, p);
  }).catch(function(e) { authMsg(e.message, true); });
}
function loginCore(u, p) {
  return postJSON('/api/login', { username: u, password: p }).then(function(j) {
    saveTok(j.token); localStorage.setItem('ag_user', u); show('main');
  });
}
function doLogin() {
  var u = document.getElementById('l-user').value.trim();
  var p = document.getElementById('l-pass').value;
  loginCore(u, p).catch(function(e) { authMsg('登录失败：' + e.message, true); });
}
function doLogout() { saveTok(''); show('login'); }

function api(url, opts) {
  opts = opts || {};
  opts.headers = Object.assign({}, opts.headers || {}, { 'Authorization': 'Bearer ' + TOKEN });
  return fetch(url, opts).then(function(r) {
    if (r.status === 401) { saveTok(''); show('login'); throw new Error('登录已过期，请重新登录'); }
    return r;
  });
}
function postJSON(url, body) {
  return api(url, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-ArcGate-Panel': '1' }, body: JSON.stringify(body || {}) })
    .then(function(r) {
      return r.text().then(function(txt) {
        var j = {};
        try { j = JSON.parse(txt); } catch (e) { j = { message: txt }; }
        if (!r.ok) throw new Error(j.message || j.error || j.detail || ('HTTP ' + r.status));
        return j;
      });
    });
}
function fmtDur(sec) {
  var d = Math.floor(sec/86400), h = Math.floor(sec%86400/3600), m = Math.floor(sec%3600/60), s = sec%60;
  var out = '';
  if (d) out += d + '天';
  if (d || h) out += h + '小时';
  if (d || h || m) out += m + '分';
  return out + s + '秒';
}
function row(t, k, v) {
  var tr = document.createElement('tr');
  var td1 = document.createElement('td'); td1.className = 'k'; td1.textContent = k;
  var td2 = document.createElement('td'); td2.className = 'v'; td2.textContent = v;
  tr.appendChild(td1); tr.appendChild(td2); t.appendChild(tr);
}
function fill(id, pairs) {
  var t = document.getElementById(id); t.textContent = '';
  pairs.forEach(function(p) { row(t, p[0], p[1]); });
}
function healthRow(t, k, v, ok) {
  var tr = document.createElement('tr');
  var td1 = document.createElement('td'); td1.className = 'k'; td1.textContent = k;
  var td2 = document.createElement('td'); td2.className = 'v';
  var dot = document.createElement('span');
  dot.className = 'dot ' + (ok ? 'ok' : 'bad');
  td2.appendChild(dot); td2.appendChild(document.createTextNode(v));
  tr.appendChild(td1); tr.appendChild(td2); t.appendChild(tr);
}
var ST_CLS = { departed: 'ok', refused: 'refused', already_departed: 'info', unknown: 'info', queued: 'warn', dead: 'warn' };
function stCls(st) { return ST_CLS[st] || (st.indexOf('error') === 0 ? 'bad' : 'info'); }
function refresh() {
  if (document.getElementById('main').style.display === 'none') return;
  api('/api/stats').then(function(r) { return r.json(); }).then(function(s) {
    document.getElementById('err').textContent = '';
    document.getElementById('ver').textContent = 'v' + s.version;
    fill('t-run', [
      ['运行时长', fmtDur(s.uptime_sec)],
      ['启动时间', s.started_at],
      ['模式', s.dry_run ? 'dry-run（演练）' : '正常'],
      ['重试队列', s.queue_enabled ? '启用' : '禁用（dry-run）'],
      ['队列积压', s.queue_pending + ' 条']
    ]);
    fill('t-cfg', [
      ['站点', s.site],
      ['站点令牌', s.site_token],
      ['上报监听', s.listen_addr],
      ['OneBot API', s.onebot_api],
      ['NapCat 鉴权', s.onebot_auth ? '启用' : '未设置'],
      ['监听群', s.watch_groups]
    ]);
    var th = document.getElementById('t-health'); th.textContent = '';
    healthRow(th, '站点 /api/health', s.site_health, s.site_ok);
    healthRow(th, 'NapCat API', s.napcat_health, s.napcat_ok);
    fill('t-count', [
      ['收到退群事件', s.received],
      ['过滤忽略（心跳等）', s.ignored],
      ['去重命中', s.deduped],
      ['Webhook 成功 / 失败', s.webhook_ok + ' / ' + s.webhook_err],
      ['自动重试次数', s.retries],
      ['播报成功 / 失败', s.msg_sent + ' / ' + s.msg_failed]
    ]);
    var chips = document.getElementById('chips'); chips.textContent = '';
    var keys = Object.keys(s.by_status);
    if (!keys.length) {
      var c0 = document.createElement('span'); c0.className = 'chip'; c0.textContent = '暂无';
      chips.appendChild(c0);
    }
    keys.sort().forEach(function(k) {
      var c = document.createElement('span'); c.className = 'chip';
      var b = document.createElement('b'); b.textContent = s.by_status[k] + '\u00d7 ';
      c.appendChild(b); c.appendChild(document.createTextNode(k));
      chips.appendChild(c);
    });
    var tb = document.getElementById('tbody'); tb.textContent = '';
    document.getElementById('empty').style.display = s.recent.length ? 'none' : '';
    s.recent.forEach(function(e) {
      var tr = document.createElement('tr');
      [e.time, e.source, e.group_id, e.qq, e.sub_type].forEach(function(v) {
        var td = document.createElement('td'); td.textContent = v; tr.appendChild(td);
      });
      var td = document.createElement('td');
      var st = document.createElement('span'); st.className = 'st ' + stCls(e.status); st.textContent = e.status;
      td.appendChild(st); tr.appendChild(td);
      var td2 = document.createElement('td'); td2.textContent = e.message || ''; tr.appendChild(td2);
      tb.appendChild(tr);
    });
  }).catch(function(e) { if (e.message !== '登录已过期，请重新登录') document.getElementById('err').textContent = '面板数据获取失败：' + e.message; });
}
function op(msg, isErr) {
  var el = document.getElementById('op-result');
  el.textContent = msg; el.className = isErr ? 'op bad' : 'op good';
}
function testDeparture() {
  var g = document.getElementById('d-group').value.trim();
  var q = document.getElementById('d-qq').value.trim();
  var t = document.getElementById('d-type').value;
  if (!g || !q) { op('群号和 QQ 都要填', true); return; }
  postJSON('/api/test-departure', { group_id: Number(g), qq: q, sub_type: t })
    .then(function(j) { op(j.detail || '已提交'); refresh(); })
    .catch(function(e) { op(e.message, true); });
}
function testAnnounce() {
  var g = document.getElementById('a-group').value.trim();
  var x = document.getElementById('a-text').value.trim();
  if (!g || !x) { op('群号和文案都要填', true); return; }
  postJSON('/api/test-announce', { group_id: Number(g), text: x })
    .then(function(j) { op(j.detail || '已提交'); refresh(); })
    .catch(function(e) { op(e.message, true); });
}
function drainNow() { postJSON('/api/queue/drain', {}).then(function(j) { op(j.detail || '已触发'); refresh(); }).catch(function(e) { op(e.message, true); }); }
function clearQueue() { postJSON('/api/queue/clear', {}).then(function(j) { op('已清空 ' + j.cleared + ' 条'); refresh(); }).catch(function(e) { op(e.message, true); }); }
function resetStats() { postJSON('/api/stats/reset', {}).then(function() { op('统计已重置'); refresh(); }).catch(function(e) { op(e.message, true); }); }
function loadLogs() {
  api('/api/logs?n=200').then(function(r) { return r.json(); }).then(function(j) {
    document.getElementById('log').textContent = (j.lines && j.lines.length) ? j.lines.join('\n') : '（暂无日志）';
  }).catch(function(e) { document.getElementById('log').textContent = '日志读取失败：' + e.message; });
}
var CFG_LABELS = [
  ['OBSIDIAN_ARC_URL', '站点地址'],
  ['OBSIDIAN_ARC_BOT_TOKEN', '站点令牌'],
  ['OBSIDIAN_ARC_WATCH_GROUPS', '监听群'],
  ['ARC_LISTEN_ADDR', '上报监听'],
  ['ARC_ONEBOT_API_URL', 'OneBot API'],
  ['ARC_ONEBOT_ACCESS_TOKEN', 'NapCat 令牌'],
  ['ARC_PANEL_USER', '面板账号'],
  ['ARC_PANEL_PASSWORD', '面板密码'],
  ['ARC_LOG_LEVEL', '日志级别']
];
var CFG_SECRET = { OBSIDIAN_ARC_BOT_TOKEN: 1, ARC_ONEBOT_ACCESS_TOKEN: 1, ARC_PANEL_PASSWORD: 1 };
function loadConfig() {
  api('/api/config').then(function(r) { return r.json(); }).then(function(j) {
    var t = document.getElementById('t-cfgedit'); t.textContent = '';
    CFG_LABELS.forEach(function(p) {
      var tr = document.createElement('tr');
      var td1 = document.createElement('td'); td1.className = 'k'; td1.textContent = p[1];
      var td2 = document.createElement('td');
      var inp = document.createElement('input'); inp.id = 'cfg-' + p[0]; inp.size = 26;
      if (CFG_SECRET[p[0]]) {
        inp.type = 'password'; inp.placeholder = j.keys[p[0]] || '';
      } else {
        inp.value = j.keys[p[0]] || '';
      }
      td2.appendChild(inp); tr.appendChild(td1); tr.appendChild(td2); t.appendChild(tr);
    });
  });
}
function saveConfig() {
  var body = {};
  CFG_LABELS.forEach(function(p) {
    var v = document.getElementById('cfg-' + p[0]).value.trim();
    if (v) body[p[0]] = v;
  });
  postJSON('/api/config', body).then(function(j) {
    document.getElementById('cfg-result').textContent = j.detail || ('已保存 ' + j.saved + ' 项');
    document.getElementById('cfg-result').className = 'op good';
    loadConfig(); refresh();
  }).catch(function(e) {
    document.getElementById('cfg-result').textContent = e.message;
    document.getElementById('cfg-result').className = 'op bad';
  });
}
boot();
setInterval(refresh, 5000);
</script>
</body>
</html>
`

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
		a.stats.recordIgnored()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if ev.NoticeType != "group_decrease" {
		a.log.debugf("忽略 notice_type=%s（心跳/消息等）", ev.NoticeType)
		a.stats.recordIgnored()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if ev.SubType != "leave" && ev.SubType != "kick" {
		a.log.debugf("忽略 sub_type=%s group_id=%d user_id=%d", ev.SubType, ev.GroupID, ev.UserID)
		a.stats.recordIgnored()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if ev.UserID == ev.SelfID {
		a.log.debugf("机器人自身变动事件，忽略 group_id=%d user_id=%d", ev.GroupID, ev.UserID)
		a.stats.recordIgnored()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(a.cfg.WatchGroups) > 0 && !a.cfg.WatchGroups[ev.GroupID] {
		a.log.debugf("群号不在白名单，忽略 group_id=%d user_id=%d", ev.GroupID, ev.UserID)
		a.stats.recordIgnored()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !qqPattern.MatchString(strconv.FormatInt(ev.UserID, 10)) {
		a.log.warnf("user_id 不符合 QQ 号格式，忽略 group_id=%d user_id=%d", ev.GroupID, ev.UserID)
		a.stats.recordIgnored()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// 尽快 204，异步处理
	a.stats.recordReceived()
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
	log := newLogger(cfg.LogDir, cfg.LogLevel, cfg.BotToken, cfg.OnebotToken, cfg.PanelPassword)

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
      同时在监听地址上提供只读状态面板：浏览器打开 http://<监听地址>/ 查看。
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

	log.infof("ArcGate v%s 启动（模式=服务器 dry-run=%v）", version, dryRun)
	log.infof("监听地址=%s", cfg.ListenAddr)
	log.infof("OneBot API=%s 入站鉴权=%v OneBot 令牌长度=%d", cfg.OnebotAPIURL, cfg.OnebotToken != "", len(cfg.OnebotToken))
	arcLabel := cfg.ArcURL
	if arcLabel == "" {
		arcLabel = "(未设置)"
	}
	log.infof("站点地址=%s 站点令牌长度=%d（仅记录长度，不记录内容）", arcLabel, len(cfg.BotToken))
	log.infof("监听群=%s", watchGroupsLabel(cfg.WatchGroups))
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
	panel := "http://" + cfg.ListenAddr + "/"
	if cfg.PanelPassword == "" {
		log.infof("状态面板 %s（首次打开需设置账号密码）", panel)
	} else {
		log.infof("状态面板 %s（账号已配置）", panel)
	}

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
