// droprelay —— 内网文件直传：WebTorrent 内核。
// 浏览器之间 WebRTC 数据通道直连，本服务只做三件事：
//  1. ws tracker：offer/answer 信令中继（bittorrent-tracker WebSocket 协议，v3 snake_case 版）
//  2. 房间：六位码 ↔ 文件清单（name/size/infohash），信令不走房间
//  3. manifest：扫 share 目录生成逐文件 .torrent 清单（webseed 做种用）
//
// 无中转兜底：直连打不通即失败（内网场景 host candidates 足够）。
// 日志仅内存环形缓冲；/api/log 只应从本机访问（公网入口由 caddy 拦掉）。
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/big"
	"mime"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

//go:embed web shareweb
var embedded embed.FS

// ---------- 内存日志（字节预算，默认 25MB，超限批量丢最旧） ----------

type logEntry struct {
	t     time.Time
	level string
	msg   string
}

type ringLog struct {
	mu      sync.Mutex
	buf     []logEntry // 时间序
	total   int        // 现存格式化字节数
	max     int        // 字节预算
	dropped int64      // 累计丢弃条数
}

func newRingLog(maxBytes int) *ringLog { return &ringLog{max: maxBytes} }

func entrySize(e logEntry) int { return len(e.msg) + 24 }

func (l *ringLog) add(level, format string, args ...any) {
	e := logEntry{time.Now(), level, fmt.Sprintf(format, args...)}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, e)
	l.total += entrySize(e)
	for l.total > l.max && len(l.buf) > 1 {
		cut := len(l.buf) / 4 // 一次丢最旧的四分之一，摊销搬移成本
		if cut < 1 {
			cut = 1
		}
		for i := 0; i < cut; i++ {
			l.total -= entrySize(l.buf[i])
			l.dropped++
		}
		l.buf = append(l.buf[:0], l.buf[cut:]...)
	}
}

func (l *ringLog) dump(tail int) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.buf) == 0 {
		return ""
	}
	entries := l.buf
	if tail > 0 && len(entries) > tail {
		entries = entries[len(entries)-tail:]
	}
	var b strings.Builder
	if l.dropped > 0 {
		fmt.Fprintf(&b, "…（已丢最旧 %d 条）\n", l.dropped)
	}
	for _, e := range entries {
		fmt.Fprintf(&b, "%s [%s] %s\n", e.t.Format("01-02 15:04:05"), e.level, e.msg)
	}
	return b.String()
}

var elog = newRingLog(25 << 20)

func human(n int64) string {
	const k = 1024
	switch {
	case n >= k*k*k:
		return fmt.Sprintf("%.2fGB", float64(n)/(k*k*k))
	case n >= k*k:
		return fmt.Sprintf("%.1fMB", float64(n)/(k*k))
	case n >= k:
		return fmt.Sprintf("%.1fKB", float64(n)/k)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// ---------- 公共小件 ----------

const codeAlphabet = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

func isSharePath(name string) bool {
	if len(name) != 6 {
		return false
	}
	for i := 0; i < 6; i++ {
		c := name[i]
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
			return false
		}
	}
	return true
}

func randToken(n int) string {
	b := make([]byte, n)
	for i := range b {
		v, err := rand.Int(rand.Reader, big.NewInt(int64(len(codeAlphabet))))
		if err != nil {
			// CSPRNG 不可用是系统级故障：宁可响亮地死，不出可预测的凭证
			panic("crypto/rand 不可用: " + err.Error())
		}
		b[i] = codeAlphabet[v.Int64()]
	}
	return string(b)
}

// cleanName 截断控制字符与超长名，防 Content-Disposition / JSON 被撑爆。
func cleanName(s string) string {
	s = strings.ReplaceAll(s, "\\", "/")
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	var b strings.Builder
	for _, r := range s {
		if r >= 32 && r != 127 {
			b.WriteRune(r)
		}
		if b.Len() >= 300 {
			break
		}
	}
	s = strings.TrimSpace(b.String())
	if s == "" {
		s = "未命名文件"
	}
	return s
}

// cleanPath 洗相对路径：'/' 分段，丢弃空段与 . / ..，逐段截控制字符与超长，
// 恒得安全相对路径；结果为空 = 无目录层级。
func cleanPath(s string) string {
	s = strings.ReplaceAll(s, "\\", "/")
	var b strings.Builder
	for _, seg := range strings.Split(s, "/") {
		var sb strings.Builder
		for _, r := range seg {
			if r >= 32 && r != 127 {
				sb.WriteRune(r)
			}
			if sb.Len() >= 120 {
				break
			}
		}
		seg = strings.TrimSpace(sb.String())
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('/')
		}
		b.WriteString(seg)
		if b.Len() >= 500 {
			break
		}
	}
	return b.String()
}

// clientIP：经 caddy 反代时 RemoteAddr 恒为回环，此时 X-Forwarded-For
// 只可能由 caddy 写入，可信；直连场景一律用 RemoteAddr，不喂伪造的 XFF。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return host
	}
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		// caddy 把真实客户端 IP 追加在末尾；取最后一段，客户端自带的伪造段自然丢弃
		if i := strings.LastIndexByte(xf, ','); i >= 0 {
			xf = xf[i+1:]
		}
		if ip := strings.TrimSpace(xf); ip != "" {
			return ip
		}
	}
	return host
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// 建房限速：每 IP 桶容量 10，约每 6 分钟回 1 个。
var buckets = struct {
	sync.Mutex
	m map[string]*bucket
}{m: map[string]*bucket{}}

type bucket struct{ tokens, last float64 }

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func allowCreate(ip string) bool {
	buckets.Lock()
	defer buckets.Unlock()
	now := time.Now().Unix()
	if len(buckets.m) > 4096 { // 慢速过期：只清一小时没动静的冷桶，别把活跃限速一并抹掉
		for k, b := range buckets.m {
			if float64(now)-b.last > 3600 {
				delete(buckets.m, k)
			}
		}
	}
	b := buckets.m[ip]
	if b == nil {
		b = &bucket{tokens: 10, last: float64(now)}
		buckets.m[ip] = b
	}
	if d := float64(now) - b.last; d > 0 {
		b.tokens = minF(10, b.tokens+d/360)
		b.last = float64(now)
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// 探测限速：对码查询（房间不存在）每 IP 容量 3，每 20 秒回 1 个。
var probeBuckets = struct {
	sync.Mutex
	m map[string]*bucket
}{m: map[string]*bucket{}}

func allowProbe(ip string) bool {
	probeBuckets.Lock()
	defer probeBuckets.Unlock()
	now := time.Now().Unix()
	if len(probeBuckets.m) > 4096 { // 同 allowCreate：只清冷桶
		for k, b := range probeBuckets.m {
			if float64(now)-b.last > 3600 {
				delete(probeBuckets.m, k)
			}
		}
	}
	b := probeBuckets.m[ip]
	if b == nil {
		b = &bucket{tokens: 3, last: float64(now)}
		probeBuckets.m[ip] = b
	}
	if d := float64(now) - b.last; d > 0 {
		b.tokens = minF(3, b.tokens+d/20)
		b.last = float64(now)
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

var (
	errRate = errors.New("太频繁")
	errBusy = errors.New("正忙")
)

// ---------- 房间：六位码 ↔ 文件清单 ----------

type fileRef struct {
	Name string `json:"name"`
	Path string `json:"path,omitempty"` // 相对路径（'/' 分隔，不含文件名本身），空 = 无层级
	Size int64  `json:"size"`
	IH   string `json:"ih"` // infohash，40 位小写 hex
}

type room struct {
	code, token string
	owner       string // 建房人 IP：per-IP 房间配额用，建后不变
	mu          sync.Mutex
	files       []fileRef
	created     time.Time
	lastSeen    time.Time
}

type hub struct {
	mu    sync.Mutex
	rooms map[string]*room
}

var hubv = &hub{rooms: map[string]*room{}}

const (
	codeLen       = 6
	maxRooms      = 8
	maxRoomsPerIP = 2 // 单 IP 同时占的房间数：灌房占满 8 席的 DoS 至少要四个来源
	maxFiles      = 64
	roomIdle      = 2 * time.Hour
	roomHardID    = 6 * time.Hour
)

func validIH(s string) bool {
	if len(s) != 40 {
		return false
	}
	for i := 0; i < 40; i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func validRef(f fileRef) bool {
	return validIH(f.IH) && f.Size >= 0 && f.Size < 1<<50
}

// sameIH：清单逐位相同才算同房（顺序即接收端展示序）。
func sameIH(a, b []fileRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].IH != b[i].IH {
			return false
		}
	}
	return true
}

// create：同清单且凭证吻合才原房续用——清单知识（peek 即得）只够「看见」，
// 动房还得有 token，接收方拿清单来换不走别人的房。新房受全局与 per-IP 双配额。
func (h *hub) create(files []fileRef, token, owner string) (*room, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.rooms {
		r.mu.Lock()
		ok := sameIH(r.files, files) && subtle.ConstantTimeCompare([]byte(token), []byte(r.token)) == 1
		if ok { // 刷新恢复：同清单原房续用
			r.lastSeen = time.Now()
			r.files = files
		}
		r.mu.Unlock()
		if ok {
			return r, nil
		}
	}
	perOwner := 0
	for _, r := range h.rooms {
		if r.owner == owner {
			perOwner++
		}
	}
	if len(h.rooms) >= maxRooms || perOwner >= maxRoomsPerIP {
		return nil, errBusy
	}
	r := &room{code: randToken(codeLen), token: randToken(12), owner: owner, files: files,
		created: time.Now(), lastSeen: time.Now()}
	for h.rooms[r.code] != nil {
		r.code = randToken(codeLen)
	}
	h.rooms[r.code] = r
	elog.add("info", "%s 建房（%d 文件）", r.code, len(files))
	return r, nil
}

func (h *hub) get(code string) *room {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.rooms[strings.ToUpper(code)]
	return r
}

func parseRefs(raw []fileRef) ([]fileRef, bool) {
	if len(raw) == 0 || len(raw) > maxFiles {
		return nil, false
	}
	out := make([]fileRef, 0, len(raw))
	seen := map[string]bool{}
	for _, f := range raw {
		if !validRef(f) || seen[f.IH] {
			return nil, false
		}
		seen[f.IH] = true
		out = append(out, fileRef{Name: cleanName(f.Name), Path: cleanPath(f.Path), Size: f.Size, IH: f.IH})
	}
	return out, true
}

func apiCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token string    `json:"token"` // 刷新恢复时随行：同清单且吻合才续用原房
		Files []fileRef `json:"files"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "无效"})
		return
	}
	files, ok := parseRefs(body.Files)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "无效"})
		return
	}
	if !allowCreate(clientIP(r)) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "太频繁"})
		return
	}
	rm, err := hubv.create(files, body.Token, clientIP(r))
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	// 先快照后解锁再写响应：持房间锁写网络，慢客户端会把 janitor（持 hub 锁等房锁）
	// 连带整个房间子系统冻住
	rm.mu.Lock()
	resp := map[string]any{"code": rm.code, "token": rm.token,
		"files": append([]fileRef(nil), rm.files...)}
	rm.mu.Unlock()
	writeJSON(w, http.StatusOK, resp)
}

// 增投文件：建房人凭 token 往同房追加（webtorrent 种子元数据不可变，逐文件一池）。
func apiAdd(w http.ResponseWriter, r *http.Request) {
	rm := hubv.get(r.PathValue("code"))
	if rm == nil {
		if !allowProbe(clientIP(r)) {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "太频繁"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "无效"})
		return
	}
	var body struct {
		Token string    `json:"token"`
		Files []fileRef `json:"files"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "无效"})
		return
	}
	files, ok := parseRefs(body.Files)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "无效"})
		return
	}
	if subtle.ConstantTimeCompare([]byte(body.Token), []byte(rm.token)) != 1 {
		if !allowProbe(clientIP(r)) {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "太频繁"})
			return
		}
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "无效"})
		return
	}
	rm.mu.Lock()
	have := map[string]bool{}
	for _, f := range rm.files {
		have[f.IH] = true
	}
	for _, f := range files {
		if !have[f.IH] {
			if len(rm.files) >= maxFiles {
				break
			}
			have[f.IH] = true
			rm.files = append(rm.files, f)
			elog.add("info", "%s 增投 %s（%s）", rm.code, f.Name, human(f.Size))
		}
	}
	snap := append([]fileRef(nil), rm.files...)
	rm.lastSeen = time.Now()
	rm.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": "1", "files": snap})
}

// 移除已投文件：建房人凭 token 从同房删一个 ih——发送端划走的行不再留给
// 新接收端当幽灵行（已在房里的接收端快照不动，刷新后可见，与增投对称）。
// 幽灵行原样 200（幂等）；清空即拆房。
func apiRemove(w http.ResponseWriter, r *http.Request) {
	rm := hubv.get(r.PathValue("code"))
	if rm == nil {
		if !allowProbe(clientIP(r)) {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "太频繁"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "无效"})
		return
	}
	var body struct {
		Token string `json:"token"`
		IH    string `json:"ih"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&body); err != nil || !validIH(body.IH) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "无效"})
		return
	}
	if subtle.ConstantTimeCompare([]byte(body.Token), []byte(rm.token)) != 1 {
		if !allowProbe(clientIP(r)) {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "太频繁"})
			return
		}
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "无效"})
		return
	}
	// 清空即拆房：判空与摘除在 hub→room 锁序内一次完成——
	// 按「判空解锁再拆」的老写法，解锁后的窗口里 apiAdd 还能往仍在 map 的空房补投，
	// 增投者拿到 200 转头房没了；按码判不按指针判，撞上码复用还会误拆新房
	hubv.mu.Lock()
	rm.mu.Lock()
	kept := rm.files[:0]
	for _, f := range rm.files {
		if f.IH != body.IH {
			kept = append(kept, f)
		}
	}
	removed := len(kept) != len(rm.files)
	if removed {
		rm.files = kept
	}
	// make+copy 而非 append 到 nil：清空拆房时后者序列化成 files:null，
	// 清了也要回空表
	snap := make([]fileRef, len(rm.files))
	copy(snap, rm.files)
	rm.lastSeen = time.Now()
	dropped := len(rm.files) == 0 && hubv.rooms[rm.code] == rm
	if dropped {
		delete(hubv.rooms, rm.code)
	}
	rm.mu.Unlock()
	hubv.mu.Unlock()
	if removed {
		elog.add("info", "%s 移除 %s（余 %d）", rm.code, body.IH[:12], len(snap))
	}
	if dropped {
		elog.add("info", "%s 房间销毁（清空）", rm.code)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": "1", "files": snap})
}

// 对码预检 + 文件清单下发：接收端凭这个渲染行并逐池 client.add。
func apiPeek(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("c")
	if len(code) != codeLen {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "无效"})
		return
	}
	rm := hubv.get(code)
	if rm == nil {
		if !allowProbe(clientIP(r)) {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "太频繁"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "无效"})
		return
	}
	rm.mu.Lock()
	snap := append([]fileRef(nil), rm.files...)
	rm.lastSeen = time.Now()
	rm.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"files": snap})
}

// ---------- 负载采样:P2P 等待窗的服务端依据 ----------
//
// shareweb 决定「P2P 等多久再落 HTTP」(addTorrent 的就绪窗)时从
// /api/health 拿 p2pWaitMs。webseed/zip 正从盘上直出大文件时把窗拉长:
// 那会儿 HTTP 兜底又慢又占磁盘,而 P2P 建链多半只是慢而非死,值得等;
// 空闲时维持短窗,兜底快进快出。

const (
	seedLoadWindow = time.Minute
	waitIdle       = 8 * time.Second
	waitBusy       = 20 * time.Second
	waitHeavy      = 45 * time.Second
)

type seedSample struct {
	at    time.Time
	bytes int64
	dur   time.Duration
}

var seedLoad struct {
	sync.Mutex
	active  int
	samples []seedSample
}

// seedBegin 直出开始时调;返回的收尾在结束后带字节数调。长传输靠 active
// 即时反映,落窗样本负责「刚刚忙过」的余温,免得窗来回跳。
func seedBegin() (finish func(n int64)) {
	seedLoad.Lock()
	seedLoad.active++
	seedLoad.Unlock()
	start := time.Now()
	return func(n int64) {
		now := time.Now()
		seedLoad.Lock()
		defer seedLoad.Unlock()
		seedLoad.active--
		if n > 0 {
			seedLoad.samples = append(seedLoad.samples, seedSample{at: start, bytes: n, dur: now.Sub(start)})
		}
		cutoff := now.Add(-seedLoadWindow)
		i := 0
		for ; i < len(seedLoad.samples) && !seedLoad.samples[i].at.After(cutoff); i++ {
		}
		seedLoad.samples = seedLoad.samples[i:]
	}
}

// p2pWaitFor 是纯策略:active=在途直出数,recentBytes=窗口内出盘字节数。
// 用字节不用吞吐:样本时长趋零时 bytes/dur 会爆表,而盘的压力本来就是
// 「最近读了多少」——512MB/min ≈ 持续 8.7MB/s,128MB/min ≈ 2.2MB/s。
func p2pWaitFor(active, recentBytes int64) time.Duration {
	switch {
	case active >= 3 || recentBytes >= 512<<20:
		return waitHeavy
	case active >= 1 || recentBytes >= 128<<20:
		return waitBusy
	default:
		return waitIdle
	}
}

func seedSnapshot() (active int64, recentBytes int64) {
	seedLoad.Lock()
	defer seedLoad.Unlock()
	active = int64(seedLoad.active)
	cutoff := time.Now().Add(-seedLoadWindow)
	for _, s := range seedLoad.samples {
		if s.at.After(cutoff) {
			recentBytes += s.bytes
		}
	}
	return active, recentBytes
}

func apiHealth(w http.ResponseWriter, _ *http.Request) {
	hubv.mu.Lock()
	n := len(hubv.rooms)
	hubv.mu.Unlock()
	trk.Lock()
	s := len(trk.swarms)
	trk.Unlock()
	active, recentBytes := seedSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "rooms": n, "swarms": s,
		"seedActive": active, "seedBytes": recentBytes,
		"p2pWaitMs": int64(p2pWaitFor(active, recentBytes) / time.Millisecond),
	})
}

// 仅本机维护用：公网入口由 caddy 对 /api/log 直接 404。
// ?tail=N 只看最后 N 条；不加参数全量导出（上限内，可能很大）。
func apiLog(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	tail := 0
	if t := r.URL.Query().Get("tail"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			tail = n
		}
	}
	io.WriteString(w, elog.dump(tail))
}

// ---------- ws tracker（bittorrent-tracker WebSocket 协议） ----------
//
// 协议要点（与 node 版 server.js/parse-websocket.js 逐字对齐）：
//   * info_hash / peer_id / offer_id / to_peer_id 在 JSON 里是「20 字节原始串」——
//     JS 把 0..255 的字节按码点直接塞进字符串再 UTF-8 序列化；Go 端按 rune 还原。
//   * announce 带 offers 时，服务端把每个 offer 转给池内一个其他 peer
//     （{action,offer,offer_id,peer_id,info_hash}），并用 complete/incomplete/interval 应答发起者；
//   * answer 按 to_peer_id 定向转发（{action,answer,offer_id,peer_id,info_hash}），不回执；
//   * 普通 ws 响应不带 peers 数组——连接建立全靠 offer/answer。
//
// 一个连接可属多个池（webtorrent 客户端对同一 tracker URL 共享 socket）；断开时全池清退。

type trkConn struct {
	ws     *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	pids   map[string]string // ihhex → 本连接在该池用的 peerIDhex

	// announce 令牌桶：只在读循环单协程里读写，无需加锁。
	// 容量 256 覆盖 share 开页续种的合法突发（数百种子一次性 announce），
	// 每秒回 2 让脚本灌池只能细水长流
	tokens float64
	last   float64
}

// allowAnnounce：读循环独占调用（trkAnnounce 只从 handleTrackerWS 的循环进入）。
func (c *trkConn) allowAnnounce() bool {
	now := float64(time.Now().Unix())
	if d := now - c.last; d > 0 {
		c.tokens = minF(trkAnnCap, c.tokens+d*trkAnnRate)
		c.last = now
	}
	if c.tokens < 1 {
		return false
	}
	c.tokens--
	return true
}

func (c *trkConn) send(v any) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	if err := c.ws.Write(ctx, websocket.MessageText, data); err != nil {
		c.cancel()
		return false
	}
	return true
}

type trkPeer struct {
	c        *trkConn
	complete bool
	lastSeen time.Time
}

type trkSwarm struct {
	peers map[string]*trkPeer // peerIDhex → peer
}

var trk = struct {
	sync.Mutex
	swarms map[string]*trkSwarm
}{
	swarms: map[string]*trkSwarm{},
}

const (
	trkMaxSwarms     = 512
	trkMaxPeers      = 256
	trkInterval      = 120 // 秒；node 版默认 intervalMs/1000/5
	trkPeerIdle      = 25 * time.Minute
	trkOfferCap      = 8
	trkMaxMsg        = 256 << 10
	trkReadIdleLimit = 15 * time.Minute // 客户端 ~2min 一报，放宽到 15min 容单池静默
	trkAnnCap        = 256              // 单连接 announce 突发容量
	trkAnnRate       = 2                // 单连接 announce 回填速率（个/秒）
)

// binStr2Hex：20 字节原始串 → 40 位 hex。JS 端每字节 = 一个码点。
func binStr2Hex(s string) (string, bool) {
	rs := []rune(s)
	if len(rs) != 20 {
		return "", false
	}
	b := make([]byte, 20)
	for i, r := range rs {
		if r > 255 {
			return "", false
		}
		b[i] = byte(r)
	}
	return hex.EncodeToString(b), true
}

type trkOffer struct {
	Offer   json.RawMessage `json:"offer"`
	OfferID string          `json:"offer_id"`
}

type trkMsg struct {
	Action   string          `json:"action"`
	InfoHash json.RawMessage `json:"info_hash"` // announce: 20 字节原始串；scrape: 原始串或其数组
	PeerID   string          `json:"peer_id"`
	ToPeerID string          `json:"to_peer_id"`
	OfferID  string          `json:"offer_id"`
	Event    string          `json:"event"`
	Left     *float64        `json:"left"`
	Answer   json.RawMessage `json:"answer"`
	Offers   []trkOffer      `json:"offers"`
}

// rawStr：RawMessage → JSON 字符串。
func rawStr(r json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(r, &s); err != nil {
		return "", false
	}
	return s, true
}

func trkFail(c *trkConn, ih string, msg string) {
	c.send(map[string]any{"action": "announce", "failure reason": msg, "info_hash": ih})
}

func getSwarm(ihhex string) (*trkSwarm, bool) {
	s := trk.swarms[ihhex]
	return s, s != nil
}

func ensureSwarm(ihhex string) (*trkSwarm, error) {
	if s, ok := trk.swarms[ihhex]; ok {
		return s, nil
	}
	if len(trk.swarms) >= trkMaxSwarms {
		return nil, errBusy
	}
	s := &trkSwarm{peers: map[string]*trkPeer{}}
	trk.swarms[ihhex] = s
	return s, nil
}

func swarmCounts(s *trkSwarm) (complete, incomplete int) {
	for _, p := range s.peers {
		if p.complete {
			complete++
		} else {
			incomplete++
		}
	}
	return
}

func removePeer(ihhex, pidhex string) {
	if s, ok := getSwarm(ihhex); ok {
		delete(s.peers, pidhex)
		if len(s.peers) == 0 {
			delete(trk.swarms, ihhex)
		}
	}
}

// pickOthers：池内随机取 n 个非自身 peer；不足则有多少给多少（与 node 版 peers.forEach 对齐）。
func pickOthers(s *trkSwarm, self string, n int) []*trkPeer {
	ids := make([]string, 0, len(s.peers))
	for id := range s.peers {
		if id != self {
			ids = append(ids, id)
		}
	}
	for i := len(ids) - 1; i > 0; i-- {
		jv, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		j := int(jv.Int64())
		ids[i], ids[j] = ids[j], ids[i]
	}
	if len(ids) > n {
		ids = ids[:n]
	}
	out := make([]*trkPeer, 0, len(ids))
	for _, id := range ids {
		if p := s.peers[id]; p != nil {
			out = append(out, p)
		}
	}
	return out
}

func handleTrackerWS(w http.ResponseWriter, r *http.Request) {
	// 经 caddy 反代后 Origin/Host 可能不一致；本服务无 cookie 凭证，码即凭证，放行跨源
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ws.SetReadLimit(trkMaxMsg) // 超限帧直接断连：别等整帧读进内存才发现超标
	ctx, cancel := context.WithCancel(context.Background())
	c := &trkConn{ws: ws, ctx: ctx, cancel: cancel, pids: map[string]string{},
		tokens: trkAnnCap, last: float64(time.Now().Unix())}
	defer cancel()

	elog.add("info", "tracker 连接 %s", clientIP(r))
	defer func() {
		trk.Lock()
		for ih, pid := range c.pids {
			removePeer(ih, pid)
		}
		n := len(c.pids) // janitor 会并发改 c.pids，计数在锁内取
		trk.Unlock()
		elog.add("info", "tracker 断开 %s（池 %d）", clientIP(r), n)
	}()

	for {
		readCtx, rc := context.WithTimeout(c.ctx, trkReadIdleLimit)
		_, data, err := c.ws.Read(readCtx)
		rc()
		if err != nil {
			return
		}
		trkHandleMsg(c, data)
	}
}

func trkHandleMsg(c *trkConn, data []byte) {
	var m trkMsg
	if err := json.Unmarshal(data, &m); err != nil {
		return
	}
	switch m.Action {
	case "announce":
		trkAnnounce(c, &m)
	case "scrape":
		trkScrape(c, &m)
	}
}

func trkAnnounce(c *trkConn, m *trkMsg) {
	ihRaw, ok := rawStr(m.InfoHash)
	if !ok {
		return
	}
	ihhex, ok := binStr2Hex(ihRaw)
	pidhex, pok := binStr2Hex(m.PeerID)
	if !ok || !pok {
		return
	}
	event := m.Event
	if event == "" || event == "empty" {
		event = "update"
	}
	ihBin := ihRaw
	pidBin := m.PeerID

	if !c.allowAnnounce() {
		trkFail(c, ihBin, "太频繁")
		return
	}

	if event == "stopped" {
		trk.Lock()
		_, wasIn := getSwarm(ihhex) // wasIn: 池此前存在（不代表本 peer 在内，但 leave 只在有效停止时可见）
		removePeer(ihhex, pidhex)
		delete(c.pids, ihhex)
		s, _ := getSwarm(ihhex)
		var comp, inc int
		if s != nil {
			comp, inc = swarmCounts(s)
		}
		trk.Unlock()
		if wasIn {
			elog.add("info", "池 %s peer- peer %s（seed=%d leech=%d）", ihhex[:12], pidhex[:12], comp, inc)
		}
		c.send(map[string]any{"action": "announce", "interval": trkInterval,
			"info_hash": ihBin, "complete": comp, "incomplete": inc})
		return
	}

	trk.Lock()
	s, err := ensureSwarm(ihhex)
	if err != nil {
		trk.Unlock()
		trkFail(c, ihBin, "正忙")
		return
	}
	prev, existed := s.peers[pidhex]
	if !existed && len(s.peers) >= trkMaxPeers {
		trk.Unlock()
		trkFail(c, ihBin, "正忙")
		return
	}
	// 同连接同池换 peer_id（种子毁掉重加）：旧身先清——
	// 留着会被 janitor 当 idle 滞留 25 分钟，届时连坐 cancel 掉整条活连接。
	// 旧身若是池内最后一人，removePeer 会连池一起摘掉，下面新身写进的就是
	// 孤儿对象（池从 map 消失，后续 announce 另起新池看不见这人）——挂回去。
	// 全程持 trk 锁，挂回不会突破 trkMaxSwarms。
	if old, dup := c.pids[ihhex]; dup && old != pidhex {
		removePeer(ihhex, old)
		if trk.swarms[ihhex] == nil {
			trk.swarms[ihhex] = s
		}
	}
	wasComplete := existed && prev.complete
	complete := event == "completed" || (m.Left != nil && *m.Left == 0)
	if existed {
		complete = prev.complete || complete
	}
	s.peers[pidhex] = &trkPeer{c: c, complete: complete, lastSeen: time.Now()}
	c.pids[ihhex] = pidhex

	// 选出本批 offers 的接收者（锁内取快照，锁外发送）
	var targets []*trkPeer
	if len(m.Offers) > 0 {
		n := len(m.Offers)
		if n > trkOfferCap {
			n = trkOfferCap
		}
		targets = pickOthers(s, pidhex, n)
	}
	var comp, inc = swarmCounts(s)
	trk.Unlock()

	// 池内动态只记「变化」：新 peer、转完成；每分钟的心跳 announce 不落日志
	role := "leech"
	if complete {
		role = "seed"
	}
	switch {
	case !existed:
		elog.add("info", "池 %s %s+ peer %s（seed=%d leech=%d）", ihhex[:12], role, pidhex[:12], comp, inc)
	case complete && !wasComplete:
		elog.add("info", "池 %s 完成 peer %s（seed=%d leech=%d）", ihhex[:12], pidhex[:12], comp, inc)
	}

	// answer 定向转发，不回执
	if len(m.Answer) > 0 {
		toidhex, ok := binStr2Hex(m.ToPeerID)
		if !ok {
			return
		}
		trk.Lock()
		to := s.peers[toidhex]
		trk.Unlock()
		if to == nil {
			elog.add("warn", "answer 无主 %s", ihhex[:12])
			return
		}
		to.c.send(map[string]any{"action": "announce", "answer": json.RawMessage(m.Answer),
			"offer_id": m.OfferID, "peer_id": pidBin, "info_hash": ihBin})
		return
	}

	// offer 广播：offers[i] → targets[i]
	n := len(targets)
	for i := 0; i < n; i++ {
		targets[i].c.send(map[string]any{"action": "announce",
			"offer": m.Offers[i].Offer, "offer_id": m.Offers[i].OfferID,
			"peer_id": pidBin, "info_hash": ihBin})
	}
	if n > 0 {
		elog.add("info", "池 %s offer×%d ← %s", ihhex[:12], n, pidhex[:12])
	}

	c.send(map[string]any{"action": "announce", "interval": trkInterval,
		"info_hash": ihBin, "complete": comp, "incomplete": inc})
}

func trkScrape(c *trkConn, m *trkMsg) {
	var raws []string
	if len(m.InfoHash) > 0 {
		var one string
		if json.Unmarshal(m.InfoHash, &one) == nil {
			raws = []string{one}
		} else {
			json.Unmarshal(m.InfoHash, &raws)
		}
	}
	files := map[string]any{}
	for _, raw := range raws {
		ihhex, ok := binStr2Hex(raw)
		if !ok {
			continue
		}
		trk.Lock()
		s, ok := getSwarm(ihhex)
		var comp, inc int
		if ok {
			comp, inc = swarmCounts(s)
		}
		trk.Unlock()
		files[raw] = map[string]int{"complete": comp, "incomplete": inc, "downloaded": comp}
	}
	c.send(map[string]any{"action": "scrape", "files": files})
}

func trkJanitor() {
	for range time.Tick(5 * time.Minute) {
		now := time.Now()
		trk.Lock()
		doomed := map[*trkConn]bool{}
		for ih, s := range trk.swarms {
			for pid, p := range s.peers {
				if now.Sub(p.lastSeen) > trkPeerIdle {
					delete(s.peers, pid)
					doomed[p.c] = true
					delete(p.c.pids, ih)
				}
			}
			if len(s.peers) == 0 {
				delete(trk.swarms, ih)
			}
		}
		trk.Unlock()
		for c := range doomed {
			c.cancel()
		}
	}
}

// ---------- share manifest：逐文件 .torrent 清单 ----------

// pieceLenPolicy：教室这点做种人，块按「几十块/文件」来——少 OPFS 文件、少校验。
func pieceLenPolicy(size int64) int64 {
	switch {
	case size < 64<<20:
		return 4 << 20
	case size < 512<<20:
		return 16 << 20
	default:
		return 32 << 20
	}
}

// bencode 极简编码器（只需 dict/int/string）。
func bPutInt(b *bytes.Buffer, i int64) {
	fmt.Fprintf(b, "i%de", i)
}

func bPutStr(b *bytes.Buffer, s []byte) {
	fmt.Fprintf(b, "%d:", len(s))
	b.Write(s)
}

func bPutKey(b *bytes.Buffer, k string) {
	bPutStr(b, []byte(k))
}

// fileTorrent 为单个文件造一份 v1 种子：announce/url-list 在顶层（不影响 infohash）。
// 返回 infohash hex 与完整种子 bencode 字节。读完全程对账总长——
// Stat 与读完之间文件被改，length 与 pieces 就对不上，宁失败不出坏种。
func fileTorrent(path string, size int64, name, announce, webseed string) (ihhex string, torrent []byte, err error) {
	pieceLen := pieceLenPolicy(size)
	f, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer f.Close()

	pieces := &bytes.Buffer{}
	buf := make([]byte, pieceLen)
	h := sha1.New()
	var total int64
	for {
		n, rerr := io.ReadFull(f, buf)
		total += int64(n)
		if n > 0 {
			h.Reset()
			h.Write(buf[:n])
			pieces.Write(h.Sum(nil))
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return "", nil, rerr
		}
	}
	if total != size {
		return "", nil, fmt.Errorf("%s: 哈希期间大小变了（%d != %d）", path, total, size)
	}

	var info bytes.Buffer
	info.WriteByte('d')
	bPutKey(&info, "length")
	bPutInt(&info, size)
	bPutKey(&info, "name")
	bPutStr(&info, []byte(name))
	bPutKey(&info, "piece length")
	bPutInt(&info, pieceLen)
	bPutKey(&info, "pieces")
	bPutStr(&info, pieces.Bytes())
	info.WriteByte('e')

	sum := sha1.Sum(info.Bytes())
	ihhex = hex.EncodeToString(sum[:])

	var t bytes.Buffer
	t.WriteByte('d')
	bPutKey(&t, "announce")
	bPutStr(&t, []byte(announce))
	bPutKey(&t, "info")
	t.Write(info.Bytes())
	if webseed != "" { // BEP-19：url-list 是字符串列表；默认空（webseed 由前端按站点根动态给）就整个省略
		bPutKey(&t, "url-list")
		t.WriteByte('l')
		bPutStr(&t, []byte(webseed))
		t.WriteByte('e')
	}
	t.WriteByte('e')
	return ihhex, t.Bytes(), nil
}

type manifestEntry struct {
	Path    string `json:"path"` // 相对路径，'/' 分隔（URL 由前端转义）
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	Mtime   int64  `json:"mtime"` // unix 纳秒；增量重扫的复用判据之一（秒粒度会漏同秒同长的重写）
	Ann     string `json:"ann"`   // 生成时的 tracker；换了就整条重哈希
	WS      string `json:"ws"`    // 生成时的 webseed 前缀
	Fm      int    `json:"fm"`    // 种子格式版本；与当前不等就重生成
	IH      string `json:"ih"`
	Torrent string `json:"torrent"` // bencode 种子的 base64
}

const manifestSkipDir = "shareapp" // 输出目录若落在 root 内则跳过
const manifestFmt = 5              // v5：教室量级大块（32MB 封顶）

// genManifest 生成逐文件种子清单；增量：out 已存在时，path+size+mtime 未变的
// 条目直接复用（不重读不重哈希），只哈希新增/变更文件，删除消失的条目。
// 因此「share 站点扩充文件」= 重跑同一条命令，只有新文件按盘速过一遍。
func genManifest(root, out, announce, webseedBase string) error {
	old := map[string]manifestEntry{}
	if b, err := os.ReadFile(out); err == nil {
		var prev []manifestEntry
		if json.Unmarshal(b, &prev) == nil {
			for _, e := range prev {
				old[e.Path] = e
			}
		}
	}
	var entries []manifestEntry
	reused, hashed := 0, 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == manifestSkipDir && p != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		mtime := info.ModTime().UnixNano()
		if e, ok := old[rel]; ok && e.Fm == manifestFmt && e.Size == info.Size() && e.Mtime == mtime && e.Ann == announce && e.WS == webseedBase {
			entries = append(entries, e)
			reused++
			return nil
		}
		webseed := "" // 默认不烙 url-list，前端按当前站点根动态给
		if webseedBase != "" {
			webseed = webseedBase + rel
		}
		ih, tor, err := fileTorrent(p, info.Size(), d.Name(), announce, webseed)
		if err != nil {
			elog.add("warn", "manifest 跳过 %s: %v", rel, err)
			return nil
		}
		entries = append(entries, manifestEntry{
			Path: rel, Name: d.Name(), Size: info.Size(), Mtime: mtime,
			Ann: announce, WS: webseedBase, Fm: manifestFmt, IH: ih,
			Torrent: base64.StdEncoding.EncodeToString(tor),
		})
		hashed++
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(out); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	// tmp 带 pid：两条 -manifest 并发跑不会互踩对方的半成品
	tmp := fmt.Sprintf("%s.%d.tmp", out, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	var total int64
	for _, e := range entries {
		total += e.Size
	}
	elog.add("info", "manifest %d 文件 %s → %s（复用 %d，新哈希 %d）", len(entries), human(total), out, reused, hashed)
	fmt.Printf("manifest: %d 文件（复用 %d，新哈希 %d）→ %s\n", len(entries), reused, hashed, out)
	return os.Rename(tmp, out)
}

// ---------- 清扫 ----------

func janitor() {
	for range time.Tick(20 * time.Second) {
		now := time.Now()
		hubv.mu.Lock()
		var dead []*room
		for code, r := range hubv.rooms {
			r.mu.Lock()
			expired := now.Sub(r.created) > roomHardID || now.Sub(r.lastSeen) > roomIdle
			r.mu.Unlock()
			if expired {
				// 判定与摘除同锁完成：免得「判死→解锁→被 peek 救活→仍被拆」的窗口
				delete(hubv.rooms, code)
				dead = append(dead, r)
			}
		}
		hubv.mu.Unlock()
		for _, r := range dead {
			elog.add("info", "%s 房间销毁（超时清理）", r.code)
		}
	}
}

// ---------- 静态资源 ----------

func main() {
	addr := flag.String("addr", "127.0.0.1:8791", "监听地址")
	debug := flag.Bool("debug", false, "调试模式（禁用静态文件缓存）")
	manifestRoot := flag.String("manifest", "", "生成清单：share 根目录（生成后退出；增量复用未变文件）")
	manifestOut := flag.String("out", "", "清单输出路径")
	announce := flag.String("announce", "", "种子默认 tracker（默认空：前端 addTorrent 自带站点根 tracker，只有给外部 BT 客户端用时才需要烙）")
	webseed := flag.String("webseed", "", "烙进种子的 url-list（默认空：webseed 由前端按当前站点根动态给出，换基准不废清单）")
	mfile := flag.String("mfile", "", "运行期在 /app/manifest.json 提供的清单文件路径")
	webroot := flag.String("webroot", "", "share 文件根目录（启用 share 主机分流，站点根路径即 webseed，带访问日志）")
	logmax := flag.Int("logmax", 25<<20, "内存日志字节上限")
	flag.Parse()
	elog.max = *logmax

	if *manifestRoot != "" {
		out := *manifestOut
		if out == "" {
			log.Fatal("-out 必填")
		}
		if err := genManifest(*manifestRoot, out, *announce, *webseed); err != nil {
			log.Fatal(err)
		}
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /tracker", handleTrackerWS)
	mux.HandleFunc("POST /api/rooms", apiCreate)
	mux.HandleFunc("POST /api/rooms/{code}", apiAdd)
	mux.HandleFunc("POST /api/rooms/{code}/remove", apiRemove)
	mux.HandleFunc("GET /api/code", apiPeek)
	mux.HandleFunc("GET /api/health", apiHealth)
	mux.HandleFunc("GET /api/log", apiLog)

	// share 前端（/app/）：嵌入的 shareweb + 磁盘上的 manifest.json
	var shareFSv fs.FS
	if fi, err := os.Stat("shareweb/index.html"); err == nil && !fi.IsDir() {
		shareFSv = os.DirFS("shareweb")
	} else {
		sub, err := fs.Sub(embedded, "shareweb")
		if err != nil {
			log.Fatal(err)
		}
		shareFSv = sub
	}
	shareStatic := http.FileServer(http.FS(shareFSv))
	serveManifest := func(w http.ResponseWriter, r *http.Request) {
		if *mfile == "" {
			http.Error(w, "manifest 未配置", 404)
			return
		}
		// no-cache = 可存但要回源校验：ServeFile 带 Last-Modified，没变回 304，
		// 清单（几百 KB 的 .torrent b64）只在库真正变化时才重传
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeFile(w, r, *mfile)
	}
	mux.HandleFunc("GET /app/manifest.json", serveManifest)
	mux.HandleFunc("GET /app/", func(w http.ResponseWriter, r *http.Request) {
		noStore(w, *debug)
		http.StripPrefix("/app/", shareStatic).ServeHTTP(w, r)
	})
	if *webroot != "" {
		mux.HandleFunc("GET /f/{path...}", webseedHandler(*webroot)) // 旧基准兼容
	}

	// drop 前端（/ 与 /{CODE}）
	var webFS fs.FS
	isDev := false
	if fi, err := os.Stat("web/index.html"); err == nil && !fi.IsDir() {
		webFS = os.DirFS("web")
		isDev = true
	} else {
		sub, err := fs.Sub(embedded, "web")
		if err != nil {
			log.Fatal(err)
		}
		webFS = sub
	}
	noCache := *debug || isDev
	static := http.FileServer(http.FS(webFS))
	serveStatic := func(w http.ResponseWriter, r *http.Request, fn func()) {
		if noCache {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			r.Header.Del("If-Modified-Since")
			r.Header.Del("If-None-Match")
		} else {
			// 生产必须显式给缓存策略，否则 webview 按启发式缓存咬住旧 bundle 不放，
			// 前端更新到不了微信/QQ 这些内置浏览器。HTML/JS/CSS 每次回源
			// （embed 无 Last-Modified，no-cache 即全量重取，LAN 下无感）；字体基本不变给长缓存。
			if strings.HasSuffix(r.URL.Path, ".woff2") {
				w.Header().Set("Cache-Control", "max-age=2592000, immutable")
			} else {
				w.Header().Set("Cache-Control", "no-cache")
			}
		}
		fn()
	}
	// 404 终局页：自包含的 web/404.html（内联样式，不再引用任何资源——
	// 404 页自己不许再 404），状态码是真 404。
	serveDrop404 := func(w http.ResponseWriter, r *http.Request) {
		f, err := webFS.Open("404.html")
		if err != nil {
			http.Error(w, "404 page not found", 404)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusNotFound)
		io.Copy(w, f)
	}
	// drop 静态命中：真实文件才交给 FileServer（顺带关掉目录列表），其余一律
	// 回 404 终局页——此前落 FileServer 的纯文本 404，浏览器里近乎白页。
	serveDropFile := func(w http.ResponseWriter, r *http.Request) {
		name := strings.Trim(path.Clean(r.URL.Path), "/")
		if fi, err := fs.Stat(webFS, name); err == nil && fi.Mode().IsRegular() {
			serveStatic(w, r, func() {
				static.ServeHTTP(w, r)
			})
			return
		}
		serveDrop404(w, r)
	}
	mux.HandleFunc("GET /{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if isSharePath(name) { // /{CODE}：接收端深链，回 index.html
			serveStatic(w, r, func() {
				http.ServeFileFS(w, r, webFS, "index.html")
			})
			return
		}
		serveDropFile(w, r)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			serveStatic(w, r, func() {
				http.ServeFileFS(w, r, webFS, "index.html")
			})
			return
		}
		serveDropFile(w, r)
	})

	// share 主机（webroot 启用时）：站点根即 webseed + 做种页。
	// / 与目录路径 → 做种 SPA；存在的文件 → 带日志直出（这就是 webseed 本身，
	// 页面里的下载与任何 HTTP 客户端的直接访问是同一条 URL）；/api/* 一律 404。
	var handler http.Handler = mux
	if *webroot != "" {
		smux := http.NewServeMux()
		smux.HandleFunc("GET /tracker", handleTrackerWS)
		smux.HandleFunc("GET /app/manifest.json", serveManifest)
		// share 站的 /api/* 默认全 404,单放 /api/health:前端要从这里
		// 拿 p2pWaitMs 决定「P2P 等多久再落 HTTP」(同源,不跨站)
		smux.HandleFunc("GET /api/health", apiHealth)
		smux.HandleFunc("GET /app/", func(w http.ResponseWriter, r *http.Request) {
			noStore(w, *debug)
			http.StripPrefix("/app/", shareStatic).ServeHTTP(w, r)
		})
		smux.HandleFunc("GET /f/{path...}", webseedHandler(*webroot)) // 旧基准兼容
		smux.HandleFunc("GET /api/", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "404 page not found", 404)
		})
		serveShareSPA := func(w http.ResponseWriter, r *http.Request) {
			noStore(w, *debug)
			http.ServeFileFS(w, r, shareFSv, "index.html")
		}
		smux.HandleFunc("GET /{$}", serveShareSPA)
		smux.HandleFunc("GET /{path...}", func(w http.ResponseWriter, r *http.Request) {
			// 目录 URL 带尾斜杠（/课本/），而 io/fs 的合法路径不带——先统一去掉
			name := strings.Trim(r.PathValue("path"), "/")
			if name == "" {
				serveShareSPA(w, r)
				return
			}
			fi, err := fs.Stat(os.DirFS(*webroot), name)
			if err == nil && fi.Mode().IsRegular() {
				webseedHandler(*webroot)(w, r)
				return
			}
			// 目录 ?zip=1：流式打包（Store 不压缩，USB 盘顺序读一轮，CPU 几乎不动）
			// 给无目录选择能力的浏览器（手机/Firefox/Safari）当「下载文件夹」兜底
			if err == nil && fi.IsDir() && r.URL.Query().Get("zip") == "1" {
				zipHandler(*webroot)(w, r)
				return
			}
			serveShareSPA(w, r)
		})
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.Host, "share.") {
				smux.ServeHTTP(w, r)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}

	go janitor()
	go trkJanitor()
	elog.add("info", "droprelay(webtorrent) 启动，监听 %s（日志上限 %s，webroot %s）", *addr, human(int64(*logmax)), orDefault(*webroot, "未启用"))
	fmt.Println("droprelay listening on", *addr)
	log.Fatal((&http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 15 * time.Second}).ListenAndServe())
}

// webseedHandler：文件由本进程直出并逐笔记日志——页面里的下载与任何 HTTP
// 客户端的直接访问是同一条 URL（站点根），这就是「/ 的默认 webseed」。
func webseedHandler(root string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := strings.Trim(r.PathValue("path"), "/")
		fsys := os.DirFS(root)
		fi, err := fs.Stat(fsys, name)
		if err != nil || !fi.Mode().IsRegular() {
			http.Error(w, "404 page not found", 404)
			return
		}
		start := time.Now()
		cw := &countWriter{ResponseWriter: w}
		finish := seedBegin()
		http.ServeFileFS(cw, r, fsys, name)
		finish(cw.n)
		elog.add("info", "webseed %s %dB %s（%s）", name, cw.n, r.Header.Get("Range"), time.Since(start).Round(time.Millisecond))
	}
}

// zipHandler：把 webroot 下的一个目录流式打成 zip（Store 存储）。
// 客户端边收边落盘，服务端不落任何临时文件；中文目录名走 RFC 5987 filename*。
func zipHandler(root string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := strings.Trim(r.PathValue("path"), "/")
		if name == "" {
			http.Error(w, "404 page not found", 404)
			return
		}
		fsys := os.DirFS(root)
		if fi, err := fs.Stat(fsys, name); err != nil || !fi.IsDir() {
			http.Error(w, "404 page not found", 404)
			return
		}
		// 预检只走 stat 不读数据：坏条目（被删/权限）在写响应头之前暴露——
		// 头一旦发出就无法改口，中途断掉只会让用户拿到 200 的半截坏 zip。
		// zip 的 Content-Length 拿不出准确值（要么整包预读一遍盘，要么按
		// Go 的头/描述符编码逐字节复算），继续 chunked 流式，宁可没总进度
		// 也不把大目录的等待时间翻倍。
		if err := fs.WalkDir(fsys, name, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			_, err = d.Info()
			return err
		}); err != nil {
			elog.add("warn", "zip %s 预检失败：%v", name, err)
			http.Error(w, "500 directory unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition",
			mime.FormatMediaType("attachment", map[string]string{"filename": path.Base(name) + ".zip"}))
		start := time.Now()
		cw := &countWriter{ResponseWriter: w}
		finish := seedBegin()
		defer func() { finish(cw.n) }()
		zw := zip.NewWriter(cw)
		var files int
		walkErr := fs.WalkDir(fsys, name, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			rc, err := fsys.Open(p)
			if err != nil {
				return err
			}
			defer rc.Close()
			h := &zip.FileHeader{Name: strings.TrimPrefix(p, name+"/"), Method: zip.Store, Modified: info.ModTime()}
			fw, err := zw.CreateHeader(h)
			if err != nil {
				return err
			}
			if _, err := io.Copy(fw, rc); err != nil {
				return err
			}
			files++
			return nil
		})
		if cerr := zw.Close(); walkErr == nil {
			walkErr = cerr
		}
		if walkErr != nil {
			elog.add("warn", "zip %s 中断：%v", name, walkErr)
			return
		}
		elog.add("info", "zip %s %d 文件 %s（%s）", name, files, human(int64(cw.n)), time.Since(start).Round(time.Millisecond))
	}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func noStore(w http.ResponseWriter, force bool) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	if force {
		w.Header().Set("Pragma", "no-cache")
	}
}

// countWriter 只为 webseed 访问日志计字节数。
type countWriter struct {
	http.ResponseWriter
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n += int64(n)
	return n, err
}
