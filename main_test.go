// main_test.go —— 服务端第 0/1 层暴力测试：
//  1. 纯函数真值表（clean/cleanPath/parseRefs/fileTorrent/…）
//  2. 限速与配额回归（含 v6.1 冷桶清扫、v6.3 token 恒时比较等已修窗口的防复发）
//  3. ws tracker 单元 + 集成（offer/answer 中继、peer/swarm 上限、超限帧）
//  4. HTTP 面滥用（体上限、webseed Range 边界、zip 往返、并发建房竞态）
//  5. hub + tracker 并发混沌（go test -race 下跑，专抓锁序回归）
//
// 全部走包内全局（hubv/trk/buckets），每个测试开头自行重置，测试间无顺序依赖。
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ---------- 帮手 ----------

func resetHub() {
	hubv.mu.Lock()
	hubv.rooms = map[string]*room{}
	hubv.mu.Unlock()
}

func resetTrk() {
	trk.Lock()
	trk.swarms = map[string]*trkSwarm{}
	trk.Unlock()
}

func resetBuckets() {
	buckets.Lock()
	buckets.m = map[string]*bucket{}
	buckets.Unlock()
	probeBuckets.Lock()
	probeBuckets.m = map[string]*bucket{}
	probeBuckets.Unlock()
}

func resetAll() {
	resetHub()
	resetTrk()
	resetBuckets()
}

// ihHex：由 seed 确定性生成 40 位小写 hex（infohash 夹具）。
func ihHex(seed byte) string {
	b := make([]byte, 20)
	for i := range b {
		b[i] = seed + byte(i)*7
	}
	return hex.EncodeToString(b)
}

// jsRaw20：同一组字节的「JS 式 20 码点原始串」（ws 协议里 info_hash/peer_id 的线格式）。
// binStr2Hex(jsRaw20(seed)) == ihHex(seed)。
func jsRaw20(seed byte) string {
	rs := make([]rune, 20)
	for i := range rs {
		rs[i] = rune(seed + byte(i)*7) // seed ≤ 100 时恒 ≤ 255
	}
	return string(rs)
}

func ref(seed byte, size int64) fileRef {
	return fileRef{Name: fmt.Sprintf("f%d.bin", seed), Size: size, IH: ihHex(seed)}
}

func refsJSON(fs ...fileRef) string {
	b, _ := json.Marshal(fs)
	return string(b)
}

// apiServer：与 main() 同款路由，仅挂房间 API。经 httptest 直连即回环，
// clientIP 会信任 XFF 末段——测试用「伪造段, 真实 IP」两段式同时钉住取末段行为。
func apiServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/rooms", apiCreate)
	mux.HandleFunc("POST /api/rooms/{code}", apiAdd)
	mux.HandleFunc("POST /api/rooms/{code}/remove", apiRemove)
	mux.HandleFunc("GET /api/code", apiPeek)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// doJSON：发一个带 XFF 的请求并解析 JSON 响应。可在测试 goroutine 之外用（不 Fatal）。
func doJSON(srv *httptest.Server, xff, method, path, body string) (int, map[string]any, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if xff != "" {
		req.Header.Set("X-Forwarded-For", "fake.spoof, "+xff)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	m := map[string]any{}
	_ = json.Unmarshal(raw, &m)
	return resp.StatusCode, m, nil
}

func postJSON(t *testing.T, srv *httptest.Server, xff, method, path, body string) (int, map[string]any) {
	t.Helper()
	st, m, err := doJSON(srv, xff, method, path, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return st, m
}

func createRoom(t *testing.T, srv *httptest.Server, ip string, files ...fileRef) (code, token string) {
	t.Helper()
	st, m := postJSON(t, srv, ip, "POST", "/api/rooms", `{"files":`+refsJSON(files...)+`}`)
	if st != http.StatusOK {
		t.Fatalf("建房失败: %d %v", st, m)
	}
	code, _ = m["code"].(string)
	token, _ = m["token"].(string)
	if len(code) != codeLen || token == "" {
		t.Fatalf("建房响应异常: %v", m)
	}
	return code, token
}

// presentRoom：出示指定 token 建房——刷新恢复路径（同清单 + 真 token → 原房续用）。
// 注意：新建房时传入的 token 会被忽略，房 token 恒由服务端 randToken 铸造并在响应里下发。
func presentRoom(t *testing.T, srv *httptest.Server, ip, token string, files ...fileRef) (int, map[string]any) {
	t.Helper()
	return postJSON(t, srv, ip, "POST", "/api/rooms", fmt.Sprintf(`{"token":%q,"files":%s}`, token, refsJSON(files...)))
}

func addFiles(t *testing.T, srv *httptest.Server, ip, code, token string, files ...fileRef) (int, map[string]any) {
	t.Helper()
	return postJSON(t, srv, ip, "POST", "/api/rooms/"+code,
		fmt.Sprintf(`{"token":%q,"files":%s}`, token, refsJSON(files...)))
}

func removeFile(t *testing.T, srv *httptest.Server, ip, code, token, ih string) (int, map[string]any) {
	t.Helper()
	return postJSON(t, srv, ip, "POST", "/api/rooms/"+code+"/remove",
		fmt.Sprintf(`{"token":%q,"ih":%q}`, token, ih))
}

func peekRoom(t *testing.T, srv *httptest.Server, ip, code string) (int, map[string]any) {
	t.Helper()
	return postJSON(t, srv, ip, "GET", "/api/code?c="+code, "")
}

func roomFiles(m map[string]any) []any {
	fs, _ := m["files"].([]any)
	return fs
}

func num(m map[string]any, k string) float64 {
	f, _ := m[k].(float64)
	return f
}

// ---------- 纯函数真值表 ----------

func TestIsSharePath(t *testing.T) {
	for _, s := range []string{"ABC123", "000000", "az09Az"} {
		if !isSharePath(s) {
			t.Errorf("isSharePath(%q) 应为真", s)
		}
	}
	for _, s := range []string{"", "ABC12", "ABC1234", "ABC12!", "ABC12 ", "ABC-23"} {
		if isSharePath(s) {
			t.Errorf("isSharePath(%q) 应为假", s)
		}
	}
}

func TestHuman(t *testing.T) {
	cases := map[int64]string{
		0: "0B", 1023: "1023B", 1024: "1.0KB", 1536: "1.5KB",
		1048576: "1.0MB", 1073741824: "1.00GB", 2684354560: "2.50GB",
	}
	for n, want := range cases {
		if got := human(n); got != want {
			t.Errorf("human(%d) = %q，应为 %q", n, got, want)
		}
	}
}

func TestRandToken(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		s := randToken(12)
		if len(s) != 12 {
			t.Fatalf("randToken(12) 长度 %d", len(s))
		}
		for _, c := range s {
			if !strings.ContainsRune(codeAlphabet, c) {
				t.Fatalf("randToken 出现字符集外字符 %q", c)
			}
		}
		seen[s] = true
	}
	if len(seen) != 200 {
		t.Errorf("200 次取样仅 %d 个不同，疑似熵源异常", len(seen))
	}
	if randToken(0) != "" {
		t.Error("randToken(0) 应为空串")
	}
}

func TestCleanName(t *testing.T) {
	cases := []struct{ in, want string }{
		{`C:\Users\张三\报告 最终版.txt`, "报告 最终版.txt"},
		{"正常名.txt", "正常名.txt"},
		{"  a\x01b\x7fc  ", "abc"},
		{strings.Repeat("a", 400), strings.Repeat("a", 300)},
		{"", "未命名文件"},
		{"\x01\x02", "未命名文件"},
		{"   ", "未命名文件"},
	}
	for _, c := range cases {
		if got := cleanName(c.in); got != c.want {
			t.Errorf("cleanName(%q) = %q，应为 %q", c.in, got, c.want)
		}
	}
	// 300 上限按字节计：200 个「中」= 600 字节，截到 300 字节 = 100 个 rune
	got := []rune(cleanName(strings.Repeat("中", 200)))
	if len(got) != 100 {
		t.Errorf("多字节截断得 %d 个 rune，应为 100", len(got))
	}
}

func TestCleanPath(t *testing.T) {
	cases := []struct{ in, want string }{
		{`..\..\etc\passwd`, "etc/passwd"},
		{"a/./b//../c/", "a/b/c"}, // .. 段直接丢弃，不做父级回退
		{"../..", ""},
		{"//", ""},
		{"  x  /  y ", "x/y"}, // 段级 trim，段内空格保留
		{"带 空格/中文目录", "带 空格/中文目录"},
	}
	for _, c := range cases {
		if got := cleanPath(c.in); got != c.want {
			t.Errorf("cleanPath(%q) = %q，应为 %q", c.in, got, c.want)
		}
	}
	if got := cleanPath(strings.Repeat("a", 200)); len(got) != 120 {
		t.Errorf("单段截断得 %d 字节，应为 120", len(got))
	}
	// 500 上限按字节计且分隔符 / 也计入：5 段各 100 字节 = 4 个分隔符 + 500 = 504
	segs := make([]string, 7)
	for i := range segs {
		segs[i] = strings.Repeat("x", 100)
	}
	got := cleanPath(strings.Join(segs, "/"))
	if len(got) != 504 || strings.Count(got, "/") != 4 {
		t.Errorf("总长截断异常: %d 字节 %d 段", len(got), strings.Count(got, "/")+1)
	}
}

func TestClientIP(t *testing.T) {
	mk := func(remote, xff string) *http.Request {
		r := httptest.NewRequest("POST", "/api/rooms", nil)
		r.RemoteAddr = remote
		if xff != "" {
			r.Header.Set("X-Forwarded-For", xff)
		}
		return r
	}
	cases := []struct {
		remote, xff, want string
	}{
		// 非回环直连：XFF 一律不信任（喂伪造段也没用）
		{"203.0.113.7:4444", "1.2.3.4, 5.6.7.8", "203.0.113.7"},
		{"weird-no-port", "1.2.3.4", "weird-no-port"},
		// 回环（caddy）：取 XFF 末段
		{"127.0.0.1:5555", "9.9.9.9, 7.7.7.7", "7.7.7.7"},
		{"[::1]:5555", "7.7.7.7", "7.7.7.7"},
		{"127.0.0.1:5555", "  7.7.7.7  ", "7.7.7.7"},
		{"127.0.0.1:5555", "", "127.0.0.1"},
		{"127.0.0.1:5555", " , ", "127.0.0.1"},
	}
	for _, c := range cases {
		if got := clientIP(mk(c.remote, c.xff)); got != c.want {
			t.Errorf("clientIP(remote=%q, xff=%q) = %q，应为 %q", c.remote, c.xff, got, c.want)
		}
	}
}

func TestValidIHAndRefs(t *testing.T) {
	if !validIH(ihHex(1)) {
		t.Error("合法 ih 应通过")
	}
	for _, s := range []string{strings.ToUpper(ihHex(1)), ihHex(1) + "a", ihHex(1)[:39], strings.Replace(ihHex(1), "0", "g", 1), ""} {
		if validIH(s) {
			t.Errorf("validIH(%q) 应为假", s)
		}
	}
	if !validRef(fileRef{IH: ihHex(1), Size: 0}) {
		t.Error("0 字节应合法")
	}
	if validRef(fileRef{IH: ihHex(1), Size: -1}) || validRef(fileRef{IH: ihHex(1), Size: 1 << 50}) {
		t.Error("负数或超上限 size 应非法")
	}
	a := []fileRef{ref(1, 1), ref(2, 2)}
	if !sameIH(a, []fileRef{{Name: "改名", Size: 99, IH: ihHex(1)}, ref(2, 2)}) {
		t.Error("sameIH 只看 ih 序列")
	}
	if sameIH(a, []fileRef{ref(2, 2), ref(1, 1)}) {
		t.Error("sameIH 应顺序敏感")
	}
	if sameIH(a, a[:1]) {
		t.Error("长度不同不应同清单")
	}
}

func TestBinStr2Hex(t *testing.T) {
	h, ok := binStr2Hex(jsRaw20(11))
	if !ok || h != ihHex(11) {
		t.Errorf("binStr2Hex 往返不符: %q vs %q", h, ihHex(11))
	}
	if _, ok := binStr2Hex(jsRaw20(11) + "\u4e2d"); ok {
		t.Error("码点 > 255 应拒绝")
	}
	if _, ok := binStr2Hex("短"); ok {
		t.Error("长度不符应拒绝")
	}
}

func TestParseRefs(t *testing.T) {
	many := make([]fileRef, maxFiles)
	for i := range many {
		many[i] = ref(byte(i+1), int64(i))
	}
	if out, ok := parseRefs(many); !ok || len(out) != maxFiles {
		t.Errorf("%d 个文件应合法", maxFiles)
	}
	if _, ok := parseRefs(append(append([]fileRef{}, many...), ref(99, 1))); ok {
		t.Error("超过 64 文件应拒绝")
	}
	if _, ok := parseRefs(nil); ok {
		t.Error("空清单应拒绝")
	}
	if _, ok := parseRefs([]fileRef{ref(1, 1), ref(1, 1)}); ok {
		t.Error("重复 ih 应拒绝")
	}
	out, ok := parseRefs([]fileRef{{Name: `目录\子 目录\最终\x01.txt`, Path: "../../安全/./x//", Size: 5, IH: ihHex(9)}})
	// 反引号串里的 \x01 是字面四字符：反斜杠先被归一成分隔符，字面段被切走，剩 x01.txt
	if !ok || out[0].Name != "x01.txt" || out[0].Path != "安全/x" {
		t.Errorf("清洗结果异常: %+v", out)
	}
	real := []byte{0x01}
	out2, ok := parseRefs([]fileRef{{Name: "目录\\脏" + string(real) + "名.txt", Path: "../..//a/./", Size: 5, IH: ihHex(9)}})
	if !ok || out2[0].Name != "脏名.txt" || out2[0].Path != "a" {
		t.Errorf("真控制字符应被清洗: %+v", out2)
	}
}

func TestPieceLenPolicy(t *testing.T) {
	cases := map[int64]int64{
		64<<20 - 1:  4 << 20,
		64 << 20:    16 << 20,
		512<<20 - 1: 16 << 20,
		512 << 20:   32 << 20,
	}
	for size, want := range cases {
		if got := pieceLenPolicy(size); got != want {
			t.Errorf("pieceLenPolicy(%d) = %d，应为 %d", size, got, want)
		}
	}
}

func TestRingLog(t *testing.T) {
	l := newRingLog(1000)
	if l.dump(0) != "" {
		t.Error("空日志应得空串")
	}
	for i := 0; i < 100; i++ {
		l.add("info", "日志条目 %03d —— 一些填充内容让每条有几十字节", i)
	}
	l.mu.Lock()
	if l.total > l.max {
		t.Errorf("total %d 超预算 %d", l.total, l.max)
	}
	sum := 0
	for _, e := range l.buf {
		sum += entrySize(e)
	}
	if sum != l.total {
		t.Errorf("字节数账目不符: %d != %d", sum, l.total)
	}
	if l.dropped == 0 {
		t.Error("100 条远超预算，应已丢弃最旧")
	}
	kept := len(l.buf)
	l.mu.Unlock()
	if n := strings.Count(l.dump(10), "\n"); n > 11 {
		t.Errorf("dump(10) 得 %d 行，应 ≤ 11（含丢弃头）", n)
	}
	if n := strings.Count(l.dump(0), "\n"); n != kept+1 {
		t.Errorf("全量导出应恰为留存 %d 条（含丢弃头 %d 行）", kept, n)
	}
}

func TestFileTorrent(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "样.bin")
	content := make([]byte, 300000)
	for i := range content {
		content[i] = byte(i * 31)
	}
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
	const ann = "wss://x/tracker"
	name := "样.bin"
	size := int64(len(content))

	ih, tor, err := fileTorrent(p, size, name, ann, "")
	if err != nil {
		t.Fatal(err)
	}
	if !validIH(ih) {
		t.Fatalf("infohash 非法: %q", ih)
	}

	// 按实现同款 bencode 布局独立重算 infohash——钉住与浏览器端 parse-torrent 的互操作格式
	pieceLen := pieceLenPolicy(size)
	var pieces []byte
	for off := int64(0); off < size; off += pieceLen {
		end := off + pieceLen
		if end > size {
			end = size
		}
		s := sha1.Sum(content[off:end])
		pieces = append(pieces, s[:]...)
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
	bPutStr(&info, pieces)
	info.WriteByte('e')
	sum := sha1.Sum(info.Bytes())
	if ih != hex.EncodeToString(sum[:]) {
		t.Error("infohash 与 bencode info 字典布局不符")
	}

	contentSum := sha1.Sum(content)
	if ih == hex.EncodeToString(contentSum[:]) {
		t.Error("infohash 不应等于文件内容哈希（防有人改成对整文件求和）")
	}

	wantHead := fmt.Sprintf("d8:announce%d:%s4:info", len(ann), ann)
	if !strings.HasPrefix(string(tor), wantHead) {
		t.Errorf("种子头部异常: %q", string(tor[:30]))
	}
	// 前端 pieceLenFromTorrent 只扫种子头 16KB 的 `12:piece lengthiNe`
	marker := fmt.Sprintf("12:piece lengthi%de", pieceLen)
	if i := strings.Index(string(tor), marker); i < 0 || i > 16384 {
		t.Errorf("piece length 键应在前 16KB 供前端解析，offset=%d", i)
	}
	if strings.Contains(string(tor), "url-list") {
		t.Error("webseed 为空时不应烙 url-list")
	}

	ih2, tor2, err := fileTorrent(p, size, name, ann, "https://w/f/")
	if err != nil || ih2 != ih {
		t.Errorf("url-list 不应影响 infohash: %q vs %q (%v)", ih2, ih, err)
	}
	if !strings.Contains(string(tor2), "url-list") {
		t.Error("webseed 非空时应含 url-list")
	}

	ih3, _, err := fileTorrent(p, size, name, ann, "")
	if err != nil || ih3 != ih {
		t.Error("同内容同参应确定性产出同 infohash")
	}

	if _, _, err := fileTorrent(p, size+1, name, ann, ""); err == nil || !strings.Contains(err.Error(), "大小变了") {
		t.Errorf("size 对账不符应报错: %v", err)
	}
	if _, _, err := fileTorrent(filepath.Join(dir, "不存在"), 1, name, ann, ""); err == nil {
		t.Error("缺文件应报错")
	}

	pe := filepath.Join(dir, "空.txt")
	if err := os.WriteFile(pe, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	ihE, _, err := fileTorrent(pe, 0, "空.txt", ann, "")
	if err != nil || !validIH(ihE) {
		t.Errorf("空文件应可做种: %v %q", err, ihE)
	}
}

// ---------- 限速与配额 ----------

func TestAllowCreateBucket(t *testing.T) {
	resetBuckets()
	ip := "11.0.0.1"
	for i := 0; i < 10; i++ {
		if !allowCreate(ip) {
			t.Fatalf("第 %d 次建房应放行", i+1)
		}
	}
	if allowCreate(ip) {
		t.Error("容量 10，第 11 次应拒绝")
	}
	if !allowCreate("11.0.0.2") {
		t.Error("相邻 IP 应互不影响")
	}
	// 拨时钟：6 分钟回 1 个令牌
	b := buckets.m[ip]
	b.last -= 360
	if !allowCreate(ip) {
		t.Error("6 分钟后应回填 1 个令牌")
	}
	if allowCreate(ip) {
		t.Error("只回填了 1 个，第 2 次仍应拒绝")
	}
	// 长时间静默：回填到满仓 10
	b.tokens = 0
	b.last -= 3600
	for i := 0; i < 10; i++ {
		if !allowCreate(ip) {
			t.Fatalf("静默 1h 后应回满，第 %d 次应放行", i+1)
		}
	}
	if allowCreate(ip) {
		t.Error("回填不应超容量 10")
	}
}

func TestAllowProbeBucket(t *testing.T) {
	resetBuckets()
	ip := "12.0.0.1"
	for i := 0; i < 3; i++ {
		if !allowProbe(ip) {
			t.Fatalf("第 %d 次探测应放行", i+1)
		}
	}
	if allowProbe(ip) {
		t.Error("容量 3，第 4 次应拒绝")
	}
	b := probeBuckets.m[ip]
	b.last -= 20
	if !allowProbe(ip) {
		t.Error("20 秒后应回 1 个")
	}
	if allowProbe(ip) {
		t.Error("只回 1 个")
	}
	b.tokens = 0
	b.last -= 60
	for i := 0; i < 3; i++ {
		if !allowProbe(ip) {
			t.Fatalf("静默 60s 后应回满 3 个，第 %d 次失败", i+1)
		}
	}
	if allowProbe(ip) {
		t.Error("探测回填不应超容量 3")
	}
}

// v6.1 回归：灌垃圾 IP 顶到 4096 触发清扫时，只清一小时冷桶，活跃桶不能被洗掉。
func TestColdBucketSweep(t *testing.T) {
	resetBuckets()
	now := time.Now().Unix()
	buckets.Lock()
	for i := 0; i < 5000; i++ {
		ip := fmt.Sprintf("10.%d.%d.%d", i/65536%256, i/256%256, i%256)
		buckets.m[ip] = &bucket{tokens: 5, last: float64(now - 7200)}
	}
	warm := map[string]*bucket{
		"77.0.0.1": {tokens: 2, last: float64(now - 1800)},
		"77.0.0.2": {tokens: 2, last: float64(now - 1800)},
	}
	for ip, b := range warm {
		buckets.m[ip] = b
	}
	buckets.Unlock()

	if !allowCreate("88.88.88.88") {
		t.Fatal("新 IP 应放行")
	}
	buckets.Lock()
	n := len(buckets.m)
	w1 := buckets.m["77.0.0.1"]
	w2 := buckets.m["77.0.0.2"]
	fresh := buckets.m["88.88.88.88"]
	buckets.Unlock()
	if n != 3 {
		t.Errorf("清扫后应剩 3 个桶（2 活跃 + 1 新），实剩 %d", n)
	}
	if w1 == nil || w2 == nil || w1.tokens != 2 || w2.tokens != 2 {
		t.Error("半小时内活跃的桶不应被冷清扫洗掉")
	}
	if fresh == nil || fresh.tokens != 9 {
		t.Errorf("新 IP 应得满仓 10 减本次 = 9，实际 %v", fresh)
	}
}

func TestHubCreateQuotaAndReuse(t *testing.T) {
	resetHub()
	o1 := "10.0.0.1"
	fA := []fileRef{ref(1, 10), ref(2, 20)}
	fArenamed := []fileRef{{Name: "改名不改内容", Size: 999, IH: ihHex(1)}, {Name: "x", IH: ihHex(2), Size: 5}}
	fB := []fileRef{ref(3, 30)}
	fArev := []fileRef{ref(2, 20), ref(1, 10)}

	r1, err := hubv.create(fA, "出示串对新房无效", o1)
	if err != nil {
		t.Fatal(err)
	}
	if r1.token == "出示串对新房无效" || len(r1.token) != 12 {
		t.Errorf("新房 token 应由服务端铸造，忽略出示串: %q", r1.token)
	}
	// 出示真凭证 + 同 ih 集合（名/大小无关）→ 续用原房
	r1b, err := hubv.create(fArenamed, r1.token, o1)
	if err != nil || r1b != r1 {
		t.Errorf("同 ih 集合 + 真 token 应续用原房: %v %v", r1b == r1, err)
	}
	r2, err := hubv.create(fB, r1.token, o1)
	if err != nil || r2 == r1 {
		t.Fatalf("凭证真但清单不同也应开新房: %v", err)
	}
	if _, err := hubv.create(fA, "wrong", o1); !errors.Is(err, errBusy) {
		t.Errorf("同清单错 token 不得续用（v6.3 偷房窗口），应因 per-IP 配额拒绝: %v", err)
	}
	if _, err := hubv.create(fArev, r1.token, o1); !errors.Is(err, errBusy) {
		t.Errorf("顺序不同非同清单: %v", err)
	}
	r5, err := hubv.create(fArev, r1.token, "10.0.0.2")
	if err != nil || r5.code == r1.code {
		t.Fatalf("换 owner 应开成新房: %v", err)
	}
	// 凭证 = 清单知识 + 真 token，跨 IP 续用原房是设计语义
	r6, err := hubv.create(fA, r1.token, "10.0.0.2")
	if err != nil || r6 != r1 {
		t.Errorf("同清单同 token 跨 IP 应续用原房: %v %v", r6 == r1, err)
	}

	// 全局 8 房上限：现有 3 房，再填 5 个
	for i := 3; i <= 7; i++ {
		if _, err := hubv.create(fB, "x", fmt.Sprintf("10.0.0.%d", i)); err != nil {
			t.Fatalf("填房 %d 失败: %v", i, err)
		}
	}
	if _, err := hubv.create(fB, "x", "10.0.0.9"); !errors.Is(err, errBusy) {
		t.Errorf("全局 8 房应拒绝第 9 房: %v", err)
	}
	hubv.mu.Lock()
	delete(hubv.rooms, r2.code)
	hubv.mu.Unlock()
	if r, err := hubv.create(fB, "x", "10.0.0.9"); err != nil || r == nil {
		t.Errorf("拆一房后应能再建: %v", err)
	}

	hubv.mu.Lock()
	defer hubv.mu.Unlock()
	codes := map[string]bool{}
	for code, r := range hubv.rooms {
		if r.code != code || len(code) != codeLen {
			t.Errorf("房间键与 code 不符: %s", code)
		}
		codes[code] = true
	}
	if len(codes) != len(hubv.rooms) {
		t.Error("存在重码房间")
	}
}

// v6.3 回归：复用分支 token 恒时比较——拿六位码 + 清单（peek 即得）偷不走别人的房。
func TestCreateAPITokenReuseSecurity(t *testing.T) {
	resetAll()
	srv := apiServer(t)
	ip := "8.8.4.4"
	fA := []fileRef{ref(1, 10), ref(2, 20)}

	c1, t1 := createRoom(t, srv, ip, fA...)
	// 刷新恢复：出示服务端铸造的真 token → 原房续用，码与 token 原样
	st, m := presentRoom(t, srv, ip, t1, fA...)
	if st != http.StatusOK || m["code"] != c1 || m["token"] != t1 {
		t.Fatalf("刷新恢复应原房续用: %d %v", st, m)
	}

	// 同清单错 token：得新房，原房原 token 不受扰
	st, m = postJSON(t, srv, ip, "POST", "/api/rooms", `{"token":"偷房","files":`+refsJSON(fA...)+`}`)
	if st != http.StatusOK {
		t.Fatalf("错 token 应开新房而非拒绝: %d", st)
	}
	c2, _ := m["code"].(string), m["token"].(string)
	if c2 == c1 {
		t.Fatal("错 token 复用了原房——偷房窗口回来了")
	}
	if st, m := peekRoom(t, srv, ip, c1); st != 200 || len(roomFiles(m)) != 2 {
		t.Errorf("原房应完好: %d %v", st, m)
	}

	// 顺序不同非同清单：另一 IP 拿同 token 来，也开新房
	rev := []fileRef{ref(2, 20), ref(1, 10)}
	st3, m3 := postJSON(t, srv, "8.8.8.8", "POST", "/api/rooms", fmt.Sprintf(`{"token":%q,"files":%s}`, t1, refsJSON(rev...)))
	if st3 != http.StatusOK {
		t.Fatalf("顺序不同应开新房: %d %v", st3, m3)
	}
	if m3["code"] == c1 {
		t.Fatal("顺序颠倒竟复用了原房——sameIH 顺序敏感被破坏")
	}

	// 清洗随响应回显：接收端拿到的就是清洗后的形态（JSON 里控制字节必须走 \u0001 转义）
	st4, m4 := postJSON(t, srv, "8.8.4.8", "POST", "/api/rooms",
		`{"files":[{"name":"目录\\脏\u0001名.txt","path":"../..//a/./","size":5,"ih":"`+ihHex(30)+`"}]}`)
	if st4 != 200 {
		t.Fatalf("建房失败: %d %v", st4, m4)
	}
	fs := roomFiles(m4)
	f0 := fs[0].(map[string]any)
	if f0["name"] != "脏名.txt" || f0["path"] != "a" {
		t.Errorf("HTTP 层清洗回显异常: %v", f0)
	}
}

func TestAPIAddRemove(t *testing.T) {
	resetAll()
	srv := apiServer(t)
	ip := "9.1.0.1"
	f1, f2, f3, f4 := ref(1, 10), ref(2, 20), ref(3, 30), ref(4, 40)
	code, tok := createRoom(t, srv, ip, f1, f2)

	// 增投：追加、去重、顺序
	st, m := addFiles(t, srv, ip, code, tok, f3)
	if st != 200 || len(roomFiles(m)) != 3 {
		t.Fatalf("增投异常: %d %v", st, m)
	}
	if last := roomFiles(m)[2].(map[string]any)["ih"]; last != f3.IH {
		t.Errorf("增投应追加在尾部: %v", last)
	}
	st, m = addFiles(t, srv, ip, code, tok, f3)
	if st != 200 || len(roomFiles(m)) != 3 {
		t.Errorf("重复 ih 应忽略: %d %v", st, m)
	}
	st, m = addFiles(t, srv, ip, code, tok, f3, f4)
	if st != 200 || len(roomFiles(m)) != 4 {
		t.Errorf("混合批次应只收新 ih: %d %v", st, m)
	}

	// 无效载荷（在 token 判定之前就 400）
	st, _ = postJSON(t, srv, ip, "POST", "/api/rooms/"+code, `{"token":"x","files":[{"name":"a","size":-1,"ih":"`+ihHex(9)+`"}]}`)
	if st != 400 {
		t.Errorf("负 size 应 400: %d", st)
	}
	st, _ = postJSON(t, srv, ip, "POST", "/api/rooms/"+code, `{"token":"x","files":[{"name":"a","size":1,"ih":"NOTHEX"}]}`)
	if st != 400 {
		t.Errorf("非法 ih 应 400: %d", st)
	}

	// 错 token：403 且烧探测额度，3 次后 429（独立 IP 免被上面流量干扰）
	bad := "9.1.0.2"
	for i := 0; i < 3; i++ {
		st, _ = postJSON(t, srv, bad, "POST", "/api/rooms/"+code, fmt.Sprintf(`{"token":"bad","files":%s}`, refsJSON(ref(9, 9))))
		if st != 403 {
			t.Fatalf("错 token 第 %d 次应 403: %d", i+1, st)
		}
	}
	st, _ = postJSON(t, srv, bad, "POST", "/api/rooms/"+code, fmt.Sprintf(`{"token":"bad","files":%s}`, refsJSON(ref(9, 9))))
	if st != 429 {
		t.Errorf("探测额度烧完应 429: %d", st)
	}

	// 移除：非法 ih 400、幂等、清空即拆房
	st, _ = removeFile(t, srv, ip, code, tok, "zz")
	if st != 400 {
		t.Errorf("remove 非法 ih 应 400: %d", st)
	}
	st, m = removeFile(t, srv, ip, code, tok, f2.IH)
	if st != 200 || len(roomFiles(m)) != 3 {
		t.Fatalf("移除异常: %d %v", st, m)
	}
	st, m = removeFile(t, srv, ip, code, tok, f2.IH)
	if st != 200 || len(roomFiles(m)) != 3 {
		t.Errorf("幽灵移除应幂等 200: %d %v", st, m)
	}
	for _, f := range []fileRef{f1, f3, f4} {
		st, m = removeFile(t, srv, ip, code, tok, f.IH)
		if st != 200 {
			t.Fatalf("清空过程异常: %d %v", st, m)
		}
	}
	// 拆房响应的 files 必须是非 nil 空表：JSON null 经 roomFiles 也变 nil、
	// 长度对账抓不住，这里直接判型
	if _, ok := m["files"].([]any); !ok {
		t.Errorf("清空拆房应回 files:[] 而非 null: %v", m["files"])
	}
	if st, _ := peekRoom(t, srv, ip, code); st != 404 {
		t.Errorf("清空后应拆房 404: %d", st)
	}
}

func TestAPIAddCap64(t *testing.T) {
	resetAll()
	srv := apiServer(t)
	seed := make([]fileRef, 60)
	for i := range seed {
		seed[i] = ref(byte(i+1), 1)
	}
	code, tok := createRoom(t, srv, "9.2.0.1", seed...)
	extra := []fileRef{ref(61, 1), ref(62, 1), ref(63, 1), ref(64, 1), ref(65, 1)}
	st, m := addFiles(t, srv, "9.2.0.1", code, tok, extra...)
	if st != 200 {
		t.Fatalf("房满增投应 200（显式截断）: %d %v", st, m)
	}
	if n := len(roomFiles(m)); n != maxFiles {
		t.Fatalf("应封顶 %d，实际 %d", maxFiles, n)
	}
	st, m = addFiles(t, srv, "9.2.0.1", code, tok, ref(1, 1), ref(65, 1))
	if st != 200 || len(roomFiles(m)) != maxFiles {
		t.Errorf("满房后增投应全忽略: %d %d", st, len(roomFiles(m)))
	}
}

func TestPeekAndProbeLimit(t *testing.T) {
	resetAll()
	srv := apiServer(t)

	// 探测限速：有效长度不存在码 3 次后 429；长度不符 400 前置（不烧额度）
	ip := "7.7.0.1"
	for i := 0; i < 3; i++ {
		if st, _ := peekRoom(t, srv, ip, "ABC234"); st != 404 {
			t.Fatalf("空码第 %d 次应 404: %d", i+1, st)
		}
	}
	if st, _ := peekRoom(t, srv, ip, "ABC235"); st != 429 {
		t.Errorf("探测额度用尽应 429: %d", st)
	}
	if st, _ := peekRoom(t, srv, ip, "ABC23"); st != 400 {
		t.Errorf("码长不符应 400（限速之前）: %d", st)
	}

	// 大小写归一 + 清单快照
	ip2 := "7.7.0.2"
	code, _ := createRoom(t, srv, ip2, ref(1, 10), ref(2, 20))
	st, m := peekRoom(t, srv, ip2, strings.ToLower(code))
	if st != 200 || len(roomFiles(m)) != 2 {
		t.Fatalf("小写码应对上房: %d %v", st, m)
	}
	if fs := roomFiles(m); fs[0].(map[string]any)["ih"] != ihHex(1) {
		t.Errorf("清单快照顺序异常: %v", fs)
	}
}

// ---------- tracker 单元 ----------

func TestEnsureSwarmCap(t *testing.T) {
	resetTrk()
	trk.Lock()
	for i := 0; i < trkMaxSwarms; i++ {
		trk.swarms[fmt.Sprintf("%040x", i)] = &trkSwarm{peers: map[string]*trkPeer{}}
	}
	trk.Unlock()
	if _, err := ensureSwarm(fmt.Sprintf("%040x", trkMaxSwarms)); !errors.Is(err, errBusy) {
		t.Errorf("512 池满应 errBusy: %v", err)
	}
	old := fmt.Sprintf("%040x", 5)
	trk.Lock()
	s1, err := ensureSwarm(old)
	trk.Unlock()
	if err != nil || s1 == nil {
		t.Errorf("既有池不应受限: %v", err)
	}
	trk.Lock()
	removePeer(old, "幽灵 pid") // 空池摘除最后一人后应整体回收
	trk.Unlock()
	resetTrk()
	trk.Lock()
	s, err := ensureSwarm(old)
	if err != nil {
		trk.Unlock()
		t.Fatal(err)
	}
	s.peers["p"] = &trkPeer{}
	removePeer(old, "p")
	_, ok := getSwarm(old)
	removePeer(old, "p") // 再摘一次不得 panic
	trk.Unlock()
	if ok {
		t.Error("空池应被 removePeer 回收")
	}
}

func TestAllowAnnounce(t *testing.T) {
	now := float64(time.Now().Unix())
	c := &trkConn{tokens: 0, last: now}
	if c.allowAnnounce() {
		t.Error("无时间流逝不应回填")
	}
	c.last = now - 2
	for i := 0; i < 4; i++ {
		if !c.allowAnnounce() {
			t.Fatalf("2s×2/s 应回 4 个，第 %d 个失败", i+1)
		}
	}
	if c.allowAnnounce() {
		t.Error("只回 4 个")
	}
	c2 := &trkConn{tokens: 0, last: 0}
	for i := 0; i < trkAnnCap; i++ {
		if !c2.allowAnnounce() {
			t.Fatalf("突发容量应 %d，第 %d 个失败", trkAnnCap, i+1)
		}
	}
	if c2.allowAnnounce() {
		t.Error("突发容量不应超")
	}
}

func TestPickOthers(t *testing.T) {
	s := &trkSwarm{peers: map[string]*trkPeer{}}
	for _, id := range []string{"a", "b", "c", "d"} {
		s.peers[id] = &trkPeer{}
	}
	got := pickOthers(s, "a", 10)
	if len(got) != 3 {
		t.Fatalf("应取 3 个非自身，得 %d", len(got))
	}
	for _, p := range got {
		if p == nil || p == s.peers["a"] {
			t.Error("取到了 nil 或自身")
		}
	}
	if len(pickOthers(s, "a", 2)) != 2 {
		t.Error("n 应封顶")
	}
	if len(pickOthers(s, "zzz", 0)) != 0 {
		t.Error("n=0 应得空")
	}
	if len(pickOthers(s, "zzz", 1)) != 1 {
		t.Error("陌生 self 也应取得到")
	}
}

// ---------- ws tracker 集成 ----------

func trackerServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /tracker", handleTrackerWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// seedSwarm：直接向 trk 注入 n 个假 peer（不占连接，配合上限类测试）。
func seedSwarm(ihhex string, n int) {
	trk.Lock()
	defer trk.Unlock()
	s := &trkSwarm{peers: map[string]*trkPeer{}}
	for i := 0; i < n; i++ {
		s.peers[fmt.Sprintf("%040x", i)] = &trkPeer{complete: true, lastSeen: time.Now(), c: &trkConn{}}
	}
	trk.swarms[ihhex] = s
}

func wsDial(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/tracker", &websocket.DialOptions{})
	if err != nil {
		t.Fatalf("dial tracker: %v", err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func wsSend(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal ws 消息: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}

func wsRecv(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	m := map[string]any{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("ws 消息非 JSON: %v", err)
	}
	return m
}

// ann：构造 announce 消息（info_hash/peer_id 为 JS 式原始串）。
func ann(ihRaw, pid string, extra map[string]any) map[string]any {
	m := map[string]any{"action": "announce", "info_hash": ihRaw, "peer_id": pid}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestTrackerWSAnnounce(t *testing.T) {
	resetTrk()
	srv := trackerServer(t)
	c1 := wsDial(t, srv)
	ih, p1 := jsRaw20(11), jsRaw20(21)
	wsSend(t, c1, ann(ih, p1, map[string]any{"left": 0}))
	m := wsRecv(t, c1)
	if num(m, "interval") != trkInterval || num(m, "complete") != 1 || num(m, "incomplete") != 0 {
		t.Errorf("种子首报应答异常: %v", m)
	}
	if m["info_hash"] != ih {
		t.Errorf("info_hash 回显异常: %v", m["info_hash"])
	}
}

func TestTrackerWSOfferAnswer(t *testing.T) {
	resetTrk()
	srv := trackerServer(t)
	c1, c2 := wsDial(t, srv), wsDial(t, srv)
	ih, p1, p2 := jsRaw20(11), jsRaw20(21), jsRaw20(22)

	wsSend(t, c1, ann(ih, p1, map[string]any{"left": 0}))
	wsRecv(t, c1) // 种子自己的 interval 应答
	wsSend(t, c2, ann(ih, p2, nil))
	m := wsRecv(t, c2)
	if num(m, "complete") != 1 || num(m, "incomplete") != 1 {
		t.Fatalf("双人池计数异常: %v", m)
	}

	// offer 定向转发给池内其他 peer
	wsSend(t, c1, ann(ih, p1, map[string]any{
		"offers": []map[string]any{{"offer": map[string]any{"t": "o1"}, "offer_id": "of1"}}}))
	mo := wsRecv(t, c2)
	off, _ := mo["offer"].(map[string]any)
	if off == nil || off["t"] != "o1" || mo["offer_id"] != "of1" || mo["peer_id"] != p1 || mo["info_hash"] != ih {
		t.Fatalf("offer 转发异常: %v", mo)
	}
	mi := wsRecv(t, c1) // 发起者自己的 interval 应答
	if num(mi, "interval") != trkInterval {
		t.Fatalf("offers 批次应答异常: %v", mi)
	}

	// answer 按 to_peer_id 定向转发，发送方不得到回执
	wsSend(t, c2, ann(ih, p2, map[string]any{
		"answer": map[string]any{"t": "a1"}, "to_peer_id": p1, "offer_id": "of1"}))
	mr := wsRecv(t, c1)
	ans, _ := mr["answer"].(map[string]any)
	if ans == nil || ans["t"] != "a1" || mr["peer_id"] != p2 || mr["offer_id"] != "of1" {
		t.Fatalf("answer 转发异常: %v", mr)
	}
	rctx, rc := context.WithTimeout(context.Background(), time.Second)
	_, _, err := c2.Read(rctx)
	rc()
	if err == nil {
		t.Error("answer 的发送方不应收到任何回执")
	}
}

func TestTrackerWSStoppedReap(t *testing.T) {
	resetTrk()
	srv := trackerServer(t)
	c1, c2 := wsDial(t, srv), wsDial(t, srv)
	ih, p1, p2 := jsRaw20(11), jsRaw20(21), jsRaw20(22)

	wsSend(t, c1, ann(ih, p1, map[string]any{"left": 0}))
	wsRecv(t, c1)
	wsSend(t, c2, ann(ih, p2, nil))
	wsRecv(t, c2)

	wsSend(t, c2, ann(ih, p2, map[string]any{"event": "stopped"}))
	m := wsRecv(t, c2)
	if num(m, "complete") != 1 || num(m, "incomplete") != 0 {
		t.Fatalf("stopped 后计数异常: %v", m)
	}
	// scrape 核对
	wsSend(t, c1, map[string]any{"action": "scrape", "info_hash": ih})
	ms := wsRecv(t, c1)
	files, _ := ms["files"].(map[string]any)
	f, _ := files[ih].(map[string]any)
	if f == nil || num(f, "complete") != 1 || num(f, "incomplete") != 0 {
		t.Fatalf("scrape 结果异常: %v", ms)
	}
	// 最后一人退场：空池回收
	wsSend(t, c1, ann(ih, p1, map[string]any{"event": "stopped"}))
	wsRecv(t, c1)
	trk.Lock()
	n := len(trk.swarms)
	trk.Unlock()
	if n != 0 {
		t.Errorf("全员退场后应无池，剩 %d", n)
	}
}

func TestTrackerWSScrapeArray(t *testing.T) {
	resetTrk()
	srv := trackerServer(t)
	c := wsDial(t, srv)
	ih1, ih2, p := jsRaw20(11), jsRaw20(12), jsRaw20(21)
	wsSend(t, c, ann(ih1, p, map[string]any{"left": 0}))
	wsRecv(t, c)
	wsSend(t, c, ann(ih2, p, nil))
	wsRecv(t, c)

	wsSend(t, c, map[string]any{"action": "scrape", "info_hash": []string{ih1, ih2, "含\u4e2d文超码点"}})
	m := wsRecv(t, c)
	files, _ := m["files"].(map[string]any)
	if len(files) != 2 {
		t.Fatalf("scrape 应只含 2 个合法 ih: %v", m)
	}
	if f := files[ih2].(map[string]any); num(f, "incomplete") != 1 {
		t.Errorf("ih2 计数异常: %v", f)
	}
}

func TestTrackerWSGarbageIgnored(t *testing.T) {
	resetTrk()
	srv := trackerServer(t)
	c := wsDial(t, srv)
	p := jsRaw20(21)

	// info_hash 带超 255 码点 → 静默忽略，连接保活
	wsSend(t, c, ann("池\u4e2d文必\u4e2d超码点", p, nil))
	// 坏 JSON → 静默忽略
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := c.Write(ctx, websocket.MessageText, []byte("{oops")); err != nil {
		t.Fatalf("写坏帧失败: %v", err)
	}
	cancel()
	// 两者都不应得到应答：下一条应答必须对应这条合法 announce
	wsSend(t, c, ann(jsRaw20(12), p, nil))
	m := wsRecv(t, c)
	if m["info_hash"] != jsRaw20(12) {
		t.Errorf("畸形消息未被静默忽略: %v", m)
	}
}

func TestTrackerWSOversizeFrame(t *testing.T) {
	resetTrk()
	srv := trackerServer(t)
	c := wsDial(t, srv)
	big := strings.Repeat("x", trkMaxMsg+1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = c.Write(ctx, websocket.MessageText, []byte(`{"action":"announce","peer_id":"`+big+`"}`))
	cancel()
	rctx, rc := context.WithTimeout(context.Background(), 5*time.Second)
	_, _, err := c.Read(rctx)
	rc()
	if err == nil {
		t.Error("超限帧后服务器应断开连接")
	}
}

func TestTrackerWSPeerSwap(t *testing.T) {
	resetTrk()
	srv := trackerServer(t)
	c := wsDial(t, srv)
	ih, p1, p2 := jsRaw20(11), jsRaw20(21), jsRaw20(22)
	wsSend(t, c, ann(ih, p1, map[string]any{"left": 0}))
	wsRecv(t, c)
	wsSend(t, c, ann(ih, p2, map[string]any{"left": 0}))
	if m := wsRecv(t, c); num(m, "complete") != 1 || num(m, "incomplete") != 0 {
		t.Fatalf("换身后计数应仍为 1 人: %v", m)
	}
	p1h, p2h := ihHex(21), ihHex(22)
	trk.Lock()
	s := trk.swarms[ihHex(11)]
	_, hasOld := s.peers[p1h]
	_, hasNew := s.peers[p2h]
	n := len(s.peers)
	trk.Unlock()
	if n != 1 || hasOld || !hasNew {
		t.Errorf("旧身应被清除（v6.3）: n=%d old=%v new=%v", n, hasOld, hasNew)
	}
}

func TestTrackerWSPeerCap(t *testing.T) {
	resetTrk()
	seedSwarm(ihHex(11), trkMaxPeers-1) // 255 假种子
	srv := trackerServer(t)
	c1, c2 := wsDial(t, srv), wsDial(t, srv)
	ih := jsRaw20(11)

	wsSend(t, c1, ann(ih, jsRaw20(21), map[string]any{"left": 0}))
	m := wsRecv(t, c1)
	if num(m, "complete") != trkMaxPeers {
		t.Fatalf("第 256 人应入池: %v", m)
	}
	wsSend(t, c2, ann(ih, jsRaw20(22), nil))
	mf := wsRecv(t, c2)
	if mf["failure reason"] != "正忙" {
		t.Fatalf("第 257 人应得「正忙」: %v", mf)
	}
}

func TestTrackerWSSwarmCap(t *testing.T) {
	resetTrk()
	for i := 0; i < trkMaxSwarms; i++ {
		seedSwarm(fmt.Sprintf("%040x", i), 1)
	}
	srv := trackerServer(t)
	c := wsDial(t, srv)

	wsSend(t, c, ann(jsRaw20(3), jsRaw20(23), nil))
	if m := wsRecv(t, c); m["failure reason"] != "正忙" {
		t.Fatalf("512 池满应得「正忙」: %v", m)
	}
	trk.Lock()
	removePeer(fmt.Sprintf("%040x", 7), fmt.Sprintf("%040x", 0)) // 腾一格
	trk.Unlock()
	wsSend(t, c, ann(jsRaw20(3), jsRaw20(23), nil))
	if m := wsRecv(t, c); num(m, "interval") != trkInterval {
		t.Fatalf("腾位后应入池: %v", m)
	}
}

// ---------- 并发混沌（-race 下跑） ----------

// TestChaosHubTrackerRace：六路并发锤 hub 与 trk 的全部锁序——
// 房间生命周期增投/划走/清空拆房/重建、直改 map 的 janitor 式摘除、
// tracker 建池退池、错 token 打探测限速。跑完核对端态不变量。
func TestChaosHubTrackerRace(t *testing.T) {
	if testing.Short() {
		t.Skip("短测跳过混沌")
	}
	resetAll()
	srv := apiServer(t)

	base := ref(200, 1)
	rm, err := hubv.create([]fileRef{base}, "chaos-tok", "10.66.0.1")
	if err != nil {
		t.Fatal(err)
	}
	var curCode atomic.Value
	curCode.Store(rm.code)

	deadline := time.Now().Add(2 * time.Second)
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				fn()
			}
		}()
	}

	// G1 房间生命周期：增投→划走→清空拆房→重建（apiAdd/apiRemove 全锁序 + hub.create 复用/新建双路）
	added := ref(201, 2)
	run(func() {
		code, _ := curCode.Load().(string)
		doJSON(srv, "10.66.0.1", "POST", "/api/rooms/"+code,
			fmt.Sprintf(`{"token":"chaos-tok","files":%s}`, refsJSON(added)))
		doJSON(srv, "10.66.0.1", "POST", "/api/rooms/"+code+"/remove",
			fmt.Sprintf(`{"token":"chaos-tok","ih":%q}`, added.IH))
		doJSON(srv, "10.66.0.1", "POST", "/api/rooms/"+code+"/remove",
			fmt.Sprintf(`{"token":"chaos-tok","ih":%q}`, base.IH))
		if nrm, err := hubv.create([]fileRef{base}, "chaos-tok", "10.66.0.1"); err == nil {
			curCode.Store(nrm.code)
		} else {
			// 撞配额：janitor 式摘掉自己多占的房，保当前码继续
			hubv.mu.Lock()
			for c, r := range hubv.rooms {
				if r.owner == "10.66.0.1" && r.code != code {
					delete(hubv.rooms, c)
				}
			}
			hubv.mu.Unlock()
		}
	})

	// G2 持续增投（可能撞上换房期 404，均为合法结局）
	run(func() {
		for i := 0; ; i++ {
			code, _ := curCode.Load().(string)
			doJSON(srv, "10.66.0.2", "POST", "/api/rooms/"+code,
				fmt.Sprintf(`{"token":"chaos-tok","files":%s}`, refsJSON(ref(byte(100+i%50), int64(i)))))
			if time.Now().After(deadline) {
				return
			}
		}
	})

	// G3 对码 peek：打中真房或落空，随它去
	run(func() {
		for i := 0; ; i++ {
			code, _ := curCode.Load().(string)
			doJSON(srv, "10.66.0.3", "GET", "/api/code?c="+code, "")
			doJSON(srv, "10.66.0.3", "GET", "/api/code?c="+fmt.Sprintf("CH%04d", i%10000), "")
			if time.Now().After(deadline) {
				return
			}
		}
	})

	// G4 直改 map 的 janitor 式摘除 + 持续建房（hub.mu 独占路径与 room.mu 并发）
	run(func() {
		for i := 0; ; i++ {
			hubv.create([]fileRef{ref(byte(100+i%50), int64(i))}, "churn-tok", "10.66.9.9")
			if i%3 == 0 {
				hubv.mu.Lock()
				for c, r := range hubv.rooms {
					if r.owner == "10.66.9.9" {
						delete(hubv.rooms, c)
					}
				}
				hubv.mu.Unlock()
			}
			if time.Now().After(deadline) {
				return
			}
		}
	})

	// G5 tracker 建池/入池/退池/随机选 peer（trk 锁内全路径）
	run(func() {
		for i := 0; ; i++ {
			ih := ihHex(byte(i % 64))
			pid := fmt.Sprintf("%040x", i)
			trk.Lock()
			if s, err := ensureSwarm(ih); err == nil {
				s.peers[pid] = &trkPeer{lastSeen: time.Now()}
				pickOthers(s, pid, 4)
				removePeer(ih, fmt.Sprintf("%040x", i%7))
			}
			trk.Unlock()
			if time.Now().After(deadline) {
				return
			}
		}
	})

	// G6 错 token 打 add/remove：403/429 探测限速路径
	run(func() {
		for {
			code, _ := curCode.Load().(string)
			doJSON(srv, "10.66.7.7", "POST", "/api/rooms/"+code+"/remove",
				fmt.Sprintf(`{"token":"bad","ih":%q}`, base.IH))
			doJSON(srv, "10.66.7.7", "POST", "/api/rooms/"+code,
				fmt.Sprintf(`{"token":"bad","files":%s}`, refsJSON(base)))
			if time.Now().After(deadline) {
				return
			}
		}
	})

	wg.Wait()

	// 端态不变量
	hubv.mu.Lock()
	if len(hubv.rooms) > maxRooms {
		t.Errorf("房间数 %d 超全局上限", len(hubv.rooms))
	}
	for code, r := range hubv.rooms {
		if r.code != code || len(code) != codeLen {
			t.Errorf("房间键/code 不符: %s", code)
		}
		r.mu.Lock()
		if len(r.files) > maxFiles {
			t.Errorf("房 %s 文件数 %d 超上限", code, len(r.files))
		}
		seen := map[string]bool{}
		for _, f := range r.files {
			if !validRef(f) {
				t.Errorf("房 %s 存在非法 fileRef: %+v", code, f)
			}
			if seen[f.IH] {
				t.Errorf("房 %s 出现重复 ih %s", code, f.IH)
			}
			seen[f.IH] = true
		}
		r.mu.Unlock()
	}
	hubv.mu.Unlock()

	trk.Lock()
	if len(trk.swarms) > trkMaxSwarms {
		t.Errorf("池数 %d 超上限", len(trk.swarms))
	}
	for ih, s := range trk.swarms {
		if len(s.peers) == 0 {
			t.Errorf("空池 %s 滞留", ih)
		}
	}
	trk.Unlock()
}

// ---------- HTTP 面滥用 ----------

func TestBodyLimits(t *testing.T) {
	resetAll()
	srv := apiServer(t)

	// create 体上限 64KiB：超限解码必败 → 400
	big := `{"files":[` + strings.Repeat(`{"name":"a","size":1,"ih":"`+ihHex(1)+`"},`, 1500) + `]}`
	if len(big) <= 64<<10 {
		t.Fatalf("夹具应超 64KiB，实际 %d", len(big))
	}
	if st, _ := postJSON(t, srv, "5.0.0.1", "POST", "/api/rooms", big); st != 400 {
		t.Errorf("超限建房体应 400: %d", st)
	}
	// 合法大清单（64 文件 × 长名 ≈ 9KiB）应放行：上限卡的是体不是文件数
	wide := make([]fileRef, maxFiles)
	for i := range wide {
		wide[i] = fileRef{Name: strings.Repeat("名", 60) + fmt.Sprintf("%d", i), Size: 1, IH: ihHex(byte(i + 1))}
	}
	if st, _ := postJSON(t, srv, "5.0.0.1", "POST", "/api/rooms", `{"files":`+refsJSON(wide...)+`}`); st != 200 {
		t.Errorf("合法大清单应 200: %d", st)
	}

	// remove 体上限 4KiB
	code, tok := createRoom(t, srv, "5.0.0.2", ref(1, 1))
	padded := fmt.Sprintf(`{"token":%q,"ih":"%s","pad":"%s"}`, tok, ihHex(2), strings.Repeat("x", 5000))
	if st, _ := postJSON(t, srv, "5.0.0.2", "POST", "/api/rooms/"+code+"/remove", padded); st != 400 {
		t.Errorf("超限 remove 体应 400: %d", st)
	}
}

func TestWebseedRangeAndTraversal(t *testing.T) {
	dir := t.TempDir()
	content := []byte("0123456789")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	bin := make([]byte, 300000)
	for i := range bin {
		bin[i] = byte(i * 7)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "b.bin"), bin, 0o644); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /f/{path...}", webseedHandler(dir))
	srv := httptest.NewServer(mux)

	get := func(path, rangeHeader string) (int, []byte) {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}

	if st, b := get("/f/a.txt", ""); st != 200 || !bytes.Equal(b, content) {
		t.Errorf("直出异常: %d %q", st, b)
	}
	if st, b := get("/f/a.txt", "bytes=0-0"); st != 206 || string(b) != "0" {
		t.Errorf("bytes=0-0 应 206「0」: %d %q", st, b)
	}
	if st, b := get("/f/a.txt", "bytes=-3"); st != 206 || string(b) != "789" {
		t.Errorf("suffix range 应 206「789」: %d %q", st, b)
	}
	if st, b := get("/f/a.txt", "bytes=2-"); st != 206 || string(b) != "23456789" {
		t.Errorf("开末端应 206「23456789」: %d %q", st, b)
	}
	if st, b := get("/f/a.txt", "bytes=0-999999999"); st != 206 || !bytes.Equal(b, content) {
		t.Errorf("超长 end 应截到文件尾: %d %d 字节", st, len(b))
	}
	if st, _ := get("/f/a.txt", "bytes=5-2"); st != 416 {
		t.Errorf("start>end 应 416: %d", st)
	}
	if st, _ := get("/f/a.txt", "bytes=10-"); st != 416 {
		t.Errorf("start=文件大小应 416: %d", st)
	}

	// 穿越：明文 .. 被 mux 规整成 30x/404，%2e%2e 由 io/fs 的 ValidPath 兜底（不允许 ..）→ 404
	if st, _ := get("/f/../../etc/passwd", ""); st == 200 {
		t.Error("明文 .. 穿越不得 200")
	}
	if st, b := get("/f/%2e%2e/%2e%2e/etc/passwd", ""); st != 404 || bytes.Contains(b, []byte("root:")) {
		t.Errorf("编码 .. 穿越应 404（io/fs ValidPath 拒 ..）: %d %q", st, b)
	}
	// 大文件的 Range 取中段也要对
	if st, b := get("/f/sub/b.bin", "bytes=100000-100003"); st != 206 || !bytes.Equal(b, bin[100000:100004]) {
		t.Errorf("中段 range 异常: %d", st)
	}
}

func TestZipRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub", "deep"), 0o755); err != nil {
		t.Fatal(err)
	}
	fa := []byte("AAA")
	fb := make([]byte, 200000)
	for i := range fb {
		fb[i] = byte(i)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "a.txt"), fa, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "deep", "b.bin"), fb, 0o644); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /z/{path...}", zipHandler(dir))
	srv := httptest.NewServer(mux)

	resp, err := srv.Client().Get(srv.URL + "/z/sub")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("zip 目录应 200: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/zip" {
		t.Errorf("Content-Type 应 application/zip: %q", ct)
	}
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("响应不是合法 zip: %v", err)
	}
	if len(zr.File) != 2 {
		t.Fatalf("应恰好 2 个文件条目: %d", len(zr.File))
	}
	want := map[string][]byte{"a.txt": fa, "deep/b.bin": fb}
	for _, f := range zr.File {
		wantBytes, ok := want[f.Name]
		if !ok {
			t.Errorf("意外条目 %q（应去掉 sub/ 前缀）", f.Name)
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, wantBytes) {
			t.Errorf("条目 %q 内容不符: %d 字节", f.Name, len(got))
		}
		if f.Method != zip.Store {
			t.Errorf("条目 %q 应 Store 存储: %v", f.Name, f.Method)
		}
		delete(want, f.Name)
	}
	if len(want) != 0 {
		t.Errorf("缺条目: %v", want)
	}

	// 非目录与不存在路径 → 404
	for _, p := range []string{"/z/sub/a.txt", "/z/不存在", "/z/"} {
		resp, err := srv.Client().Get(srv.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Errorf("GET %s 应 404: %d", p, resp.StatusCode)
		}
	}
}

// 同 IP 并发 20 次建房：桶互斥串行 → 恰 10 个过限速，其中恰 2 个成房（per-IP 上限）、
// 8 个 503 配额满、10 个 429 限速——分布完全确定，是配额逻辑的端到端竞态钉子。
func TestConcurrentCreateBurst(t *testing.T) {
	resetAll()
	srv := apiServer(t)
	body := `{"files":` + refsJSON(ref(1, 1)) + `}`
	var mu sync.Mutex
	var st200, st429, st503, other int
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, _, err := doJSON(srv, "6.6.6.6", "POST", "/api/rooms", body)
			if err != nil {
				t.Errorf("建房请求失败: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			switch st {
			case 200:
				st200++
			case 429:
				st429++
			case 503:
				st503++
			default:
				other++
			}
		}()
	}
	wg.Wait()
	if other != 0 {
		t.Errorf("出现意外状态码 ×%d", other)
	}
	if st200 != maxRoomsPerIP {
		t.Errorf("同 IP 并发 20 次应恰好成 %d 房，实际 %d", maxRoomsPerIP, st200)
	}
	if st429 != 10 || st503 != 8 {
		t.Errorf("分布应为 2/10/8，实际 200×%d 429×%d 503×%d", st200, st429, st503)
	}
	hubv.mu.Lock()
	n := len(hubv.rooms)
	hubv.mu.Unlock()
	if n != maxRoomsPerIP {
		t.Errorf("hub 应恰有 %d 房，实际 %d", maxRoomsPerIP, n)
	}
}

// WS 并发连接风暴：n 条连接同时 announce 同池，全员得正常应答；
// 全部断开后池应清空（连接关闭路径与 trkJanitor 式摘除并发）。
func TestWSConcurrentFlood(t *testing.T) {
	if testing.Short() {
		t.Skip("短测跳过连接风暴")
	}
	resetTrk()
	srv := trackerServer(t)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/tracker"
	const n = 24
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{})
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer c.CloseNow()
			b, err := json.Marshal(ann(jsRaw20(11), jsRaw20(byte(30+i)), nil))
			if err != nil {
				t.Errorf("marshal: %v", err)
				return
			}
			if err := c.Write(ctx, websocket.MessageText, b); err != nil {
				t.Errorf("write: %v", err)
				return
			}
			_, data, err := c.Read(ctx)
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			var m map[string]any
			if err := json.Unmarshal(data, &m); err != nil || num(m, "interval") != trkInterval {
				t.Errorf("应答异常: %s", data)
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if ok != n {
		t.Errorf("%d/%d 连接得到正常应答", ok, n)
	}
	// 断开后清理是异步的：轮询等连接关闭路径把池排干
	deadline := time.Now().Add(3 * time.Second)
	for {
		trk.Lock()
		peers := 0
		if s, exists := trk.swarms[ihHex(11)]; exists {
			peers = len(s.peers)
		}
		trk.Unlock()
		if peers == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("全员断开后池未排干: 剩 %d peer", peers)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestP2PWaitFor(t *testing.T) {
	const mb = 1 << 20
	cases := []struct {
		active, bytes int64
		want          time.Duration
	}{
		{0, 0, waitIdle},
		{0, 64 * mb, waitIdle},  // 余温不够档位
		{0, 128 * mb, waitBusy}, // 光出盘量也升档
		{1, 0, waitBusy},        // 一个在途就升档
		{2, 200 * mb, waitBusy},
		{3, 0, waitHeavy},        // 在途满三升顶
		{1, 512 * mb, waitHeavy}, // 出盘量过半 GB 升顶
	}
	for _, c := range cases {
		if got := p2pWaitFor(c.active, c.bytes); got != c.want {
			t.Fatalf("p2pWaitFor(%d, %dMB) = %v, 要 %v", c.active, c.bytes>>20, got, c.want)
		}
	}
}

func TestSeedBegin(t *testing.T) {
	active, _ := seedSnapshot()
	fin := seedBegin()
	if a, _ := seedSnapshot(); a != active+1 {
		t.Fatalf("在途数没涨: %d -> %d", active, a)
	}
	fin(300 << 20)
	if a, _ := seedSnapshot(); a != active {
		t.Fatalf("收尾后在途没回落: %d", a)
	}
	// 300MB 落窗:升到 busy 档;策略只看字节,不除时长,没有爆表问题
	_, recent := seedSnapshot()
	if recent < 300<<20 {
		t.Fatalf("窗口字节数没记上: %d", recent)
	}
	if got := p2pWaitFor(0, recent); got != waitBusy {
		t.Fatalf("300MB 应升 busy 档,得 %v", got)
	}
}
