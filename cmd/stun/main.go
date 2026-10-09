// cmd/stun —— 内网自托管 STUN + TURN。
// STUN Binding 给能打洞的人反射地址；打不洞或洞不稳时 WebRTC 走 TURN 中继。
// 中继地址用对外可达的本机 IPv4（多宿主机器建议 -relay 显式指定）。
// 通配监听开 IP_PKTINFO：多宿主上打到 A 地址的包不能从 B 地址回。
// 用法：droprelay-stun [-addr 0.0.0.0:3478 -user u -pass p -realm example.com -relay ip]；
// -probe <host:port> 自测 Binding。
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/pion/logging"
	"github.com/pion/turn/v4"
)

const (
	magicCookie      = 0x2112A442
	typeBindingReq   = 0x0001
	typeBindingResp  = 0x0101
	attrXorMapped    = 0x0020
	stunHeaderSize   = 20
	probeWaitTimeout = 3 * time.Second

	defaultUser  = "droprelay"
	defaultPass  = "change-me"
	defaultRealm = "droprelay.invalid"
	relayMinPort = 49152
	relayMaxPort = 49311
	// 全局配额受中继端口段（49152-49311 共 160 个）约束，留两成余量。
	// 打满即新 Allocate 全收 486，客户端 ICE 无中继候选可用，静默失败。
	maxAlloc = 128

	// 单 IP Allocate 限速：shareweb 开页即建 5 个 PC、每个都 Allocate，
	// webtorrent 每次 announce 重拨又是一轮。不限速时几轮 gather 就把全局
	// 配额吃光，后来者全 486——浏览器侧只会报「TURN broken」。
	perIPAllocWindow = 10 * time.Minute
	perIPMaxAlloc    = 60
)

func main() {
	addr := flag.String("addr", "0.0.0.0:3478", "STUN/TURN UDP 监听")
	relay := flag.String("relay", "", "XOR-RELAYED-ADDRESS：对 peer 可达的本机 IPv4；多宿主机器建议显式指定，空则按 10/8→172.16/12→192.168/16 自动挑")
	user := flag.String("user", defaultUser, "TURN 用户名（须与前端 ICE_SERVERS 一致）")
	pass := flag.String("pass", defaultPass, "TURN 密码（须与前端 ICE_SERVERS 一致）")
	realm := flag.String("realm", defaultRealm, "TURN realm")
	probe := flag.String("probe", "", "不自建服务：向 <host:port> 发一发 Binding，打印反射地址后退出")
	flag.Parse()

	if *probe != "" {
		if err := runProbe(*probe); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *pass == defaultPass {
		log.Printf("警告：TURN 密码还是默认值 %q——正式部署前改掉，并与前端 ICE_SERVERS 的 credential 配对", defaultPass)
	}

	var relayIP net.IP
	if *relay != "" {
		relayIP = net.ParseIP(*relay)
		if relayIP == nil {
			log.Fatalf("relay 不是 IP: %s", *relay)
		}
	}

	srv, _, err := startServer(*addr, *user, *pass, *realm, relayIP)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

func startServer(addr, user, pass, realm string, relay net.IP) (*turn.Server, string, error) {
	lc := net.ListenConfig{Control: pktinfoControl}
	ln, err := lc.ListenPacket(context.Background(), "udp4", addr)
	if err != nil {
		return nil, "", err
	}
	uc := ln.(*net.UDPConn)
	pc := newPktinfoPC(uc)
	actual := uc.LocalAddr().String()

	if relay == nil {
		relay = pickRelay()
	}
	relayIP := relay.To4()
	if relayIP == nil {
		pc.Close()
		return nil, "", fmt.Errorf("relay 不是 IPv4: %s", relay)
	}

	lf := logging.NewDefaultLoggerFactory()
	lf.DefaultLogLevel = logging.LogLevelError
	authKey := turn.GenerateAuthKey(user, realm, pass)
	allocs := newAllocGate(perIPMaxAlloc, perIPAllocWindow)

	var srv *turn.Server
	srv, err = turn.NewServer(turn.ServerConfig{
		Realm:         realm,
		LoggerFactory: lf,
		AuthHandler: func(username, _ string, src net.Addr) ([]byte, bool) {
			if username != user {
				return nil, false
			}
			log.Printf("turn %s %s", username, src)
			return authKey, true
		},
		QuotaHandler: func(_ string, _ string, src net.Addr) bool {
			if srv != nil && srv.AllocationCount() >= maxAlloc {
				log.Printf("turn 配额满 %s", src)
				return false
			}
			if !allocs.Allow(src) {
				log.Printf("turn 限速 %s", src)
				return false
			}
			return true
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: pc,
			RelayAddressGenerator: &turn.RelayAddressGeneratorPortRange{
				RelayAddress: relayIP,
				Address:      relayIP.String(),
				MinPort:      relayMinPort,
				MaxPort:      relayMaxPort,
			},
			PermissionHandler: privatePeer,
		}},
	})
	if err != nil {
		pc.Close()
		return nil, "", err
	}
	log.Printf("stun+turn 监听 %s（中继 %s:%d-%d）", actual, relayIP, relayMinPort, relayMaxPort)
	return srv, actual, nil
}

// pickRelay 没显式 -relay 时从本机 IPv4 里挑中继地址，兜底策略见 pickRelayFrom。
func pickRelay() net.IP {
	has := localIPv4()
	ips := make([]net.IP, 0, len(has))
	for _, ip := range has {
		ips = append(ips, ip)
	}
	return pickRelayFrom(ips)
}

// pickRelayFrom 多宿主机器（如 WAN 10/8 + 内网 192.168/16）哪个地址对 peer 可达
// 只有部署者知道，建议显式 -relay；这里给个确定性兜底：10/8 → 172.16/12 →
// 192.168/16 → 其余，跳过环回与链路本地，同组按字典序取最小。
func pickRelayFrom(ips []net.IP) net.IP {
	var groups [4][]net.IP
	for _, ip := range ips {
		ip = ip.To4()
		if ip == nil {
			continue
		}
		switch {
		case ip.IsLoopback() || ip.IsLinkLocalUnicast():
		case ip.IsPrivate() && ip[0] == 10:
			groups[0] = append(groups[0], ip)
		case ip.IsPrivate() && ip[0] == 172 && ip[1]&0xf0 == 16:
			groups[1] = append(groups[1], ip)
		case ip.IsPrivate() && ip[0] == 192 && ip[1] == 168:
			groups[2] = append(groups[2], ip)
		default:
			groups[3] = append(groups[3], ip)
		}
	}
	for _, g := range groups {
		if len(g) == 0 {
			continue
		}
		sort.Slice(g, func(i, j int) bool { return g[i].String() < g[j].String() })
		return g[0]
	}
	return net.IPv4(127, 0, 0, 1)
}

func localIPv4() map[string]net.IP {
	out := map[string]net.IP{}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return out
	}
	for _, a := range addrs {
		in, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := in.IP.To4()
		if ip == nil {
			continue
		}
		out[ip.String()] = ip
	}
	return out
}

func privatePeer(_ net.Addr, peerIP net.IP) bool {
	if peerIP.IsPrivate() || peerIP.IsLoopback() || peerIP.IsLinkLocalUnicast() {
		return true
	}
	log.Printf("turn 拒绝公网 peer %s", peerIP)
	return false
}

// allocGate 按源 IP 记 Allocate 时间戳,窗口内超限即拒。
// pion 只在 Allocate 时回调 QuotaHandler,分配何时释放无从得知,
// 做不了精确的「并发数/每 IP」;滑动窗口限制新增速率,效果等价——
// 分配滞留再久,也涨不过窗口内的增速上限。
type allocGate struct {
	mu     sync.Mutex
	window time.Duration
	max    int
	hits   map[string][]time.Time
}

func newAllocGate(max int, window time.Duration) *allocGate {
	return &allocGate{window: window, max: max, hits: map[string][]time.Time{}}
}

func (g *allocGate) Allow(src net.Addr) bool {
	key := src.String()
	if ua, ok := src.(*net.UDPAddr); ok {
		key = ua.IP.String()
	}
	now := time.Now()

	g.mu.Lock()
	defer g.mu.Unlock()
	live := g.hits[key][:0]
	for _, t := range g.hits[key] {
		if now.Sub(t) < g.window {
			live = append(live, t)
		}
	}
	if len(live) >= g.max {
		g.hits[key] = live
		return false
	}
	g.hits[key] = append(live, now)

	if len(g.hits) > 1024 { // 防死键堆积:全表扫一遍,窗口外整键清掉
		for k, ts := range g.hits {
			keep := ts[:0]
			for _, t := range ts {
				if now.Sub(t) < g.window {
					keep = append(keep, t)
				}
			}
			if len(keep) == 0 {
				delete(g.hits, k)
			} else {
				g.hits[k] = keep
			}
		}
	}
	return true
}

func runProbe(target string) error {
	c, err := net.DialTimeout("udp", target, probeWaitTimeout)
	if err != nil {
		return err
	}
	defer c.Close()

	req := make([]byte, stunHeaderSize)
	binary.BigEndian.PutUint16(req[0:2], typeBindingReq)
	binary.BigEndian.PutUint32(req[4:8], magicCookie)
	for i := 8; i < 20; i++ {
		req[i] = byte(i)
	}
	if _, err := c.Write(req); err != nil {
		return err
	}
	c.SetReadDeadline(time.Now().Add(probeWaitTimeout))
	buf := make([]byte, 1500)
	n, err := c.Read(buf)
	if err != nil {
		return fmt.Errorf("%s 无应答: %w", target, err)
	}
	addr, err := parseXorMapped(buf[:n], req[8:20])
	if err != nil {
		return err
	}
	fmt.Printf("%s → 我的地址（服务端看来）: %s\n", target, addr)
	return nil
}

func parseXorMapped(resp, txid []byte) (string, error) {
	if len(resp) < stunHeaderSize {
		return "", fmt.Errorf("应答过短")
	}
	if binary.BigEndian.Uint16(resp[0:2]) != typeBindingResp {
		return "", fmt.Errorf("不是 Binding Response")
	}
	for i := range txid {
		if resp[8+i] != txid[i] {
			return "", fmt.Errorf("事务 ID 不匹配")
		}
	}
	attrs := resp[stunHeaderSize:]
	for len(attrs) >= 4 {
		typ := binary.BigEndian.Uint16(attrs[0:2])
		n := int(binary.BigEndian.Uint16(attrs[2:4]))
		if 4+n > len(attrs) {
			return "", fmt.Errorf("属性越界")
		}
		if typ == attrXorMapped && n >= 8 {
			port := binary.BigEndian.Uint16(attrs[6:8]) ^ uint16(magicCookie>>16)
			if attrs[5] == 0x01 {
				ip := make(net.IP, 4)
				cookie := make([]byte, 4)
				binary.BigEndian.PutUint32(cookie, magicCookie)
				for i := range ip {
					ip[i] = attrs[8+i] ^ cookie[i]
				}
				return (&net.UDPAddr{IP: ip, Port: int(port)}).String(), nil
			}
			if attrs[5] == 0x02 && n >= 20 {
				seed := make([]byte, 16)
				binary.BigEndian.PutUint32(seed, magicCookie)
				copy(seed[4:], txid)
				ip := make(net.IP, 16)
				for i := range ip {
					ip[i] = attrs[8+i] ^ seed[i]
				}
				return (&net.UDPAddr{IP: ip, Port: int(port)}).String(), nil
			}
		}
		attrs = attrs[4+((n+3)&^3):]
	}
	return "", fmt.Errorf("应答里没有 XOR-MAPPED-ADDRESS")
}
