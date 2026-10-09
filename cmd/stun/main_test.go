package main

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/pion/turn/v4"
)

func bindingRequest() []byte {
	req := make([]byte, stunHeaderSize)
	binary.BigEndian.PutUint16(req[0:2], typeBindingReq)
	binary.BigEndian.PutUint32(req[4:8], magicCookie)
	for i := 8; i < 20; i++ {
		req[i] = byte(i)
	}
	return req
}

func TestBindingRoundtripIPv4(t *testing.T) {
	srv, addr, err := startServer("127.0.0.1:0", defaultUser, defaultPass, defaultRealm, net.IPv4(127, 0, 0, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	dst, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := bindingRequest()
	if _, err := client.WriteTo(req, dst); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	n, _, err := client.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseXorMapped(buf[:n], req[8:20])
	if err != nil {
		t.Fatal(err)
	}
	local := client.LocalAddr().(*net.UDPAddr)
	want := (&net.UDPAddr{IP: local.IP.To4(), Port: local.Port}).String()
	if got != want {
		t.Fatalf("反射地址不符：got %s want %s", got, want)
	}
}

func TestTURNAllocate(t *testing.T) {
	srv, addr, err := startServer("127.0.0.1:0", defaultUser, defaultPass, defaultRealm, net.IPv4(127, 0, 0, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cl, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           conn,
		Username:       defaultUser,
		Password:       defaultPass,
		Realm:          defaultRealm,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Listen(); err != nil {
		t.Fatal(err)
	}

	mapped, err := cl.SendBindingRequest()
	if err != nil {
		t.Fatal(err)
	}
	if mapped == nil {
		t.Fatal("Binding 没返回映射地址")
	}

	rel, err := cl.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	defer rel.Close()
	ua, ok := rel.LocalAddr().(*net.UDPAddr)
	if !ok || ua.IP.String() != "127.0.0.1" {
		t.Fatalf("中继地址不符: %v", rel.LocalAddr())
	}
	if ua.Port < relayMinPort || ua.Port > relayMaxPort {
		t.Fatalf("中继端口不在 %d-%d: %d", relayMinPort, relayMaxPort, ua.Port)
	}
	if srv.AllocationCount() < 1 {
		t.Fatal("服务端没记下 allocation")
	}
}

func TestPrivatePeer(t *testing.T) {
	if !privatePeer(nil, net.ParseIP("10.22.1.9")) {
		t.Fatal("私网 10/8 应放行")
	}
	if !privatePeer(nil, net.ParseIP("192.168.0.2")) {
		t.Fatal("私网 192.168/16 应放行")
	}
	if privatePeer(nil, net.ParseIP("8.8.8.8")) {
		t.Fatal("公网应拒绝")
	}
}

func TestAllocGate(t *testing.T) {
	g := newAllocGate(3, time.Minute)
	ua := &net.UDPAddr{IP: net.ParseIP("10.30.4.158"), Port: 1}
	other := &net.UDPAddr{IP: net.ParseIP("10.44.206.37"), Port: 2}
	for i := 0; i < 3; i++ {
		if !g.Allow(ua) {
			t.Fatalf("第 %d 次不该拒", i+1)
		}
	}
	if g.Allow(ua) {
		t.Fatal("窗口内超限应拒")
	}
	if !g.Allow(other) {
		t.Fatal("别的 IP 不应被牵连")
	}
	// 同一 IP 换端口(浏览器每 PC 一个源口)应算同一桶
	if g.Allow(&net.UDPAddr{IP: net.ParseIP("10.30.4.158"), Port: 9999}) {
		t.Fatal("换端口不应绕开限速")
	}
}

func TestPktinfoRoundtrip(t *testing.T) {
	dst := net.IPv4(10, 254, 1, 2)
	out := pktinfoOut(dst, 7)
	got, ifidx := pktinfoDst(out)
	if got == nil || ifidx != 7 {
		t.Fatalf("pktinfo 编解码不符: dst=%v ifidx=%d", got, ifidx)
	}
	if !got.Equal(dst) {
		t.Fatalf("目的地址不符: got %s want %s", got, dst)
	}
	if binary.NativeEndian.Uint64(out[0:8]) != uint64(len(out)) {
		t.Fatalf("cmsg_len 不符")
	}
}

func TestPickRelayFrom(t *testing.T) {
	pick := func(addrs ...string) string {
		ips := make([]net.IP, 0, len(addrs))
		for _, s := range addrs {
			ips = append(ips, net.ParseIP(s))
		}
		return pickRelayFrom(ips).String()
	}
	if got := pick("192.168.1.1", "10.254.1.2", "172.18.0.1"); got != "10.254.1.2" {
		t.Fatalf("10/8 应优先: %s", got)
	}
	if got := pick("192.168.1.1", "172.18.0.1"); got != "172.18.0.1" {
		t.Fatalf("172.16/12 次之: %s", got)
	}
	if got := pick("192.168.1.1", "169.254.3.4", "127.0.0.1"); got != "192.168.1.1" {
		t.Fatalf("环回/链路本地应跳过: %s", got)
	}
	if got := pick(); got != "127.0.0.1" {
		t.Fatalf("无可用地址应兜底 127.0.0.1: %s", got)
	}
}
