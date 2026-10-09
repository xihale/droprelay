package main

import (
	"encoding/binary"
	"net"
	"sync"
	"syscall"
)

// pktinfoPC：通配 UDP 上按请求目的地址回包。内核默认按出接口选源，
// 多宿主上打到 A 地址的 STUN 会被 B 地址回，浏览器按对端过滤直接丢。
type pktinfoPC struct {
	*net.UDPConn
	mu  sync.Mutex
	dst map[string]dstInfo
}

type dstInfo struct {
	ip    net.IP
	ifidx int
}

func newPktinfoPC(uc *net.UDPConn) *pktinfoPC {
	return &pktinfoPC{UDPConn: uc, dst: make(map[string]dstInfo)}
}

func pktinfoControl(_, _ string, c syscall.RawConn) error {
	var soErr error
	c.Control(func(fd uintptr) {
		soErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_PKTINFO, 1)
	})
	return soErr
}

func (c *pktinfoPC) ReadFrom(b []byte) (int, net.Addr, error) {
	oob := make([]byte, 128)
	n, oobn, _, addr, err := c.UDPConn.ReadMsgUDP(b, oob)
	if err != nil {
		return n, addr, err
	}
	if addr != nil {
		if ip, ifidx := pktinfoDst(oob[:oobn]); ip != nil {
			c.mu.Lock()
			c.dst[addr.String()] = dstInfo{ip: ip, ifidx: ifidx}
			c.mu.Unlock()
		}
	}
	return n, addr, nil
}

func (c *pktinfoPC) WriteTo(b []byte, addr net.Addr) (int, error) {
	ua, ok := addr.(*net.UDPAddr)
	if !ok {
		return c.UDPConn.WriteTo(b, addr)
	}
	c.mu.Lock()
	info, hit := c.dst[ua.String()]
	c.mu.Unlock()
	if !hit {
		return c.UDPConn.WriteTo(b, ua)
	}
	n, _, err := c.UDPConn.WriteMsgUDP(b, pktinfoOut(info.ip, info.ifidx), ua)
	return n, err
}

// pktinfoDst 从控制信息解出请求的目的地址与本机接口序号（linux in_pktinfo：
// ifindex 4 字节本机序 + spec_dst 4 字节 + addr 4 字节）
func pktinfoDst(oob []byte) (net.IP, int) {
	cmsgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, 0
	}
	for _, cm := range cmsgs {
		if cm.Header.Level == syscall.SOL_IP && cm.Header.Type == syscall.IP_PKTINFO && len(cm.Data) >= 12 {
			ifidx := int(binary.NativeEndian.Uint32(cm.Data[0:4]))
			return net.IP(append([]byte(nil), cm.Data[4:8]...)), ifidx
		}
	}
	return nil, 0
}

// pktinfoOut 组 IP_PKTINFO 控制信息（cmsghdr 16 字节 + in_pktinfo 12 字节），
// 让内核以 dst 为源地址发包
func pktinfoOut(dst net.IP, ifidx int) []byte {
	oob := make([]byte, 28)
	binary.NativeEndian.PutUint64(oob[0:8], 28)
	binary.NativeEndian.PutUint32(oob[8:12], syscall.SOL_IP)
	binary.NativeEndian.PutUint32(oob[12:16], syscall.IP_PKTINFO)
	binary.NativeEndian.PutUint32(oob[16:20], uint32(ifidx))
	copy(oob[20:24], dst.To4())
	return oob
}
