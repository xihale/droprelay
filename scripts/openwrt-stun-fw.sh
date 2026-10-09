#!/bin/sh
# WAN 侧 INPUT 默认拒绝时放行 STUN/TURN；在路由器上跑，或 make stun-fw。
set -e
add() {
	n=$1
	p=$2
	if uci show firewall | grep -q "name='$n'"; then
		echo already "$n"
		return
	fi
	uci add firewall rule >/dev/null
	uci set firewall.@rule[-1].name="$n"
	uci set firewall.@rule[-1].src='wan'
	uci set firewall.@rule[-1].proto='udp'
	uci set firewall.@rule[-1].dest_port="$p"
	uci set firewall.@rule[-1].family='ipv4'
	uci set firewall.@rule[-1].target='ACCEPT'
	echo added "$n"
}
add Allow-WAN-STUN 3478
add Allow-WAN-TURN-Relay 49152-49311
uci commit firewall
/etc/init.d/firewall reload
nft list chain inet fw4 input_wan | grep -E '3478|49152' || true
