#!/bin/sh
# 按 md5 推到目标机器：hash 一样就不动、不重启。
set -e
: "${HOST:?用法: HOST=user@host make deploy}"

push() {
	src=$1
	dest=$2
	svc=$3
	local_md5=$(md5sum "$src" | awk '{print $1}')
	remote_md5=$(ssh "$HOST" "md5sum '$dest' 2>/dev/null || true" | awk '{print $1}')
	if [ -n "$remote_md5" ] && [ "$local_md5" = "$remote_md5" ]; then
		echo "skip $dest"
		return
	fi
	echo "push $src → $dest"
	scp "$src" "$HOST:/tmp/$(basename "$src")"
	ssh "$HOST" "/etc/init.d/$svc stop; mv -f '/tmp/$(basename "$src")' '$dest' && chmod 0755 '$dest' && /etc/init.d/$svc start"
}

push droprelay-linux-arm64 /usr/bin/droprelay droprelay
push droprelay-stun-linux-arm64 /usr/bin/droprelay-stun droprelay-stun
