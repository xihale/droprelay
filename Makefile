# linux/arm64 static → $HOST:/usr/bin/droprelay + droprelay-stun && restart
HOST ?=
BIN  := droprelay-linux-arm64
STUN := droprelay-stun-linux-arm64

.PHONY: all web build deploy stun stun-fw

all: build

stun: ## 本机 STUN+TURN；droprelay-stun -probe 127.0.0.1:3478 自测
	go build -trimpath -ldflags='-s -w' -o droprelay-stun ./cmd/stun

# WAN 侧 UDP 3478 + 49152-49311 要在路由器防火墙放行（详见 README「部署」）
stun-fw: ## 目标路由器 UCI 放行 WAN 上 STUN/TURN 端口
	ssh $(HOST) 'sh -s' < scripts/openwrt-stun-fw.sh

web:
	npm install
	node build-web.mjs

build: web
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags='-s -w' -o $(BIN) .
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags='-s -w' -o $(STUN) ./cmd/stun

deploy: build
	HOST=$(HOST) sh scripts/deploy.sh
