// esbuild 打包：node builtin → 浏览器垫片别名，注入 Buffer/process 全局
import * as esbuild from 'esbuild'
import fs from 'node:fs/promises'
import path from 'node:path'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)

const alias = {
  events: 'events',
  path: 'path-browserify',
  stream: 'stream-browserify',
  string_decoder: 'string_decoder',
  util: 'util',
  os: 'os-browserify/browser',
  constants: 'constants-browserify',
  timers: 'timers-browserify',
  buffer: 'buffer',
  process: 'process/browser.js',
  global: 'esbuild-shim.js',
}

function mustReplace(src, from, to, label) {
  if (!src.includes(from)) throw new Error('lan-wt: 找不到 ' + label)
  return src.replace(from, to)
}

// 内网场景改 webtorrent/simple-peer 的公网默认：ICE 等 TURN Allocate、64KB 发送窗、
// webseed 流水线从 1 块起跳（测速低→窗口更低的死循环）。
const lanPatch = {
  name: 'lan-wt',
  setup(build) {
    // onLoad 拦不到默认命名空间里已有 loader 的 .js，要先 onResolve 换命名空间
    build.onResolve({ filter: /^@thaunknown\/simple-peer\/lite\.js$/ }, () => ({
      path: require.resolve('@thaunknown/simple-peer/lite.js'),
      namespace: 'lan-wt',
    }))
    build.onResolve({ filter: /^\.\/lib\/torrent\.js$/ }, args => {
      if (!args.resolveDir.includes('webtorrent')) return null
      return { path: path.join(args.resolveDir, 'lib/torrent.js'), namespace: 'lan-wt' }
    })
    build.onResolve({ filter: /^\.\/lib\/client\/websocket-tracker\.js$/ }, args => {
      if (!args.resolveDir.includes('bittorrent-tracker')) return null
      return { path: path.join(args.resolveDir, 'lib/client/websocket-tracker.js'), namespace: 'lan-wt' }
    })
    build.onLoad({ filter: /.*/, namespace: 'lan-wt' }, async args => {
      let t = await fs.readFile(args.path, 'utf8')
      if (args.path.includes('simple-peer')) {
        t = mustReplace(t, 'const MAX_BUFFERED_AMOUNT = 64 * 1024', 'const MAX_BUFFERED_AMOUNT = 4 * 1024 * 1024', 'MAX_BUFFERED_AMOUNT')
        // trickle:false，offer 必须等齐候选。800ms 够本地 STUN，不够 TURN Allocate（401+重试）。
        t = mustReplace(t, 'const ICECOMPLETE_TIMEOUT = 5 * 1000', 'const ICECOMPLETE_TIMEOUT = 2000', 'ICECOMPLETE_TIMEOUT')
      } else if (args.path.includes('websocket-tracker')) {
        // 多种同时 announce 会在共用 ws 上堆 once('connect')，默认 10 个就报警，续种卡死
        t = mustReplace(t,
          'this.socket = socketPool[this.announceUrl] = new Socket({ url: this.announceUrl, agent })\n      this.socket.consumers = 1',
          'this.socket = socketPool[this.announceUrl] = new Socket({ url: this.announceUrl, agent })\n      this.socket.setMaxListeners(0)\n      this.socket.consumers = 1',
          'ws maxListeners')
        t = mustReplace(t,
          `    if (!this.socket.connected) {
      this.socket.once('connect', () => {
        this.announce(opts)
      })
      return
    }`,
          `    if (!this.socket.connected) {
      this.socket.setMaxListeners(0)
      this.socket.once('connect', () => {
        this.announce(opts)
      })
      return
    }`,
          'ws announce wait')
      } else {
        t = mustReplace(t,
          `    if (IDLE_CALLBACK) {
      IDLE_CALLBACK(() => this._updateWireWrapper(), { timeout: 250 })
    } else {
      this._updateWireWrapper()
    }`,
          '    this._updateWireWrapper()',
          '_update idle')
        t = mustReplace(t,
          'let length = 2 + Math.ceil(duration * wire.downloadSpeed() / Piece.BLOCK_LENGTH)',
          'let length = 16 + Math.ceil(duration * wire.downloadSpeed() / Piece.BLOCK_LENGTH)',
          'block pipeline')
        t = mustReplace(t,
          'return 1 + Math.ceil(duration * wire.downloadSpeed() / pieceLength)',
          'return Math.max(8, 1 + Math.ceil(duration * wire.downloadSpeed() / pieceLength))',
          'piece pipeline')
      }
      return { contents: t, loader: 'js', resolveDir: path.dirname(args.path) }
    })
  },
}

// ICE 服务器来源：环境变量 ICE_SERVERS 优先，其次 scripts/local.env（KEY=value，
// git 忽略，放部署方自己的 STUN/TURN 凭据）。两边都没有 → []，只用 host 候选。
// 非空时必须能 JSON.parse 成数组，坏配置在打包时炸而不是页面上静默失联。
async function iceServersSrc() {
  let src = process.env.ICE_SERVERS || ''
  if (!src) {
    try {
      const m = (await fs.readFile('scripts/local.env', 'utf8')).match(/^ICE_SERVERS=(.*)$/m)
      if (m) src = m[1].replace(/^['"]|['"]$/g, '')
    } catch { /* 无 local.env，走默认 */ }
  }
  if (!src) return '[]'
  const v = JSON.parse(src)
  if (!Array.isArray(v)) throw new Error('ICE_SERVERS 必须是 JSON 数组')
  return src
}

const common = {
  bundle: true,
  minify: true,
  format: 'iife',
  platform: 'browser',
  // 不设 target 时 esbuild 默认 esnext：源码与依赖里的 ?./??/class fields 原样进
  // bundle，Chrome 80 之前的内核（微信 X5 老核、iOS 13 以下、旧 Android WebView）
  // 整包 SyntaxError——一行代码不执行，页面只剩 HTML 里静态的搜索框。es2018 降级
  // 语法后覆盖 Chromium 63+/Safari 11.1+；API 不在此垫（顶层初始化没用新 API）。
  target: ['es2018'],
  alias,
  inject: ['./esbuild-shim.js'],
  define: {
    'process.env.NODE_ENV': '"production"',
    'global': 'globalThis',
    // ICE 服务器构建期注入：环境变量 ICE_SERVERS（JSON 数组字符串），缺省 []。
    // 部署自己的 STUN/TURN 时传，如：ICE_SERVERS='[{"urls":["turn:stun.example.com:3478"],
    // "username":"u","credential":"p"}]' make build。也可写在 scripts/local.env（git 忽略），
    // 本脚本自动读。shared.js 里的 __ICE_SERVERS__ 占位符在打包时被这里的字面量替换。
    '__ICE_SERVERS__': JSON.stringify(await iceServersSrc()),
  },
  plugins: [lanPatch],
}

for (const [entry, out] of [
  ['web/app.js', 'web/app.bundle.js'],
  ['shareweb/app.js', 'shareweb/app.bundle.js'],
]) {
  await esbuild.build({ entryPoints: [entry], outfile: out, ...common })
  console.log('built', out)
}
