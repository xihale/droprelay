/* drop / share 两页共用的小件。只收「两边逐字相同」的东西——
   页面各自的状态机、UI 语义与落盘策略留在各自 app.js。
   sink 三兄弟（testSink/memSink/opfsSink）只认 (key, name)，
   与调用方的记录形状解耦，谁也不许再单方面改签名。 */

export const TRACKER = (location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host + '/tracker'

// ICE 服务器构建期注入：ICE_SERVERS='<json 数组>' node build-web.mjs（见 build-web.mjs）。
// 缺省空 = 只用 host 候选，同网段直连；跨 NAT 的部署才需要自托管 STUN/TURN，
// 例：[{"urls":["stun:stun.example.com:3478"]},
//     {"urls":["turn:stun.example.com:3478"],"username":"u","credential":"p"}]
// __ICE_SERVERS__ 是裸标识符：打包时被 build-web.mjs 的 define 换成 JSON 数组字符串字面量。
export const ICE = { iceServers: JSON.parse(__ICE_SERVERS__) }

// webtorrent 3 把 rtcConfig 挪进 tracker 选项；只传顶层 rtcConfig 会被静默忽略，
// 客户端落到 simple-peer 默认的 Google/Twilio 公网 STUN。tracker 协议还关了 trickle，
// ICE 等公网超时（~5s）才发出 offer，连上后可能走公网反射地址，开头只有几百 KB/s。
export function wtOpts() {
  return {
    rtcConfig: ICE,
    tracker: { rtcConfig: ICE },
    maxConns: 64, // 教室这点人 × 几种文件，尽量全连上
  }
}

// 个位数～十位数做种：不掐人、webseed 一路顺序打 USB、
// 块大所以内存只缓存 2 块（默认 20 块 × 32MB 会把手机打爆）。
export function swarmOpts() {
  return { strategy: 'sequential', uploads: 32, maxWebConns: 1, storeCacheSlots: 2 }
}

// P2P 等待窗:服务器正忙着从盘上直出文件时拉长——那会儿 HTTP 兜底又慢
// 又占盘,P2P 建链多半只是慢而非死,值得等;空闲维持 8s 快进快出。
// 策略在服务端算好(p2pWaitMs 字段),这里 15s 缓存一次;端点拿不到就
// 沿用旧值——服务器不可达时 HTTP 兜底同样要悬,没必要反复打。
let waitHint = { at: 0, ms: 8000 }
export async function p2pWaitMs() {
  const now = Date.now()
  if (now - waitHint.at < 15000) return waitHint.ms
  waitHint.at = now
  try {
    const r = await fetch('/api/health', { cache: 'no-store' })
    const j = await r.json()
    if (Number.isFinite(j.p2pWaitMs)) waitHint.ms = Math.min(Math.max(j.p2pWaitMs, 0), 120000)
  } catch { /* 旧值兜底 */ }
  return waitHint.ms
}

// 与 Go pieceLenPolicy 对齐。做种人少，块按「几十块/文件」来：少 OPFS 文件、少校验。
export function pieceLenOf(size) {
  if (size < 64 << 20) return 4 << 20
  if (size < 512 << 20) return 16 << 20
  return 32 << 20
}

export function pieceLenFromTorrent(buf) {
  const n = Math.min(buf.length, 16384)
  let s = ''
  for (let i = 0; i < n; i++) s += String.fromCharCode(buf[i])
  const m = /12:piece lengthi(\d+)e/.exec(s)
  return m ? +m[1] : 0
}

// 全零 bitfield：webtorrent 入池默认会把空 OPFS 逐块 get+hash 一遍（每块还 create:true），
// 大文件「连接中」卡好几秒。给对得上的空 bitfield 走 bitfield 捷径，一块都不读。
export function emptyBitfield(size, pieceLen) {
  const pl = pieceLen || pieceLenOf(size)
  const n = Math.ceil(size / pl) || 1
  return new Uint8Array(Math.ceil(n / 8))
}

export function withTimeout(p, ms) {
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error('超时')), ms)
    p.then(v => { clearTimeout(t); res(v) }, e => { clearTimeout(t); rej(e) })
  })
}

// 每种一个 OPFS 整文件（data），按块偏移读写。教室这点人用不着一块一个文件。
export class OneFileStore {
  constructor(chunkLength, opts = {}) {
    this.chunkLength = Number(chunkLength)
    this.length = Number(opts.length) || 0
    this.name = opts.name || 'store'
    this.lastChunkLength = this.length % this.chunkLength || this.chunkLength
    this.lastChunkIndex = Math.max(0, Math.ceil(this.length / this.chunkLength) - 1)
    this._root = navigator.storage.getDirectory()
    this._fh = null
    this.closed = false
  }

  async _handle() {
    if (!this._fh) {
      const root = await this._root
      const dir = await root.getDirectoryHandle(this.name, { create: true })
      this._dir = dir
      this._fh = await dir.getFileHandle('data', { create: true })
    }
    return this._fh
  }

  async writeFrom(blob) {
    const fh = await this._handle()
    const w = await fh.createWritable({ keepExistingData: false })
    const reader = blob.stream().getReader()
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        await w.write(value)
      }
      await w.close()
    } catch (e) {
      try { await w.abort() } catch { /* */ }
      throw e
    }
  }

  put(index, buf, cb = () => {}) {
    this._put(index, buf).then(() => cb(null), err => cb(err))
  }

  async _put(index, buf) {
    if (this.closed) throw new Error('Storage is closed')
    const fh = await this._handle()
    const w = await fh.createWritable({ keepExistingData: true })
    await w.seek(index * this.chunkLength)
    await w.write(buf)
    await w.close()
  }

  get(index, opts, cb) {
    if (typeof opts === 'function') { cb = opts; opts = {} }
    this._get(index, opts || {}).then(buf => cb(null, buf), err => cb(err))
  }

  async _get(index, opts) {
    if (this.closed) throw new Error('Storage is closed')
    const fh = await this._handle()
    const blob = await fh.getFile()
    const start = index * this.chunkLength + (opts.offset || 0)
    const pieceLen = index === this.lastChunkIndex ? this.lastChunkLength : this.chunkLength
    const len = opts.length != null ? opts.length : pieceLen - (opts.offset || 0)
    return new Uint8Array(await blob.slice(start, start + len).arrayBuffer())
  }

  close(cb) { this.closed = true; if (cb) cb(null) }

  destroy(cb) {
    this.closed = true
    this._root.then(root => root.removeEntry(this.name, { recursive: true }))
      .then(() => cb && cb(null), err => cb && cb(err))
  }
}

export function human(n) {
  if (!Number.isFinite(n) || n < 0) return '—'
  const k = 1024
  if (n >= k ** 3) return (n / k ** 3).toFixed(2) + ' GB'
  if (n >= k ** 2) return (n / k ** 2).toFixed(1) + ' MB'
  if (n >= k) return (n / k).toFixed(1) + ' KB'
  return n + ' B'
}

export function canDirPicker() {
  return !!(window.showDirectoryPicker && matchMedia('(hover: hover) and (pointer: fine)').matches)
}

export function canSavePicker() {
  return !!(window.showSaveFilePicker && matchMedia('(hover: hover) and (pointer: fine)').matches)
}

/* fsSink：串行可写流。HTTP 块经常只有十几 KB，逐块 write 会让 FSA 把真正落盘拖到 close()——
   进度条 100% 之后还要再等一轮。攒到 1MB 再写。 */
export function fsSink(w) {
  let q = Promise.resolve()
  const run = fn => { q = q.then(fn, fn); return q }
  const parts = []
  let n = 0
  const flush = () => {
    if (!n) return
    let data
    if (parts.length === 1) data = parts[0]
    else {
      data = new Uint8Array(n)
      let off = 0
      for (const p of parts) {
        const u = p instanceof Uint8Array ? p : new Uint8Array(p)
        data.set(u, off)
        off += u.byteLength
      }
    }
    parts.length = 0
    n = 0
    return w.write(data)
  }
  return {
    write: c => run(() => {
      const len = c.byteLength != null ? c.byteLength : c.length
      parts.push(c)
      n += len
      if (n >= 1 << 20) return flush()
    }),
    close: () => run(async () => { await flush(); await w.close() }),
    abort: () => run(() => { parts.length = 0; n = 0; return w.abort() }),
  }
}

// blob URL / OPFS 中转要活过浏览器把文件拷进「下载」：按体积估，最快按 2MB/s，顶 30 分钟
function blobTtl(size) {
  return Math.min(30 * 60 * 1000, Math.max(120000, (Number(size) || 0) / (2 << 20) * 1000))
}

export function saveBlob(blob, name) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = name
  a.rel = 'noopener'
  a.style.display = 'none'
  document.body.appendChild(a)
  a.click()
  setTimeout(() => { a.remove(); URL.revokeObjectURL(url) }, blobTtl(blob && blob.size))
}

/* ?testdump=1 校验 sink：块攒内存，close 时算 sha256 挂到 window.__result[key] */
export function testSink(key, name) {
  const chunks = []
  let n = 0
  return {
    write: async c => { chunks.push(c); n += c.byteLength },
    close: async () => {
      const all = new Uint8Array(n)
      let off = 0
      for (const c of chunks) { all.set(new Uint8Array(c), off); off += c.byteLength }
      const d = await crypto.subtle.digest('SHA-256', all)
      const hex = [...new Uint8Array(d)].map(b => b.toString(16).padStart(2, '0')).join('')
      ;(window.__result = window.__result || {})[key] = { name, size: n, sha256: hex }
    },
  }
}

/* 无 FSA 兜底：整份攒内存，close 时触发浏览器自带下载 */
export function memSink(name) {
  const chunks = []
  let file
  return {
    write: async c => chunks.push(c),
    close: async () => {
      file = new Blob(chunks)
      chunks.length = 0
      saveBlob(file, name)
    },
    seedFile: async () => file,
  }
}

/* OPFS 中转 sink：边收边落 OPFS「in/key」，close 时转浏览器下载。
   中转文件要活过浏览器拷进「下载」+事后灌块库，TTL 按体积估，不是固定 60s。
   代数守卫：同 key 又开了一笔新下载时，旧 close 留下的迟到定时器不得把新中转删掉。 */
const opfsGen = new Map()

export async function opfsSink(key, name) {
  if (!navigator.storage || !navigator.storage.getDirectory) return null
  const k = String(key)
  const gen = (opfsGen.get(k) || 0) + 1
  opfsGen.set(k, gen)
  try {
    const root = await withTimeout(navigator.storage.getDirectory(), 2000)
    const dir = await withTimeout(root.getDirectoryHandle('in', { create: true }), 2000)
    const fh = await withTimeout(dir.getFileHandle(k, { create: true }), 2000)
    const w = await withTimeout(fh.createWritable({ keepExistingData: false }), 2000)
    const sink = fsSink(w)
    let file
    return {
      write: c => sink.write(c),
      close: async () => {
        await sink.close()
        file = await fh.getFile()
        saveBlob(file, name)
        setTimeout(() => { if (opfsGen.get(k) === gen) dir.removeEntry(k).catch(() => {}) }, blobTtl(file.size))
      },
      seedFile: async () => file || fh.getFile(),
      abort: async () => {
        try { await sink.abort() } catch { /* 已关 */ }
        try { await dir.removeEntry(k) } catch { /* 没有 */ }
      },
    }
  } catch {
    return null
  }
}

/* 传输中断管线：makeStop 给出可反复触发的 stop()（看门狗与用户取消共用一条路），
   drainReader 里 read() 与中断信号赛跑——挂起的读取立刻落败，绝不吊死队列 */
export function makeStop() {
  const ac = new AbortController()
  return {
    signal: ac.signal,
    stop: why => { if (!ac.signal.aborted) ac.abort(why || new Error('取消')) },
  }
}

export async function drainReader(reader, sink, signal, onChunk) {
  const stopped = new Promise((_, rej) =>
    signal.addEventListener('abort', () => rej(signal.reason || new Error('取消')), { once: true }))
  for (;;) {
    const { done: fin, value } = await Promise.race([reader.read(), stopped])
    if (fin) break
    await sink.write(value)
    if (onChunk) await onChunk(value)
  }
}
