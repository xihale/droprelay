/* drop 前端 v3 —— WebTorrent 内核：发送端逐文件做种，接收端加池即收。
   拖入文件/文件夹或点上传井即做种（可持续添加，每文件一个池），输入码即接收；
   文件夹：拖拽遍历或右键/长按上传井整夹选入，相对路径随清单下发，
   接收端按目录分组展示（与 share 站同范式），落盘一律拍平为平铺文件（重名自动去重）；
   接收端点「下载」逐个落盘。信令走 ws tracker（/tracker），字节只过 WebRTC 直连，
   服务器不经手数据、也没有中转兜底：直连打不通即失败（内网场景足够）。
   共享桌：桌面端每文件一枚发丝线卡，拖动理序（仅本地），拖出桌面即移出；
   触屏仍是裸行，左右划走。
   断线/刷新自愈：会话存 sessionStorage（码/凭证/清单），发送端文件本体另存 IndexedDB，
   刷新后原房续种。清单与行列表只在进房时快照——发送端中途增投，接收端刷新后可见。
   调试参数：?testdump=1 接收端不落盘，结果在 window.__result（sha256 校验用）；
   ?iab=1 模拟微信内置浏览器（亮引导横幅+拦下载+藏做种+安卓尝试拉起外部浏览器） */
'use strict'

import WebTorrent from 'webtorrent'
import { TRACKER, wtOpts, swarmOpts, pieceLenOf, emptyBitfield, human, canDirPicker, canSavePicker, fsSink, testSink, memSink, opfsSink, makeStop, drainReader } from '../common/shared.js'

const $ = id => document.getElementById(id)
const params = new URLSearchParams(location.search)
const TESTDUMP = params.get('testdump') === '1'
// 微信/QQ 等 App 内置浏览器没有下载能力（anchor download 被吞、FSA 缺席、WebRTC 残缺），
// 进站即亮横幅引导复制链接去系统浏览器，接收动作一律拦下别让传输白跑。
// 调试钩子 ?iab=1 以微信 UA 走同一条检测路径
const IAB = (() => {
  const ua = params.get('iab') === '1' ? 'MicroMessenger' : navigator.userAgent
  if (/MicroMessenger/i.test(ua)) return '微信'
  if (/QQ\/[\d.]/i.test(ua)) return 'QQ'
  if (/Weibo/i.test(ua)) return '微博'
  if (/DingTalk/i.test(ua)) return '钉钉'
  return ''
})()
const CODE_LEN = 6
const MAX_FILES = 64 // 与服务端 maxFiles 对齐
const SKEY = 'drop-session'

const st = {
  role: null,            // null | 'send' | 'recv'
  resumed: false,        // 本次页面来自刷新恢复
  code: '', token: '',
  creating: false,       // 房间创建在途（首个种子落位前只此一次）
  files: new Map(),      // id → {id,name,size,ih,el,done,fail,file?,hashing?}
  order: [],             // 到达顺序 = 接收端展示/下载顺序
  torrents: new Map(),   // id → Torrent
  want: [],              // 接收端：点名要拿的文件 id
  queueRunning: false,
  dirHandle: null, usedNames: new Set(),
  curRecv: null,         // {id,got,stop}：进行中的那笔接收（看门狗/划走经 stop 中断它）
  addQueue: [], seq: 0,  // 发送端：自增 id
  createWaiters: [],     // 建房在途时排队等落位的增投文件
  lastHint: ['', ''],
  epoch: 0,              // 离房时自增，作废进行中的进房
  hadFiles: false,
  dismissed: new Set(),  // 本地划走的行，重进不再加回
}
window.__dr = st

let client = null
function engine() {
  if (client && client.destroyed) client = null // bfcache 复活/bye 之后别把尸体递出去
  if (!client) {
    client = new WebTorrent(wtOpts())
    window.__client = client // 调试观测位
  }
  return client
}

function magnetOf(ih, name) {
  return 'magnet:?xt=urn:btih:' + ih +
    '&dn=' + encodeURIComponent(name) +
    '&tr=' + encodeURIComponent(TRACKER)
}

/* ---------- 小工具 ---------- */

function hint(text, tone) {
  st.lastHint = [text, tone || '']
  const h = $('hint')
  if (!text) {
    h.hidden = true
    h.textContent = ''
    h.dataset.tone = ''
    return
  }
  h.hidden = false
  h.textContent = text
  h.dataset.tone = tone || 'err'
}

function errWord(status, msg) {
  const m = String(msg || '')
  if (status === 429 || /频繁|too many/i.test(m)) return '太频繁'
  if (status === 503 || status === 502 || status === 500 || /正忙|上限/.test(m)) return '正忙'
  if (status === 404 || status === 403 || /不存在|无效/.test(m)) return '无效'
  return '失败'
}

async function readJSON(res) {
  try { return await res.json() } catch { return {} }
}

function showFail(word) {
  flashZone('bad', 640)
  hint(word || '失败', 'err')
}

function failWord(e) {
  const m = (e && e.message) || ''
  if (m === '太频繁' || m === '正忙' || m === '无效' || m === '失败') return m
  return '失败'
}

let flashTimer = 0
let flashRaf = 0
function flashZone(cls, ms) {
  const z = $('codezone')
  z.classList.remove('copied', 'bad')
  void z.offsetWidth
  z.classList.add(cls)
  clearTimeout(flashTimer)
  if (flashRaf) cancelAnimationFrame(flashRaf)
  flashTimer = 0
  flashRaf = 0
  const off = () => { flashRaf = 0; flashTimer = 0; z.classList.remove(cls) }
  if (ms === 0) {
    flashRaf = requestAnimationFrame(() => { flashRaf = requestAnimationFrame(off) })
    return
  }
  flashTimer = setTimeout(off, ms || 480)
}

function setRole(role) {
  st.role = role
  document.documentElement.dataset.role = role || ''
  for (const f of st.files.values()) syncRowRole(f)
  updatePick()
}

// 行的读屏语义随角色同步（首批行进列时房间往往还没建，角色会变，不能钉死在进列那一刻）
function syncRowRole(f) {
  if (!f.el) return
  f.el.dataset.mine = isMine() ? '1' : '0'
  if (!f.done && !f.fail) {
    f.el.setAttribute('aria-label', f.name + '，' + human(f.size) + '，' + (isMine() ? '按 Delete 移除' : '按回车下载'))
  }
}

// 链路温度：任一活跃池有 peer 直连即「清亮」
function setLink(mode) {
  if (mode) document.documentElement.dataset.link = mode
  else delete document.documentElement.dataset.link
}

function updateLink() {
  for (const t of st.torrents.values()) {
    if (t.numPeers > 0) { setLink('p2p'); return }
  }
  setLink('')
}

// 桌面（键鼠）：文件是发丝线卡，拖动理序、拖出即弃；触屏保持裸行划走。
function isDesk() {
  return matchMedia('(hover: hover) and (pointer: fine)').matches
}

function rand2() {
  const abc = 'abcdefghjkmnpqrstuvwxyz23456789'
  return abc[Math.random() * abc.length | 0] + abc[Math.random() * abc.length | 0]
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

/* ---------- 会话存取（刷新恢复用） ---------- */

function saveSession() {
  if (!st.role || !st.code) { sessionStorage.removeItem(SKEY); return }
  const files = st.order.map(id => {
    const f = st.files.get(id)
    return f ? { id: f.id, name: f.name, path: f.path || '', size: f.size, ih: f.ih || '' } : null
  }).filter(Boolean)
  try {
    const dismissed = st.dismissed.size ? [...st.dismissed] : []
    sessionStorage.setItem(SKEY, JSON.stringify({ role: st.role, code: st.code, token: st.token, files, dismissed }))
  } catch { /* 隐身模式等存不进就算了 */ }
}

/* ---------- 发送端文件缓存（IndexedDB，刷新后可继续供文件） ---------- */

let idb = null
function idbOpen() {
  if (idb) return Promise.resolve(idb)
  return new Promise((res, rej) => {
    const rq = indexedDB.open('droprelay', 1)
    rq.onupgradeneeded = () => rq.result.createObjectStore('files')
    rq.onsuccess = () => { idb = rq.result; res(idb) }
    rq.onerror = () => rej(rq.error)
    rq.onblocked = () => rej(new Error('blocked'))
  })
}

async function idbPut(id, file) {
  try {
    const db = await idbOpen()
    await new Promise((res, rej) => {
      const tx = db.transaction('files', 'readwrite')
      tx.objectStore('files').put(file, id)
      tx.oncomplete = res
      tx.onerror = () => rej(tx.error)
      tx.onabort = () => rej(tx.error)
    })
    savedAt[id] = Date.now()
    saveStamps()
  } catch { /* 配额/隐私模式：放弃缓存，仅影响刷新后重传 */ }
}

// 存储一次性：drop 不做仓库，本机缓存（发送端文件本体）24h 没有下载活动就清。
// 「有人下载」= 名下种子有上传字节增长（接收端从 P2P 拿块也算数）。
// 房间服务端本就 6h 硬顶，24h 后即便刷新续上也只是死房自愈，块删了无感。
const DKEY = 'drop-savedat'
const AKEY = 'drop-lastact'
let savedAt = {}
let lastAct = {}
const upSeen = new Map() // id → 上次巡检时的 uploaded
try { savedAt = JSON.parse(localStorage.getItem(DKEY) || '{}') } catch { /* 坏数据当空 */ }
try { lastAct = JSON.parse(localStorage.getItem(AKEY) || '{}') } catch { /* 同上 */ }
function saveStamps() {
  try { localStorage.setItem(DKEY, JSON.stringify(savedAt)) } catch { /* 无所谓 */ }
  try { localStorage.setItem(AKEY, JSON.stringify(lastAct)) } catch { /* 无所谓 */ }
}
async function sweepTTL() {
  const now = Date.now()
  for (const [id, t] of st.torrents) {
    if (t.uploaded !== upSeen.get(id)) { upSeen.set(id, t.uploaded); lastAct[id] = now }
  }
  const cutoff = now - 24 * 3600 * 1000
  try {
    const db = await idbOpen()
    const rq = db.transaction('files', 'readwrite').objectStore('files').openKeyCursor()
    rq.onsuccess = () => {
      const c = rq.result
      if (!c) { saveStamps(); return }
      const last = Math.max(savedAt[c.key] || 0, lastAct[c.key] || 0)
      if (last && last < cutoff) {
        c.delete()
        delete savedAt[c.key]
        delete lastAct[c.key]
      }
      c.continue()
    }
  } catch { /* IDB 不可用就没有可清的 */ }
}

function idbGet(id) {
  return idbOpen().then(db => new Promise((res, rej) => {
    const rq = db.transaction('files').objectStore('files').get(id)
    rq.onsuccess = () => res(rq.result || null)
    rq.onerror = () => rej(rq.error)
  })).catch(() => null)
}

async function idbKeep(ids) { // 清掉房间里已不存在的缓存
  try {
    const db = await idbOpen()
    const rq = db.transaction('files', 'readwrite').objectStore('files').openKeyCursor()
    rq.onsuccess = () => {
      const c = rq.result
      if (!c) return
      if (!ids.includes(c.key)) c.delete()
      c.continue()
    }
  } catch { /* 无所谓 */ }
}

// 开页清扫：上一段会话在 OPFS 留下的块目录（fsa-chunk-store 的「名字 - ih前8位」形态）。
// 当前清单里的 ih 保留（接收端刷新续传靠它），其余都是死重，就地清；顺带清 in/ 中转残留。
// 跨标签：清扫要求独占 drop-active 锁——另一个 drop 标签在收/发（做种也把文件整份拷进
// OPFS），它的块目录可能在用，宁可漏扫也不能拆台。无锁 API 的环境（几乎等于非安全上下文，
// OPFS 本也不可用）退回旧行为。
let activeLockRelease = null
function holdActiveLock() {
  if (!navigator.locks || activeLockRelease) return
  navigator.locks.request('drop-active', { mode: 'shared' }, () => new Promise(res => { activeLockRelease = res }))
}
function releaseActiveLock() {
  if (activeLockRelease) { activeLockRelease(); activeLockRelease = null }
}

async function sweepOPFS() {
  if (!navigator.storage || !navigator.storage.getDirectory) return
  const run = async () => {
    try {
      const root = await navigator.storage.getDirectory()
      const names = []
      for await (const [name, h] of root.entries()) {
        if (h.kind === 'directory' && / - [0-9a-f]{8}$/.test(name)) names.push(name)
      }
      for (const name of names) {
        const ih8 = name.slice(-8)
        let live = false
        for (const f of st.files.values()) {
          if (f.ih && f.ih.slice(0, 8) === ih8) { live = true; break }
        }
        if (live) continue
        try { await root.removeEntry(name, { recursive: true }) } catch { /* 无所谓 */ }
      }
      // in/ 是 opfsSink 的中转区：能拿到独占锁说明没有别的标签在下，全清
      try {
        const dh = await root.getDirectoryHandle('in', { create: false })
        for await (const [name] of dh.entries()) {
          try { await dh.removeEntry(name) } catch { /* 无所谓 */ }
        }
      } catch { /* 没有 in/ */ }
    } catch { /* OPFS 不可用就算了 */ }
  }
  if (navigator.locks) {
    await navigator.locks.request('drop-active', { ifAvailable: true }, async lock => {
      if (lock) await run() // 拿不到 = 有标签在用：本轮不扫
    })
    return
  }
  await run()
}

/* ---------- 码区 ---------- */

const codeInput = $('codeInput')
let slotEls = []

function buildSlots() {
  const s = $('slots')
  for (let i = 0; i < CODE_LEN; i++) {
    const el = document.createElement('span')
    el.className = 'slot'
    const b = document.createElement('b')
    el.appendChild(b)
    s.appendChild(el)
    slotEls.push(el)
  }
}

let lastCodeChars = []
function renderSlots(str, animate) {
  const chars = [...str.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, CODE_LEN)]
  slotEls.forEach((el, i) => {
    const c = chars[i] || ''
    const prev = lastCodeChars[i] || ''
    const b = el.firstChild
    if (b.textContent !== c) b.textContent = c
    el.classList.toggle('on', !!c)
    if (c && (animate || c !== prev)) {
      el.classList.remove('pop')
      void el.offsetWidth
      el.classList.add('pop')
      b.style.animationDelay = animate ? (i * 45) + 'ms' : '0ms'
    }
  })
  lastCodeChars = chars
  setCursor(chars.length)
}

function setCursor(n) {
  slotEls.forEach((el, i) => el.classList.toggle('cur', i === n))
}

function normalizeCode(s) {
  return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '')
}

function extractCode(text) {
  if (!text) return ''
  const raw = String(text).trim()
  const found = []
  const consider = s => { if (s) found.push(s) }
  const tryURL = s => {
    try {
      const u = new URL(s)
      const seg = (u.pathname.split('/').filter(Boolean).pop()) || ''
      consider(seg)
      consider(u.searchParams.get('c'))
      consider(u.searchParams.get('code'))
      consider((u.hash || '').replace(/^#\/?/, ''))
      return true
    } catch { return false }
  }
  if (!tryURL(raw)) {
    const um = raw.match(/https?:\/\/[^\s<>"']+/i)
    if (um) tryURL(um[0])
    else {
      const bare = raw.match(/(?:[\w-]+\.)+[\w-]+\/[A-Za-z0-9]{6}\b/)
      if (bare) tryURL('https://' + bare[0])
    }
  }
  for (const cand of found) {
    const n = normalizeCode(cand)
    if (n.length === CODE_LEN) return n
  }
  const n = normalizeCode(raw)
  if (n.length <= CODE_LEN) return n
  return ''
}

function takeCode(text) {
  const ex = extractCode(text)
  if (ex) return ex
  const s = String(text || '')
  if (/https?:\/\//i.test(s) || /\.[a-z]{2,}\//i.test(s)) return ''
  return normalizeCode(s).slice(0, CODE_LEN)
}

function shareURL(code) {
  return location.origin + '/' + code
}

function codeFromLocation() {
  const path = (location.pathname || '/').replace(/^\//, '').split('/')[0]
  if (/^[A-Za-z0-9]{6}$/.test(path)) return path.toUpperCase()
  const q = params.get('c') || params.get('code')
  if (q && /^[A-Za-z0-9]{6}$/.test(q)) return q.toUpperCase()
  const hash = (location.hash || '').replace(/^#\/?/, '')
  if (/^[A-Za-z0-9]{6}$/.test(hash)) return hash.toUpperCase()
  return ''
}

function syncShareURL() {
  if (!st.code || st.role !== 'recv') return
  const want = '/' + st.code
  if (location.pathname !== want) history.replaceState(null, '', want + (location.search || ''))
}

function clearShareURL() {
  const rest = (location.pathname || '/').replace(/^\//, '').split('/')[0]
  if (/^[A-Za-z0-9]{6}$/.test(rest)) history.replaceState(null, '', '/' + (location.search || ''))
}

function lockCode(on) {
  const z = $('codezone')
  z.classList.toggle('locked', !!on)
  codeInput.disabled = !!on
  $('slots').dataset.focus = '0'
  if (on) {
    z.setAttribute('role', 'button')
    z.setAttribute('aria-label', '复制分享链接')
    z.tabIndex = 0
    syncShareURL()
  } else {
    z.setAttribute('role', 'group')
    z.setAttribute('aria-label', '传送码')
    z.tabIndex = -1
    clearShareURL()
  }
}

function copyShare() {
  if (!st.code) return
  const url = shareURL(st.code)

  const onOk = () => {
    flashZone('copied', 480)
    if (navigator.vibrate) navigator.vibrate(12)
    const cs = $('copyStatus')
    if (cs) {
      cs.textContent = '已复制分享链接'
      setTimeout(() => { if (cs.textContent === '已复制分享链接') cs.textContent = '' }, 2000)
    }
  }
  const onFail = () => {
    flashZone('bad', 400)
    const cs = $('copyStatus')
    if (cs) {
      cs.textContent = '复制失败'
      setTimeout(() => { if (cs.textContent === '复制失败') cs.textContent = '' }, 2000)
    }
  }

  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(url).then(onOk, () => copyShareFallback(url, onOk, onFail))
  } else {
    copyShareFallback(url, onOk, onFail)
  }
}

function copyShareFallback(url, onOk, onFail) {
  try {
    const ta = document.createElement('textarea')
    ta.value = url
    ta.setAttribute('readonly', '')
    ta.style.cssText = 'position:fixed;left:0;top:0;opacity:0;width:1px;height:1px'
    document.body.appendChild(ta)
    ta.select()
    ta.setSelectionRange(0, url.length)
    const done = document.execCommand('copy')
    ta.remove()
    if (done) onOk()
    else onFail()
  } catch { onFail() }
}

function applyTyped(raw) {
  const code = takeCode(raw)
  codeInput.value = code
  renderSlots(code, false)
  if (code.length === CODE_LEN && st.role === null) {
    codeInput.blur()
    joinRecv(code)
  }
}

codeInput.addEventListener('focus', () => { $('slots').dataset.focus = '1'; setCursor(takeCode(codeInput.value).length) })
codeInput.addEventListener('blur', () => { $('slots').dataset.focus = '0' })
codeInput.addEventListener('beforeinput', e => {
  if (e.isComposing || e.inputType === 'insertCompositionText') e.preventDefault()
})
codeInput.addEventListener('compositionstart', e => e.preventDefault())
codeInput.addEventListener('input', e => {
  if (e.isComposing) return
  applyTyped(codeInput.value)
})
codeInput.addEventListener('compositionend', () => applyTyped(codeInput.value))
codeInput.addEventListener('keydown', e => {
  if (e.key === 'Escape' && !st.role) {
    codeInput.value = ''
    renderSlots('', false)
    hint('')
  }
})

const cz = $('codezone')
cz.addEventListener('click', () => {
  if (st.code) copyShare()
  else if (st.role === null) codeInput.focus()
})
cz.addEventListener('keydown', e => {
  if (!st.code) return
  if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); copyShare() }
})

/* ---------- 房间 API（码 ↔ 清单） ---------- */

async function apiJSON(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  const j = await readJSON(res)
  return { ok: res.ok, status: res.status, j }
}

function refOf(f) {
  const r = { name: f.name, size: f.size, ih: f.ih }
  if (f.path) r.path = f.path
  return r
}

function flushCreateWaiters(ok) {
  const ws = st.createWaiters
  st.createWaiters = []
  for (const res of ws) res(ok)
}

// 返回 'ok' | 'full'（房满）| HTTP 状态码（404/403=死房易主，429/503=不硬来）；网络异常上抛
async function addToRoom(f) {
  const r = await apiJSON('POST', '/api/rooms/' + encodeURIComponent(st.code),
    { token: st.token, files: [refOf(f)] })
  if (r.ok) {
    if (!(r.j.files || []).some(x => x.ih === f.ih)) { hint('上限', 'warn'); return 'full' } // 房满被服务端静默丢弃
    return 'ok'
  }
  showFail(errWord(r.status, r.j.error))
  return r.status
}

// 划走的行同步删出服务端房间（清空即拆房）；发后即忘，失败不影响本地移除
function removeFromRoom(ih) {
  apiJSON('POST', '/api/rooms/' + encodeURIComponent(st.code) + '/remove', { token: st.token, ih }).catch(() => {})
}

// 首个种子落位：建房或续房。成功返回 true；失败已提示。
// 死房自愈：续房撞 404/403（房被收或 token 易主）就清旧码并入建房单飞队列重建，
// 其余并发增投经 createWaiters 排队等新房落位后补投（建房失败时排队者直接败退，
// 不再连环重试去撞建房限速）。
async function ensureRoom(f) {
  for (;;) {
    if (st.files.get(f.id) !== f) return true // 已被划走
    if (st.code) {
      const mine = st.code
      const how = await addToRoom(f)
      if (how === 'ok') {
        if (st.files.get(f.id) !== f) removeFromRoom(f.ih) // 等待期间被划走：把刚补投的撤回
        return true
      }
      if (how === 'full') return false
      if (st.code !== mine) continue // 已有同伴换到新房：重投
      if (how !== 404 && how !== 403) return false // 限速/网络问题，不硬来
      st.code = ''; st.token = '' // 死房/易主：清码，走下面的重建
      sessionStorage.removeItem(SKEY)
    }
    if (st.creating) {
      // 建房在途（整夹选入会并发做种到同一窗口）：排队等落位后补投
      const ok = await new Promise(res => st.createWaiters.push(res))
      if (!ok) return false
      continue
    }
    st.creating = true
    const epoch = st.epoch
    try {
      // token 随行：房还活着且凭证吻合时服务端原房续用，不至于无谓分家
      const r = await apiJSON('POST', '/api/rooms', { token: st.token || '', files: [refOf(f)] })
      if (epoch !== st.epoch) return false
      st.creating = false
      if (!r.ok) {
        flushCreateWaiters(false)
        showFail(errWord(r.status, r.j.error))
        return false
      }
      flushCreateWaiters(true)
      st.code = r.j.code
      st.token = r.j.token
      setRole('send')
      renderSlots(st.code, true)
      lockCode(true)
      hint('')
      saveSession()
      return true
    } catch {
      st.creating = false
      flushCreateWaiters(false)
      if (epoch === st.epoch) showFail('网络错误')
      return false
    }
  }
}

/* ---------- 文件列表渲染 ---------- */

function isMine() {
  return st.role === 'send' // 接收端的行都是别人的
}

function addRow(f) {
  const el = document.createElement('div')
  el.className = 'row'
  el.dataset.id = f.id
  el.innerHTML = '<div class="row-fill"></div><span class="row-name"></span><span class="row-side"></span>'
  el.querySelector('.row-name').textContent = f.name
  el.querySelector('.row-side').textContent = human(f.size)
  // 角色在事件时判定：绑定时 role 常还是空（首批行先于建房进列），行为不能定死
  el.tabIndex = 0
  el.setAttribute('role', 'button')
  syncRowRole(f)
  el.addEventListener('keydown', e => {
    if (e.key === 'Enter' || e.key === ' ') {
      if (isMine()) return
      e.preventDefault()
      if (el._tossed || el._dragged || f.done || f.fail) return
      requestDownload([f.id])
    } else if (e.key === 'Delete' || e.key === 'Backspace') {
      if (!isMine()) return
      e.preventDefault()
      if (el._tossed || el._dragged) return
      el._tossed = true
      tossFile(f.id, { dir: 1 })
    }
  })
  el.addEventListener('click', () => {
    if (isMine()) return // 发送端的行不给点下载（下载给自己是接收端动作）
    if (el._tossed || el._dragged || f.done || f.fail) return
    requestDownload([f.id])
  })
  if (isDesk()) {
    bindDeckDrag(el, f)
  } else {
    bindToss(el, f)
  }
  $('rows').appendChild(el)
  f.el = el
  st.hadFiles = true
  $('list').hidden = false
}

function bindToss(el, f) {
  el.addEventListener('pointerdown', e => {
    if (!e.isPrimary || e.button !== 0) return
    const x0 = e.clientX, y0 = e.clientY, t0 = performance.now()
    let dx = 0, axis = '', live = false
    const move = ev => {
      if (ev.pointerId !== e.pointerId) return
      const x = ev.clientX - x0, y = ev.clientY - y0
      if (!axis) {
        if (Math.hypot(x, y) < 10) return
        axis = Math.abs(x) > Math.abs(y) * 1.2 ? 'x' : 'y'
        if (axis === 'x') {
          live = true
          el.classList.add('tugging')
          try { el.setPointerCapture(e.pointerId) } catch { /* 老环境 */ }
        }
      }
      if (axis !== 'x') return
      ev.preventDefault()
      dx = x
      el.style.transform = 'translateX(' + dx + 'px)'
      el.style.opacity = String(Math.max(.25, 1 - Math.abs(dx) / Math.max(120, el.offsetWidth * .85)))
    }
    const up = ev => {
      if (ev.pointerId !== e.pointerId) return
      el.removeEventListener('pointermove', move)
      el.removeEventListener('pointerup', up)
      el.removeEventListener('pointercancel', up)
      el.classList.remove('tugging')
      const dt = Math.max(1, performance.now() - t0)
      const th = Math.min(88, el.offsetWidth * .28)
      if (live && (Math.abs(dx) > th || (Math.abs(dx / dt) > .65 && Math.abs(dx) > 24))) {
        el._tossed = true
        tossFile(f.id, { dir: dx < 0 ? -1 : 1 })
      } else if (live) {
        el.style.transition = 'transform .22s cubic-bezier(.2,.7,.3,1), opacity .22s cubic-bezier(.2,.7,.3,1)'
        el.style.transform = ''
        el.style.opacity = ''
        const clr = () => { el.style.transition = ''; el.removeEventListener('transitionend', clr) }
        el.addEventListener('transitionend', clr)
      }
    }
    el.addEventListener('pointermove', move)
    el.addEventListener('pointerup', up)
    el.addEventListener('pointercancel', up)
  })
}

/* ---------- 桌面卡桌：每文件一枚发丝线卡，拖动理序、拖出桌面即移出 ---------- */

function bindDeckDrag(el, f) {
  el.addEventListener('dragstart', e => e.preventDefault())
  el.addEventListener('pointerdown', e => {
    if (!e.isPrimary || e.button !== 0) return
    el._dragged = false
    const x0 = e.clientX, y0 = e.clientY
    let live = false, outside = false, clone = null

    const move = ev => {
      if (ev.pointerId !== e.pointerId) return
      const dx = ev.clientX - x0, dy = ev.clientY - y0
      if (!live) {
        if (Math.hypot(dx, dy) < 6) return
        live = true
        el._dragged = true
        const r = el.getBoundingClientRect()
        clone = el.cloneNode(true)
        clone.className = 'row ghost'
        clone.removeAttribute('data-id')
        clone.style.cssText = 'position:fixed;z-index:99;margin:0;pointer-events:none;' +
          'left:' + r.left + 'px;top:' + r.top + 'px;width:' + r.width + 'px;height:' + r.height + 'px'
        document.body.appendChild(clone)
        el.classList.add('well')
        document.body.classList.add('deck-dragging')
      }
      ev.preventDefault()
      clone.style.transform = 'translate(' + dx + 'px,' + dy + 'px)'
      const lr = $('list').getBoundingClientRect()
      const out = ev.clientX < lr.left - 24 || ev.clientX > lr.right + 24 ||
        ev.clientY < lr.top - 24 || ev.clientY > lr.bottom + 24
      if (out !== outside) {
        outside = out
        clone.classList.toggle('bye', outside)
      }
      if (!outside && el.parentNode) {
        let next = null
        for (const s of el.parentNode.children) {
          if (s === el) continue
          const r = s.getBoundingClientRect()
          if (ev.clientY < r.top + r.height / 2) { next = s; break }
        }
        if (next !== el.nextSibling) {
          el.parentNode.insertBefore(el, next)
        }
      }
    }

    const cleanUp = () => {
      window.removeEventListener('pointermove', move)
      window.removeEventListener('pointerup', up)
      window.removeEventListener('pointercancel', up)
      document.body.classList.remove('deck-dragging')
      el.classList.remove('well')
      if (clone) {
        clone.remove()
        clone = null
      }
    }

    const up = ev => {
      if (ev.pointerId !== e.pointerId) return
      cleanUp()
      if (!live) return
      if (ev.type === 'pointercancel') {
        setTimeout(() => { el._dragged = false }, 50)
        return
      }
      if (outside) {
        el._tossed = true
        tossFile(f.id, { instant: true })
        return
      }
      setTimeout(() => { el._dragged = false }, 50)
    }

    window.addEventListener('pointermove', move)
    window.addEventListener('pointerup', up)
    window.addEventListener('pointercancel', up)
  })
}

function tossFile(id, opts) {
  opts = opts || {}
  const f = st.files.get(id)
  if (!f || f.tossing) return
  f.tossing = true
  if (st.curRecv && st.curRecv.id === id) {
    const c = st.curRecv
    st.curRecv = null
    c.stop(new Error('取消'))
  }
  st.want = st.want.filter(x => x !== id)
  st.dismissed.add(id)
  st.files.delete(id)
  st.order = st.order.filter(x => x !== id)
  saveSession()
  if (st.role === 'send' && st.code && st.token && f.ih) {
    removeFromRoom(f.ih) // 同步删出服务端房间，新接收端不再看到幽灵行
  }
  const t = st.torrents.get(id)
  if (t) {
    st.torrents.delete(id)
    try { engine().remove(t, { destroyStore: true }, () => {}) } catch { /* 已毁 */ }
  }
  if (st.role === 'send') idbKeep([...st.files.keys()])
  updateLink()
  updateAllBtn()
  const empty = st.files.size === 0 && st.hadFiles
  const el = f.el
  const finish = () => {
    if (el && el.parentNode) el.remove()
    pruneDirs()
    if (empty && st.role === 'send') leaveSession()
    else if (st.files.size === 0) {
      $('list').hidden = true
      $('doneArea').hidden = true
    } else if (![...st.files.values()].some(x => x.done && x.el && x.el.parentNode === $('doneRows'))) {
      $('doneArea').hidden = true
    }
  }
  const reduce = matchMedia('(prefers-reduced-motion: reduce)').matches
  if (!el || opts.instant || reduce) { finish(); return }
  const dir = opts.dir || 1
  el.classList.add('off')
  el.style.transform = 'translateX(' + (dir * (el.offsetWidth + 48)) + 'px)'
  el.style.opacity = '0'
  let done = false
  const once = () => { if (done) return; done = true; finish() }
  el.addEventListener('transitionend', once)
  setTimeout(once, 320)
}

function sideOf(f, text, cls) {
  if (!f.el) return
  const s = f.el.querySelector('.row-side')
  s.textContent = text
  s.className = 'row-side' + (cls ? ' ' + cls : '')
}

function setRowProgress(id, bytes, total) {
  const f = st.files.get(id)
  if (!f || !f.el) return
  const pct = total > 0 ? Math.min(1, bytes / total) : 0
  const fill = f.el.querySelector('.row-fill')
  fill.style.animation = 'none'
  fill.style.transform = `scaleX(${pct.toFixed(4)})`
}

function rowComplete(id) {
  const f = st.files.get(id)
  if (!f || !f.el) return
  const fill = f.el.querySelector('.row-fill')
  fill.style.animation = ''
  fill.style.transform = 'scaleX(0)'
  sideOf(f, human(f.size), '')
  f.el.classList.remove('busy')
  f.el.classList.add('fin')
  f.el.removeAttribute('role')
  f.el.removeAttribute('aria-label')
  f.el.removeAttribute('tabindex')
  f.el.style.animation = 'none'
  void f.el.offsetWidth
  f.el.style.animation = ''
  f.el.remove()
  $('doneArea').hidden = false
  $('doneRows').appendChild(f.el)
  pruneDirs()
  saveSession()
}

function rowFail(id, retrying) {
  const f = st.files.get(id)
  if (!f) return
  if (f.el) {
    const fill = f.el.querySelector('.row-fill')
    fill.style.animation = ''
    fill.style.transform = 'scaleX(0)'
    f.el.classList.remove('busy')
    if (!retrying) {
      f.el.classList.add('err')
      f.el.removeAttribute('role')
      f.el.removeAttribute('aria-label')
      f.el.removeAttribute('tabindex')
    }
    sideOf(f, retrying ? '重试' : human(f.size), retrying ? 'warn' : '')
  }
  if (retrying) {
    setTimeout(() => {
      if (st.files.get(id) === f && !f.done && !f.fail && f.el) {
        f.el.classList.remove('err')
        f.el.classList.add('busy')
        sideOf(f, human(f.size), '')
      }
    }, 1400)
  }
}

function dirLabel(p) {
  const i = (p || '').lastIndexOf('/')
  return i < 0 ? '' : p.slice(0, i)
}

function renderFiles(list) {
  let lastDir = null
  for (const ref of list) {
    if (st.files.has(ref.id) || st.dismissed.has(ref.id)) continue
    const rec = { id: ref.id, name: ref.name, path: ref.path || '', size: ref.size, ih: ref.ih || '', el: null, done: false, fail: false }
    // 接收端按目录分组（与 share 站同款）：层级变了就插一枚目录签，根层为「·」
    if (!isMine()) {
      const d = dirLabel(rec.path)
      if (d !== lastDir) {
        lastDir = d
        const h = document.createElement('div')
        h.className = 'dir'
        h.textContent = d || '·'
        $('rows').appendChild(h)
      }
    }
    st.files.set(rec.id, rec)
    st.order.push(rec.id)
    addRow(rec)
  }
  updateAllBtn()
}

/* ---------- 发送端：逐文件做种 ---------- */

async function addFiles(list) {
  if (IAB) { externalGuide(); return } // IAB 不做种：入口藏掉，漏进来的（拖拽/粘贴）也劝去外部
  const files = [...list].filter(f => f)
  if (!files.length) return
  if (st.role === 'recv') leaveSession() // 接收态拖入=换轨发送：先整场离房，别让 recv 码与建房状态机绞在一起
  holdActiveLock() // 发送意图成立即持锁：做种的 OPFS 块目录别被别的标签清扫
  for (const f of files) st.addQueue.push(f)
  flushAddQueue()
}

function flushAddQueue() {
  while (st.addQueue.length) {
    if (st.files.size >= MAX_FILES) { hint('上限', 'warn'); st.addQueue = []; break }
    const file = st.addQueue.shift()
    const id = 'f' + (++st.seq) + rand2()
    const rec = { id, name: file.name, path: file.__drPath || file.webkitRelativePath || '', size: file.size, ih: '', el: null, done: false, fail: false, file, hashing: true }
    st.files.set(id, rec)
    st.order.push(id)
    addRow(rec)
    idbPut(id, file)
    seedOne(rec)
  }
}

function watchTorrent(t, id) {
  t.on('wire', w => { updateLink(); w.once('close', updateLink) }) // wire 断开也要收敛 p2p 标记
  t.on('close', () => { st.torrents.delete(id); updateLink() })
  t.on('error', () => {
    const f = st.files.get(id)
    if (f && !f.done) { f.fail = true; rowFail(id, false) } // 种子级错误是终态：置败，别留个能点却没底的行
  })
}

async function seedOne(rec) {
  // 落位中：雾从左往右爬（create-torrent 的 onProgress 给 passed/total 字节数）
  const opts = {
    announce: [TRACKER],
    pieceLength: pieceLenOf(rec.size),
    onProgress: passed => setRowProgress(rec.id, passed, rec.size),
    ...swarmOpts(),
  }
  try {
    const t = await new Promise((res, rej) => {
      const tor = engine().seed(rec.file, opts, tor2 => res(tor2))
      tor.on('error', rej)
    })
    if (st.files.get(rec.id) !== rec) { // 已被划走
      try { engine().remove(t, { destroyStore: true }, () => {}) } catch { /* 已毁 */ }
      return
    }
    rec.hashing = false
    rec.ih = t.infoHash
    setRowProgress(rec.id, 0, rec.size)
    sideOf(rec, human(rec.size), '')
    watchTorrent(t, rec.id)
    st.torrents.set(rec.id, t)
    const ok = await ensureRoom(rec)
    if (!ok) rowFail(rec.id, false)
    else saveSession()
  } catch (e) {
    // 同内容第二份撞 duplicate：复用已在池的那只种子，行照常走完进房流程
    const dup = /duplicate/i.test(e && e.message || '') && /([0-9a-f]{40})/.exec(e && e.message || '')
    const t = dup ? engine().torrents.find(x => x.infoHash === dup[1]) : null
    if (t && st.files.get(rec.id) === rec) {
      rec.hashing = false
      rec.ih = t.infoHash
      sideOf(rec, human(rec.size), '')
      watchTorrent(t, rec.id)
      st.torrents.set(rec.id, t)
      const ok = await ensureRoom(rec)
      if (!ok) rowFail(rec.id, false)
      else saveSession()
      return
    }
    if (st.files.get(rec.id) === rec) {
      rec.hashing = false
      rowFail(rec.id, false)
      hint('失败', 'err')
    }
  }
}

/* ---------- 接收端 ---------- */

async function peekCode(code) {
  const r = await apiJSON('GET', '/api/code?c=' + encodeURIComponent(code))
  return { ok: r.ok, word: errWord(r.status, r.j.error), files: r.j.files || [] }
}

let joinInflight = false
async function joinRecv(raw) {
  if (st.role || joinInflight) return
  const code = takeCode(raw)
  if (code.length !== CODE_LEN) return
  joinInflight = true
  const epoch = st.epoch
  try {
    const p = await peekCode(code)
    if (epoch !== st.epoch) return
    if (!p.ok) {
      showFail(p.word)
      if (codeFromLocation() === code) clearShareURL()
      return
    }
    setRole('recv')
    st.code = code
    lockCode(true)
    hint('')
    saveSession()
    renderFiles(p.files.map(ref => ({ id: ref.ih, name: ref.name, path: ref.path || '', size: ref.size, ih: ref.ih })))
    await sweepOPFS()
    holdActiveLock() // 清扫完再持锁：块目录从此受跨标签保护
    for (const id of st.order) joinSwarm(st.files.get(id))
  } catch {
    if (epoch === st.epoch) showFail('网络错误')
  } finally {
    joinInflight = false // 无条件复位：epoch 变过（换轨/离房）也不许把后续进房永久堵死
  }
}

// 加池但不选块：点了才下（file.select 由 recvFile 触发）
function joinSwarm(f) {
  if (!f || !f.ih || st.torrents.has(f.id)) return
  try {
    engine().add(magnetOf(f.ih, f.name), { announce: [TRACKER], bitfield: emptyBitfield(f.size), deselect: true, ...swarmOpts() }, t => {
      if (st.files.get(f.id) !== f) {
        try { engine().remove(t, { destroyStore: true }, () => {}) } catch { /* 已毁 */ }
        return
      }
      st.torrents.set(f.id, t)
      watchTorrent(t, f.id)
      try { t.files[0] && t.files[0].deselect() } catch { /* 已选 */ }
      updateLink()
    })
  } catch { rowFail(f.id, false) }
}

function pendingIds() {
  return st.order.filter(id => { const f = st.files.get(id); return f && !f.done && !f.fail && !isMine() })
}

function updateAllBtn() {
  const b = $('btnAll')
  if (!st.role) { b.hidden = true; b.disabled = false; return }
  const rest = pendingIds().filter(id => !st.want.includes(id) && st.curRecv?.id !== id)
  if (st.queueRunning && rest.length === 0) {
    b.hidden = false
    b.disabled = true
    return
  }
  b.disabled = false
  b.hidden = rest.length === 0
}

function requestDownload(ids) {
  if (!st.role) return
  if (IAB) { externalGuide(); return }
  for (const id of ids) {
    const f = st.files.get(id)
    if (!f || f.done || f.fail) continue
    if (!st.want.includes(id)) st.want.push(id)
    if (f.el) f.el.classList.add('busy')
  }
  updateAllBtn()
  pumpDownloads()
}

async function pumpDownloads() {
  if (IAB) return // 双保险：IAB 里任何漏网路径都别启动传输
  if (st.queueRunning || !st.role) return
  if (!st.want.length) return
  st.queueRunning = true
  updateAllBtn()

  if (!TESTDUMP && !st.dirHandle) {
    const ids = st.want.slice()
    try {
      if (canDirPicker() && ids.length > 1) {
        st.dirHandle = await window.showDirectoryPicker({ mode: 'readwrite' })
      } else if (canSavePicker() && ids.length === 1) {
        const f = st.files.get(ids[0])
        if (f && !f.saveHandle) {
          f.saveHandle = await window.showSaveFilePicker({ suggestedName: f.name || 'file' })
        }
      }
    } catch (e) {
      st.queueRunning = false
      st.want = []
      for (const rec of st.files.values()) {
        if (rec.el && !rec.done && !rec.fail) rec.el.classList.remove('busy')
      }
      if (!(e && e.name === 'AbortError')) hint('失败', 'err')
      updateAllBtn()
      return
    }
  }

  hint('')
  let fails = 0
  while (st.want.length) {
    if (!st.role) break
    const id = st.want.shift()
    const f = st.files.get(id)
    if (!f || f.done || f.fail) continue
    try {
      await recvFile(f)
      f.done = true
      f.tries = 0
      rowComplete(f.id)
      fails = 0
    } catch (e) {
      if (e && e.message === '取消') continue // 划走等主动取消：不算败、不重试
      fails++
      f.tries = (f.tries || 0) + 1
      if (f.tries >= 3) {
        f.fail = true
        rowFail(f.id, false)
        hint(failWord(e), 'err')
      } else {
        rowFail(f.id, true)
        st.want.push(id)
        await sleep(1600)
      }
      if (fails >= 6) { st.want = []; hint(failWord(e), 'err'); break }
    }
    updateAllBtn()
  }
  st.queueRunning = false
  if (!fails) hint('')
  updateAllBtn()
}

async function recvFile(f) {
  const t = await waitForTorrent(f)
  const file = t.files && t.files[0]
  if (!file) throw new Error('失败')

  const sink = await makeSink(f)
  if (st.files.get(f.id) !== f) { // makeSink 期间被划走：别把已划走的行再落盘
    if (sink.abort) { try { await sink.abort() } catch { /* 无所谓 */ } }
    throw new Error('取消')
  }
  const stopCtl = makeStop()
  const cur = { id: f.id, got: 0, stop: stopCtl.stop }
  st.curRecv = cur
  const reader = file.stream().getReader()

  let watchdog = 0
  const kick = () => {
    clearTimeout(watchdog)
    watchdog = setTimeout(() => { if (st.curRecv === cur) { st.curRecv = null; cur.stop(new Error('超时无数据')) } }, 60000)
  }
  const done = () => clearTimeout(watchdog)

  try {
    file.select()
    kick()
    // 看门狗/划走行都经 stop 中断：race 让挂起的 read 立刻落败，收尾交给 catch；
    // 每块 kick 一次——看门狗盯的是「停滞 60s」，不是整段传输时长
    await drainReader(reader, sink, stopCtl.signal, value => {
      kick()
      cur.got += value.byteLength
      setRowProgress(f.id, cur.got, f.size)
    })
  } catch (e) {
    done()
    if (st.curRecv === cur) st.curRecv = null
    try { reader.cancel() } catch { /* 已断 */ }
    try { file.deselect() } catch { /* 无所谓 */ }
    if (sink.abort) { try { await sink.abort() } catch { /* 无所谓 */ } }
    throw e
  }
  st.curRecv = null
  done()
  await sink.close()
  if (cur.got !== f.size) throw new Error('大小不符（' + human(cur.got) + ' / ' + human(f.size) + '）')
}

// 元数据未到（发送端刚建房/掉线）就等：tracker 建连 + ut_metadata 通常秒级
function waitForTorrent(f) {
  const t = st.torrents.get(f.id)
  if (t && t.files && t.files.length) return Promise.resolve(t)
  return new Promise((res, rej) => {
    const t0 = Date.now()
    const timer = setInterval(() => {
      const cur = st.torrents.get(f.id)
      if (cur && cur.files && cur.files.length) {
        clearInterval(timer)
        res(cur)
      } else if (!st.files.has(f.id)) {
        clearInterval(timer)
        rej(new Error('取消'))
      } else if (Date.now() - t0 > 30000) {
        clearInterval(timer)
        rej(new Error('超时无数据'))
      }
    }, 150)
  })
}

async function makeSink(f) {
  if (TESTDUMP) return testSink(f.id, f.name)
  if (f.saveHandle) {
    const w = await f.saveHandle.createWritable()
    return fsSink(w)
  }
  if (st.dirHandle) {
    const name = f.savedName || uniqName(f.name) // 重试复用首次占的名字：别一次重试留一个截断副本
    f.savedName = name
    const fh = await st.dirHandle.getFileHandle(name, { create: true })
    const w = await fh.createWritable()
    return fsSink(w)
  }
  const opfs = await opfsSink(f.id || ('t' + Date.now()), f.name)
  if (opfs) return opfs
  return memSink(f.name)
}

function uniqName(name) {
  if (!st.usedNames.has(name)) { st.usedNames.add(name); return name }
  const dot = name.lastIndexOf('.')
  const base = dot > 0 ? name.slice(0, dot) : name
  const ext = dot > 0 ? name.slice(dot) : ''
  for (let i = 2; ; i++) {
    const cand = `${base} (${i})${ext}`
    if (!st.usedNames.has(cand)) { st.usedNames.add(cand); return cand }
  }
}

$('btnAll').addEventListener('click', () => requestDownload(pendingIds()))

/* ---------- 离房 / 重置 ---------- */

function resetRecv() {
  sessionStorage.removeItem(SKEY)
  releaseActiveLock()
  setRole(null)
  st.resumed = false
  st.code = ''
  st.want = []
  st.queueRunning = false
  st.dismissed = new Set()
  setLink('')
  lockCode(false)
  codeInput.value = ''
  renderSlots('', false)
  updateAllBtn()
}

function destroyTorrents() {
  for (const [id, t] of [...st.torrents]) {
    st.torrents.delete(id)
    try { engine().remove(t, { destroyStore: true }, () => {}) } catch { /* 已毁 */ }
  }
  updateLink()
}

function leaveSession() {
  st.epoch++
  st.creating = false
  flushCreateWaiters(false)
  releaseActiveLock()
  if (st.curRecv) {
    const c = st.curRecv
    st.curRecv = null
    c.stop(new Error('取消'))
  }
  destroyTorrents()
  idbKeep([])
  sessionStorage.removeItem(SKEY)
  setRole(null)
  st.resumed = false
  st.code = ''
  st.token = ''
  st.want = []
  st.queueRunning = false
  st.files.clear()
  st.order = []
  st.addQueue = []
  st.seq = 0
  st.hadFiles = false
  st.dismissed = new Set()
  setLink('')
  $('rows').innerHTML = ''
  $('doneRows').innerHTML = ''
  $('list').hidden = true
  $('doneArea').hidden = true
  lockCode(false)
  codeInput.value = ''
  renderSlots('', false)
  updateAllBtn()
  hint('')
  if (matchMedia('(pointer: fine)').matches) codeInput.focus()
}

/* ---------- 全页拖放 / 粘贴 / 触屏选择 ---------- */

let dragDepth = 0
let dragSavedHint = null

window.addEventListener('dragover', e => {
  e.preventDefault()
  if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy'
})
window.addEventListener('dragenter', e => {
  e.preventDefault()
  if (e.dataTransfer) {
    e.dataTransfer.dropEffect = 'copy'
    if (e.dataTransfer.types && [...e.dataTransfer.types].includes('Files')) {
      dragDepth++
      document.body.classList.add('dragging')
      if (!dragSavedHint) dragSavedHint = st.lastHint
    }
  }
})
window.addEventListener('dragleave', e => {
  e.preventDefault()
  dragDepth--
  if (dragDepth <= 0 || e.clientX <= 0 || e.clientY <= 0 || e.clientX >= window.innerWidth || e.clientY >= window.innerHeight) {
    endDrag()
  }
})
window.addEventListener('dragend', () => endDrag())
window.addEventListener('mouseleave', () => {
  if (dragDepth > 0) endDrag()
})

function endDrag() {
  dragDepth = 0
  document.body.classList.remove('dragging')
  if (dragSavedHint) {
    hint(dragSavedHint[0], dragSavedHint[1])
    dragSavedHint = null
  }
}
window.addEventListener('paste', e => {
  if (e.clipboardData && e.clipboardData.files && e.clipboardData.files.length) {
    e.preventDefault()
    addFiles(e.clipboardData.files)
    return
  }
  if (st.role !== null) return
  const text = e.clipboardData && e.clipboardData.getData('text')
  if (!text) return
  const extracted = extractCode(text)
  const compact = String(text).toUpperCase().replace(/\s+/g, '')
  const looksLink = /https?:\/\//i.test(text) || /(?:[\w-]+\.)+[\w-]+\/[A-Za-z0-9]{6}/.test(text)
  const looksCode = /^[A-Z0-9]{6}$/.test(compact)
  if (extracted.length === CODE_LEN && (looksLink || looksCode)) {
    e.preventDefault()
    applyTyped(extracted)
  }
})

function updatePick() {
  $('btnPick').hidden = st.role === 'recv'
}

// 行被划走后收尾：清掉已无内容的目录签（只接收端有）
function pruneDirs() {
  for (const h of [...document.querySelectorAll('#rows .dir')]) {
    const nx = h.nextElementSibling
    if (!nx || nx.classList.contains('dir')) h.remove()
  }
}

$('btnPick').addEventListener('click', () => $('fileInput').click())
$('fileInput').addEventListener('change', () => {
  addFiles($('fileInput').files)
  $('fileInput').value = ''
})

/* ---------- 文件夹上传：拖文件夹、右键/长按「+」整夹选入 ---------- */

// 文件夹里来的是「带目录标签的文件」：拖拽遍历时挂 __drPath（含文件名的相对路径），
// 选夹时读原生 webkitRelativePath；无层级者两者皆空，走原路。
function dirInput() { return $('dirInput') }

$('btnPick').addEventListener('contextmenu', e => {
  if (st.role === 'recv') return
  e.preventDefault()
  dirInput().click()
})
let pickTaken = false
let pickLong = 0
$('btnPick').addEventListener('pointerdown', e => {
  if (!e.isPrimary || e.button !== 0 || st.role === 'recv') return
  pickLong = setTimeout(() => {
    pickLong = 0
    pickTaken = true
    if (navigator.vibrate) navigator.vibrate(12)
    dirInput().click()
  }, 550)
})
for (const ev of ['pointerup', 'pointercancel', 'pointerleave']) {
  $('btnPick').addEventListener(ev, () => clearTimeout(pickLong))
}
$('btnPick').addEventListener('click', () => {
  if (pickTaken) { pickTaken = false; return }
  $('fileInput').click()
})
dirInput().addEventListener('change', () => {
  addFiles(dirInput().files)
  dirInput().value = ''
})

// 递归展开拖入项：普通文件 path 即名字，文件夹逐层下钻（readEntries 每批至多百枚）。
async function walkEntry(entry, base, out) {
  if (!entry) return
  if (entry.isFile) {
    const f = await new Promise(res => entry.file(res, () => res(null)))
    if (f) {
      f.__drPath = base + entry.name
      out.push(f)
    }
  } else if (entry.isDirectory) {
    const reader = entry.createReader()
    for (;;) {
      const batch = await new Promise(res => reader.readEntries(res, () => res([])))
      if (!batch.length) break
      for (const c of batch) await walkEntry(c, base + entry.name + '/', out)
    }
  }
}

window.addEventListener('drop', e => {
  e.preventDefault()
  endDrag()
  const dt = e.dataTransfer
  if (!dt) return
  const plain = dt.files && dt.files.length ? [...dt.files] : []
  const entries = dt.items && dt.items.length
    ? [...dt.items].map(it => (it.webkitGetAsEntry && it.webkitGetAsEntry())) // 必须同步取，事件返回即失效
    : []
  if (!entries.some(Boolean)) {
    if (plain.length) addFiles(plain)
    return
  }
  const out = []
  Promise.all(entries.map(en => walkEntry(en, '', out))).then(() => {
    if (out.length) addFiles(out)
  })
})

/* ---------- 启动 ---------- */

buildSlots()
renderSlots('', false)
updatePick()
if (matchMedia('(pointer: fine)').matches && document.activeElement === document.body) codeInput.focus()

function resumeSession(s) {
  st.resumed = true
  setRole(s.role)
  st.code = s.code
  st.token = s.token || ''
  st.dismissed = new Set(s.dismissed || []) // 划走过的行，刷新后不加回
  renderSlots(st.code, true)
  lockCode(true)
  if (s.role === 'send') {
    // 原房续种：清单先行入列（ih 已知），先扫后种（免得清扫枚举撞上正在建的新块目录），
    // 文件本体从 IndexedDB 捞回来再补种
    renderFiles(s.files || [])
    ;(async () => {
      await sweepOPFS()
      holdActiveLock()
      for (const ref of (s.files || [])) {
        const rec = st.files.get(ref.id)
        if (!rec) continue
        const file = await idbGet(ref.id)
        if (!file) { rowFail(rec.id, false); continue }
        rec.file = file
        rec.hashing = true
        seedOne(rec)
      }
    })()
  } else {
    idbKeep([]) // 接收端不用文件缓存，顺手清掉可能残留的发送端遗留
    ;(async () => {
      try {
        const p = await peekCode(st.code)
        if (p.ok) {
          renderFiles(p.files.map(ref => ({ id: ref.ih, name: ref.name, path: ref.path || '', size: ref.size, ih: ref.ih })))
          await sweepOPFS()
          holdActiveLock()
          for (const id of st.order) joinSwarm(st.files.get(id))
        } else if (p.word === '无效') { // 确认死房才弃会话
          hint('已过期', 'err')
          resetRecv()
        } else { // 太频繁等瞬态：保会话，刷新可重试
          hint(p.word || '失败', 'err')
        }
      } catch { hint('网络错误', 'err') }
    })()
  }
}

;(async () => {
  let s = null
  try { s = JSON.parse(sessionStorage.getItem(SKEY) || 'null') } catch { /* 坏数据就当没有 */ }
  const urlCode = codeFromLocation()
  if (s && s.code && (s.role === 'send' || s.role === 'recv')) {
    if (s.role === 'send') {
      // 先探房：只有确认死房（无效=404）才清码重建，让首枚种子以全新建房起步——
      // 否则逐文件 addToRoom 全撞 404/探测限速（文件还在 IndexedDB，重建不丢数据）。
      // 网络异常/限速不当死房：保留原码，交给 ensureRoom 的增投路径自己判。
      let p = null
      try { p = await peekCode(s.code) } catch { /* 网络异常：当房还在 */ }
      if (p && !p.ok && p.word === '无效') { sessionStorage.removeItem(SKEY); s = { ...s, code: '', token: '' } }
      resumeSession(s)
      return
    }
    try {
      const p = await peekCode(s.code)
      if (p.ok) { resumeSession(s); return }
    } catch { /* 当房间没了 */ }
    sessionStorage.removeItem(SKEY)
    if (urlCode === s.code) { clearShareURL(); return }
  }
  idbKeep([]) // 非发送恢复：上一段会话的文件缓存已是死重
  if (urlCode) {
    codeInput.value = urlCode
    renderSlots(urlCode, true)
    joinRecv(urlCode)
  } else {
    sweepOPFS() // 空页：把上段会话的块目录清掉
  }
})()

sweepTTL() // 开页先清一轮超时缓存
setInterval(sweepTTL, 30 * 60 * 1000) // 打开期间的 24h TTL 巡检

/* ---------- 内置浏览器引导 ---------- */

const IS_IOS = /iPhone|iPad|iPod/i.test(navigator.userAgent)

// 安卓尽力拉起系统浏览器（intent 不指定包名 = 弹浏览器选择器；X5 内核可能静默吞掉，
// 此时横幅的「复制链接」仍是出路）；iOS 没有公开拉起接口，只能引导右上角菜单。
// 只试一次：真导航走了页面交给浏览器，被吞了也别反复骚扰
let intentTried = false
function openExternal() {
  if (intentTried || !/Android/i.test(navigator.userAgent)) return false
  intentTried = true
  location.href = 'intent://' + location.host + location.pathname +
    (location.search || '') + '#Intent;scheme=https;action=android.intent.action.VIEW;end'
  return true
}

function externalGuide() {
  guideBrowser(true)
  openExternal()
  hint(IS_IOS ? '点右上角「···」在浏览器中打开' : '请在系统浏览器中打开', 'warn')
}

// 微信/QQ 等 IAB 没有保存文件的能力：亮横幅引导去系统浏览器；
// emph=true 时脉冲一下把视线拽过去（被拦下的下载动作也走这里）
function guideBrowser(emph) {
  const bar = $('iab')
  if (!bar) return
  if (bar.hidden) {
    $('iabMsg').textContent = IAB + '内无法保存文件，复制链接到系统浏览器打开'
    bar.hidden = false
  }
  if (emph) {
    bar.classList.remove('pulse')
    void bar.offsetWidth
    bar.classList.add('pulse')
  }
}

$('iabCopy').addEventListener('click', () => {
  const url = location.href
  const b = $('iabCopy')
  const ok = () => {
    if (navigator.vibrate) navigator.vibrate(12)
    b.textContent = '已复制'
    setTimeout(() => { b.textContent = '复制链接' }, 2000)
  }
  const bad = () => {
    b.textContent = '长按地址栏复制'
    setTimeout(() => { b.textContent = '复制链接' }, 2400)
  }
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(url).then(ok, () => copyShareFallback(url, ok, bad))
  } else {
    copyShareFallback(url, ok, bad)
  }
})

if (IAB) {
  document.body.classList.add('iab') // CSS 据此藏做种入口
  guideBrowser(false)
}

if (window.visualViewport) {
  visualViewport.addEventListener('resize', () => {
    if (document.activeElement === codeInput) {
      $('codezone').scrollIntoView({ block: 'center', behavior: 'auto' })
    }
  })
}

// 移动端 beforeunload 常不触发，pagehide 补一手；重复调用无害。
// destroy 会对 tracker 发 stopped，池随页面关闭而散。
function bye() {
  if (client) { try { client.destroy() } catch { /* 已毁 */ } }
}
window.addEventListener('beforeunload', bye)
window.addEventListener('pagehide', bye)
// bfcache 复活：client 已在 pagehide 毁掉，页面却原样回来了——重载走正常恢复路径
window.addEventListener('pageshow', e => { if (e.persisted) location.reload() })
