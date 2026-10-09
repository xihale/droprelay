/* share 前端 —— MD3 检索条 + 列表 + WebTorrent 做种。
   检索覆盖全库；空检索按目录浏览。目录 SPA 就地切换。
   文件链接（左键）拦截为 P2P 下载，Ctrl/Cmd/中键/长按菜单仍是直链——直链即 webseed 本身。
   ICE：自托管 STUN + TURN（droprelay-stun，构建期经 ICE_SERVERS 注入）。
   JS 失效时链接自然退化为直链下载，行为等同旧 Caddy。
   一次下载只走一个源：本机整文件 → 同学（tracker 说有人在做种）→ 路由器顺序 HTTP → 浏览器自带下载。
   绝不 P2P 和 HTTP Range 同时打 USB。用户拿到文件之前不做种。
   做种库每种一个 OPFS 整文件（「<文件名> - <ih前8位>/data」），不是一块一个文件。
   行标绿以这个整文件的字节数为真源。localStorage 只记保留期时钟。
   空手访客不入池。有人做种时后来者走 P2P（USB 不再被打）；没人则第一份顺序读 USB，下完再做种。
   做种保留期 SEED_RETAIN：下载满 14 天持续做种，过期自动清出（账本 + OPFS + 池），页面开着期间每半小时巡检。
   浏览器配额装不下块（estimate 预检不足，或 P2P 半路 QuotaExceeded）不硬撑：自动转直链——
   有落盘句柄就流式直取 webseed 写盘，否则触发浏览器自带下载，配额再小也有得下。
   深链：#/课本/数学/xxx.pdf 触发下载（已有的不重存）；?testdump=1 校验 sha256；
   ?webseed= 本地联调改向。 */
'use strict'

import WebTorrent from 'webtorrent'
import { TRACKER, wtOpts, swarmOpts, p2pWaitMs, pieceLenFromTorrent, emptyBitfield, OneFileStore, withTimeout, human, canDirPicker, canSavePicker, fsSink, testSink, memSink, saveBlob, makeStop, drainReader } from '../common/shared.js'

const $ = id => document.getElementById(id)
const params = new URLSearchParams(location.search)
const TESTDUMP = params.get('testdump') === '1'

const WEBSEED = (() => {
  const p = params.get('webseed')
  if (p) return p.endsWith('/') ? p : p + '/' // 联调改向也统一带尾斜杠，拼接处不产出双斜杠/缺斜杠
  return location.origin + '/' // 站点根即 webseed
})()

/* 保留期账本（localStorage）：ih → 完成时间戳。只管「块留多久」，不管「块在不在」——
   在不在以 OPFS 探盘为准。v1（数组）格式自动迁移。 */
const DONE_KEY = 'share-done'
const SEED_RETAIN = 14 * 24 * 3600 * 1000 // share 持续做种 14 天；drop 仍是一次性 24h

function loadDone() {
  try {
    const raw = JSON.parse(localStorage.getItem(DONE_KEY) || '{}')
    const m = new Map()
    if (Array.isArray(raw)) for (const ih of raw) m.set(ih, Date.now()) // v1 数组格式迁移
    else for (const ih in raw) if (Number.isFinite(raw[ih])) m.set(ih, raw[ih])
    return m
  } catch { return new Map() }
}

function saveDone() {
  try { localStorage.setItem(DONE_KEY, JSON.stringify(Object.fromEntries(st.doneMap))) } catch { /* 隐身模式等存不进就算了 */ }
}

// 申请持久化存储（免遭磁盘紧张的 best-effort 驱逐）；进站即申请，下载完成后会再调（幂等）
let persistAsked = false
function requestPersist() {
  try {
    const s = navigator.storage
    if (!s || !s.persist || !s.persisted || persistAsked) return
    persistAsked = true
    s.persisted().then(has => { if (!has) s.persist().catch(() => {}) }).catch(() => {})
  } catch { /* 不支持就算了 */ }
}

const st = {
  entries: [],
  byPath: new Map(),     // decodeURIComponent(path) → entry
  files: new Map(),      // path → {entry, tr, got, saveHandle, base}（按路径建账：同内容多路径各一份，进度互不串）
  torrents: new Map(),   // ih → Torrent（续种 + 按需下载入池的）
  have: new Set(),       // 本机 OPFS 有完整数据的 ih（行标绿 + 自动续种的唯一依据）
  active: new Set(),     // 排队或下载中的 path（行画进度）
  fail: new Set(),       // 上次尝试失败的 path（行标红，点击重试）
  doneMap: loadDone(),   // 已下载 ih → 完成时间戳（只做保留期时钟）
  queue: [], queueRunning: false,
  dirHandle: null,
  recvs: new Set(),      // 在传记录 {id, stop}：看门狗到点时自证还是本笔在传（并发下多笔共存）
  curDir: '',            // 当前目录（manifest 相对路径，'' = 根）
  largest: 0,            // 当前目录最大文件（闲置相对条已不用，保留给旧调用）
  query: '',             // 全库搜索词；非空时列表改显示匹配项
  viaUser: false,        // 本次队列是否来自真实点击（决定能否弹保存框）
}
window.__share = st
// E2E 观测位：下载文件夹按钮的分支绑着 hover/fine 媒体特征，无头环境点不出 dirPicker 路径
window.__act = { requestFolder, requestDownload }

const client = new WebTorrent(wtOpts())
window.__client = client // 调试观测位

function storeName(e) {
  return e.name + ' - ' + e.ih.slice(0, 8)
}
client.on('error', err => {
  console.warn('[webtorrent]', err)
  for (const [ih, t] of st.torrents) if (t.destroyed) st.torrents.delete(ih) // 炸掉的种子出池，别赖成僵尸
})
client.on('warning', () => { /* tracker 单点抖动不必理会 */ })

/* ---------- 小工具 ---------- */

function fmtTime(ns) {
  if (!ns) return '—'
  return new Date(ns / 1e6).toLocaleString('zh-CN', { hour12: false, year: 'numeric', month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' }) // 秒级精度只在悬停 title 里
}

function encPath(p) {
  return p.split('/').map(encodeURIComponent).join('/')
}

function b64ToBuf(b64) {
  const bin = atob(b64)
  const u = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i)
  return u
}

/* ---------- 图标（tabler，与 Caddy 模板同款） ---------- */

const T = (cls, inner) =>
  `<svg xmlns="http://www.w3.org/2000/svg" class="icon icon-tabler ${cls}" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round">${inner}</svg>`
const P0 = '<path stroke="none" d="M0 0h24v24H0z" fill="none"/>'

const ICONS = {
  dir: T('icon-tabler-folder-filled', P0 +
    '<path d="M9 3a1 1 0 0 1 .608 .206l.1 .087l2.706 2.707h6.586a3 3 0 0 1 2.995 2.824l.005 .176v8a3 3 0 0 1 -2.824 2.995l-.176 .005h-14a3 3 0 0 1 -2.995 -2.824l-.005 -.176v-11a3 3 0 0 1 2.824 -2.995l.176 -.005h4z" stroke-width="0" fill="currentColor"/>'),
  file: T('icon-tabler-file', P0 +
    '<path d="M14 3v4a1 1 0 0 0 1 1h4"/><path d="M17 21h-10a2 2 0 0 1 -2 -2v-14a2 2 0 0 1 2 -2h7l5 5v11a2 2 0 0 1 -2 2z"/>'),
  pdf: T('icon-tabler-file-type-pdf', P0 +
    '<path d="M14 3v4a1 1 0 0 0 1 1h4"/><path d="M5 12v-7a2 2 0 0 1 2 -2h7l5 5v4"/><path d="M5 18h1.5a1.5 1.5 0 0 0 0 -3h-1.5v6"/><path d="M17 18h2"/><path d="M20 15h-3v6"/><path d="M11 15v6h1a2 2 0 0 0 2 -2v-2a2 2 0 0 0 -2 -2h-1z"/>'),
  zip: T('icon-tabler-file-zip', P0 +
    '<path d="M6 20.735a2 2 0 0 1 -1 -1.735v-14a2 2 0 0 1 2 -2h7l5 5v11a2 2 0 0 1 -2 2h-1"/><path d="M11 17a2 2 0 0 1 2 2v2a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1v-2a2 2 0 0 1 2 -2z"/><path d="M11 5l-1 0"/><path d="M13 7l-1 0"/><path d="M11 9l-1 0"/><path d="M13 11l-1 0"/><path d="M11 13l-1 0"/><path d="M13 15l-1 0"/>'),
  movie: T('icon-tabler-movie', P0 +
    '<path d="M4 4m0 2a2 2 0 0 1 2 -2h12a2 2 0 0 1 2 2v12a2 2 0 0 1 -2 2h-12a2 2 0 0 1 -2 -2z"/><path d="M8 4l0 16"/><path d="M16 4l0 16"/><path d="M4 8l4 0"/><path d="M4 16l4 0"/><path d="M4 12l16 0"/><path d="M16 8l4 0"/><path d="M16 16l4 0"/>'),
  img: T('icon-tabler-photo', P0 +
    '<path d="M15 8h.01"/><path d="M3 6a3 3 0 0 1 3 -3h12a3 3 0 0 1 3 3v12a3 3 0 0 1 -3 3h-12a3 3 0 0 1 -3 -3v-12z"/><path d="M3 16l5 -5c.928 -.893 2.072 -.893 3 0l5 5"/><path d="M14 14l1 -1c.928 -.893 2.072 -.893 3 0l3 3"/>'),
  audio: T('icon-tabler-music', P0 +
    '<path d="M6 17m-3 0a3 3 0 1 0 6 0"/><path d="M16 17m-3 0a3 3 0 1 0 6 0"/><path d="M9 17l0 -13l10 0l0 13"/><path d="M9 8l10 0"/>'),
  text: T('icon-tabler-file-text', P0 +
    '<path d="M14 3v4a1 1 0 0 0 1 1h4"/><path d="M17 21h-10a2 2 0 0 1 -2 -2v-14a2 2 0 0 1 2 -2h7l5 5v11a2 2 0 0 1 -2 2z"/><path d="M9 9l1 0"/><path d="M9 13l6 0"/><path d="M9 17l6 0"/>'),
  up: T('icon-tabler-corner-left-up', P0 +
    '<path d="M18 18h-6a3 3 0 0 1 -3 -3v-10l-4 4m8 0l-4 -4"/>'),
  dldir: T('icon-tabler-download', P0 +
    '<path d="M4 16v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2 -2v-2"/><path d="M7 11l5 5l5 -5"/><path d="M12 4l0 12"/>'),
}

function iconFor(name) {
  const dot = name.lastIndexOf('.')
  const ext = dot > 0 ? name.slice(dot + 1).toLowerCase() : ''
  if (['jpg', 'jpeg', 'png', 'gif', 'webp', 'tiff', 'bmp', 'heif', 'heic', 'svg', 'avif'].includes(ext)) return ICONS.img
  if (['mp4', 'mov', 'm4v', 'mpeg', 'mpg', 'avi', 'ogg', 'webm', 'mkv', 'vob', '3gp'].includes(ext)) return ICONS.movie
  if (['mp3', 'm4a', 'aac', 'flac', 'wav', 'wma', 'midi'].includes(ext)) return ICONS.audio
  if (ext === 'pdf') return ICONS.pdf
  if (['zip', 'gz', 'xz', 'tar', '7z', 'rar', 'zst'].includes(ext)) return ICONS.zip
  if (['txt', 'doc', 'docx', 'odt', 'rtf', 'md'].includes(ext)) return ICONS.text
  return ICONS.file
}

/* ---------- 目录模型：manifest 扁平路径 → 当前目录的子目录/文件 ---------- */

function dirFromLocation() {
  let seg
  try { seg = decodeURIComponent(location.pathname).split('/').filter(Boolean) } catch { seg = [] }
  if (seg[0] === 'app' || seg[0] === 'f') return '' // 旧入口按根处理
  return seg.join('/')
}

function listDir(entries, dir) {
  const dirs = new Map()
  const files = []
  const prefix = dir ? dir + '/' : ''
  for (const e of entries) {
    if (!e.path.startsWith(prefix)) continue
    const rest = e.path.slice(prefix.length)
    const i = rest.indexOf('/')
    if (i < 0) { files.push(e); continue }
    const name = rest.slice(0, i)
    let d = dirs.get(name)
    if (!d) dirs.set(name, d = { name, mtime: 0, size: 0, count: 0 })
    d.count++
    d.size += e.size
    if (e.mtime > d.mtime) d.mtime = e.mtime
  }
  return { dirs, files }
}

function parentOf(p) {
  const i = p.lastIndexOf('/')
  return i < 0 ? '' : p.slice(0, i)
}

function fillName(el, text, q) {
  el.textContent = ''
  const needle = (q || '').trim()
  if (!needle) { el.textContent = text; return }
  const low = text.toLowerCase()
  const n = needle.toLowerCase()
  let i = 0
  while (i < text.length) {
    const j = low.indexOf(n, i)
    if (j < 0) { el.append(text.slice(i)); break }
    if (j > i) el.append(text.slice(i, j))
    const m = document.createElement('mark')
    m.textContent = text.slice(j, j + needle.length)
    el.append(m)
    i = j + needle.length
  }
}

// 全库搜索：文件名/路径命中列出文件；段名命中列出目录
function searchHits(q) {
  q = q.trim().toLowerCase()
  if (!q) return null
  const dirs = new Map()
  const files = []
  for (const e of st.entries) {
    if (e.name.toLowerCase().includes(q) || e.path.toLowerCase().includes(q)) files.push(e)
    const segs = e.path.split('/')
    let acc = ''
    for (let i = 0; i < segs.length - 1; i++) {
      acc = acc ? acc + '/' + segs[i] : segs[i]
      if (segs[i].toLowerCase().includes(q) && !dirs.has(acc)) {
        dirs.set(acc, { name: segs[i], path: acc, mtime: 0, size: 0, count: 0 })
      }
    }
  }
  for (const e of st.entries) {
    for (const [p, d] of dirs) {
      if (e.path.startsWith(p + '/')) {
        d.count++
        d.size += e.size
        if (e.mtime > d.mtime) d.mtime = e.mtime
      }
    }
  }
  return { dirs, files }
}

/* ---------- SPA 导航：目录跳转不重载页面，做种池/peer 连接常驻 ---------- */

function navigate(url) {
  if (location.pathname + location.search !== url) history.pushState(null, '', url)
  render()
  window.scrollTo(0, 0)
}

// 带 data-nav 的链接（面包屑/目录行）就地切换；修饰键仍走整页加载（新标签等）
document.addEventListener('click', e => {
  if (e.ctrlKey || e.metaKey || e.altKey || e.shiftKey || e.button !== 0) return
  const a = e.target instanceof Element && e.target.closest('a[data-nav]')
  if (!a) return
  e.preventDefault()
  if (st.query) { // 搜到目录再点进去：清检索，露出该夹内容
    st.query = ''
    const fi = $('filter')
    if (fi) fi.value = ''
    const cl = $('search-clear')
    if (cl) cl.hidden = true
  }
  navigate(a.getAttribute('href'))
})

window.addEventListener('popstate', () => {
  render()
  window.scrollTo(0, 0)
})

/* ---------- 渲染：面包屑 + 表格 ---------- */

function renderCrumbs() {
  const nav = $('crumbs')
  nav.innerHTML = ''
  if (!st.curDir || (st.query || '').trim()) { nav.hidden = true; return }
  nav.hidden = false
  const add = (href, label, last) => {
    const a = document.createElement('a')
    a.className = 'crumb'
    a.dataset.nav = ''
    a.href = href
    a.textContent = label
    if (last) a.setAttribute('aria-current', 'page')
    nav.appendChild(a)
  }
  add('/', '根')
  const seg = st.curDir.split('/')
  seg.forEach((s, i) => {
    const sep = document.createElement('span')
    sep.className = 'crumb-sep'
    sep.textContent = '/'
    sep.setAttribute('aria-hidden', 'true')
    nav.appendChild(sep)
    add('/' + seg.slice(0, i + 1).map(encodeURIComponent).join('/') + '/', s, i === seg.length - 1)
  })
}

function idleBarWidth(size) {
  return st.largest > 0 ? (size / st.largest * 100).toFixed(2) + '%' : '0%'
}

// 下载速率：≥200ms 的采样窗算瞬时值，EMA 抹平抖动；起步未出字节时不给数（显示「连接中」）
function tickRate(f) {
  const now = performance.now()
  if (f._lt == null) { f._lt = now; f._lg = f.got; return }
  const dt = now - f._lt
  if (dt < 200) return
  const inst = Math.max(0, f.got - f._lg) / (dt / 1000)
  f.rate = f.rate == null ? inst : f.rate * 0.6 + inst * 0.4
  f._lt = now
  f._lg = f.got
}

function humanDur(sec) {
  if (!Number.isFinite(sec) || sec <= 0) return ''
  if (sec < 90) return Math.round(sec) + ' 秒'
  if (sec < 5400) return Math.round(sec / 60) + ' 分钟'
  return (sec / 3600).toFixed(1) + ' 小时'
}

function timeCell(ns) {
  if (!ns) return '<td class="timestamp hideable"></td>'
  const iso = new Date(ns / 1e6).toISOString()
  return `<td class="timestamp hideable"><time datetime="${iso}" title="${new Date(ns / 1e6).toLocaleString('zh-CN', { hour12: false })}">${fmtTime(ns)}</time></td>`
}

function dirRow(d, loc) {
  const li = document.createElement('li')
  li.className = 'item dir'
  const prefix = loc || ((st.curDir ? st.curDir + '/' : '') + d.name)
  const href = '/' + encPath(prefix) + '/'
  const zipHref = '/' + encPath(prefix) + '?zip=1'
  const parent = parentOf(prefix)
  const sub = loc
    ? (parent ? parent + ' · ' : '') + (d.count ? d.count + ' 个文件' : '')
    : (d.count ? d.count + ' 个文件 · ' + human(d.size) : '')
  li.innerHTML =
    `<a class="item-main" data-nav href="${href}"><span class="lead">${ICONS.dir}</span><span class="text"><span class="name"></span><span class="sub"></span></span></a>` +
    (d.count > 0
      ? `<a class="zip-dir icon-btn" href="${zipHref}" title="下载此文件夹（桌面 P2P；Ctrl/中键 = 直接下 zip）" aria-label="下载此文件夹">${ICONS.dldir}</a>`
      : '')
  fillName(li.querySelector('.name'), d.name, st.query)
  li.querySelector('.sub').textContent = sub
  if (d.count > 0) {
    const a = li.querySelector('a.zip-dir')
    a.addEventListener('click', ev => {
      if (ev.ctrlKey || ev.metaKey || ev.altKey || ev.shiftKey || ev.button !== 0) return // 修饰键 = zip 直下
      if (!canDirPicker()) return // 无目录选择能力 → 放行走服务端 zip
      ev.preventDefault()
      requestFolder(prefix + '/')
    })
  }
  return li
}

function fileRow(f, showPath) {
  const e = f.entry
  const li = document.createElement('li')
  li.className = 'item'
  li.dataset.path = e.path
  const href = '/' + encPath(e.path)
  li.innerHTML =
    `<div class="fill" aria-hidden="true"></div>` +
    `<a class="item-main file-link" href="${href}" title="点按下载（Ctrl/中键 = 直链）"><span class="lead">${iconFor(e.name)}</span><span class="text"><span class="name"></span><span class="sub size-text"></span></span></a>`
  fillName(li.querySelector('.name'), e.name, st.query)
  const a = li.querySelector('a.file-link')
  a.addEventListener('click', ev => {
    if (ev.ctrlKey || ev.metaKey || ev.altKey || ev.shiftKey || ev.button !== 0) return // 修饰键/中键 = 直链
    ev.preventDefault()
    requestDownload([e.path], true)
  })
  a.addEventListener('keydown', ev => {
    if (ev.key !== 'Enter' && ev.key !== ' ') return
    ev.preventDefault()
    requestDownload([e.path], true)
  })
  f.tr = li
  f._showPath = showPath
  renderRow(f) // 状态只有一个画笔：renderRow
  return li
}

// 行状态唯一画笔：active → 进度；fail → 重试；have → 绿标；否则空闲相对大小条。
// 下载回调、翻页重画、探盘完成……任何时刻都走这里，行不会漂移出状态机。
// 排队/失败按 path 记账（同一份内容在不同路径是两笔下载），have 按 ih（块是同一份）。
function renderRow(f) {
  if (!f || !f.tr) return
  const k = f.entry.path
  const ih = f.entry.ih
  const bar = f.tr.querySelector('.fill')
  const txt = f.tr.querySelector('.size-text')
  f.tr.classList.remove('dl', 'err', 'have', 'ok-flash')
  const pathHint = f._showPath && parentOf(f.entry.path) ? parentOf(f.entry.path) + ' · ' : ''
  if (st.active.has(k)) {
    f.tr.classList.add('dl')
    const pct = f.entry.size > 0 ? Math.min(1, f.got / f.entry.size) : 0
    if (bar) bar.style.transform = 'scaleX(' + (f.got || f.saving ? pct : 0.07).toFixed(4) + ')'
    if (f.saving) {
      txt.textContent = '正在保存 · ' + human(f.entry.size)
    } else if (!f.got) {
      txt.textContent = '连接中 · ' + human(f.entry.size) // 入池/announce/握手里，别让行看着像死了
    } else {
      let s = human(f.got) + ' / ' + human(f.entry.size)
      if (f.rate > 0) {
        s += ' · ' + human(f.rate) + '/s'
        const eta = humanDur((f.entry.size - f.got) / f.rate)
        if (eta) s += ' · 剩 ' + eta
      }
      txt.textContent = s
    }
  } else if (st.fail.has(k)) {
    f.tr.classList.add('err')
    if (bar) bar.style.transform = 'scaleX(0)'
    txt.textContent = '失败 · 点击重试'
  } else {
    if (st.have.has(ih)) f.tr.classList.add('have')
    if (bar) bar.style.transform = 'scaleX(0)'
    txt.textContent = pathHint + human(f.entry.size)
  }
}

function setMsg(text, cls) {
  const s = $('snack')
  if (!s) return
  if (!text) { s.hidden = true; s.textContent = ''; s.classList.remove('err'); return }
  s.hidden = false
  s.textContent = text
  s.classList.toggle('err', cls === 'err')
}

function bindFilter() {
  const fi = $('filter')
  const clear = $('search-clear')
  const apply = () => {
    st.query = fi.value
    clear.hidden = !fi.value
    render()
  }
  fi.addEventListener('input', apply)
  clear.addEventListener('click', () => { fi.value = ''; apply(); fi.focus() })
  document.addEventListener('keydown', e => {
    if (e.key === '/' && e.target !== fi && !/^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName)) {
      e.preventDefault()
      fi.focus()
    }
    if (e.key === 'Escape' && document.activeElement === fi) {
      if (fi.value) { fi.value = ''; apply() }
      else fi.blur()
    }
  })
}

function render() {
  st.curDir = dirFromLocation()
  const q = (st.query || '').trim()
  const hits = searchHits(q)
  const { dirs, files } = hits || listDir(st.entries, st.curDir)
  const dirList = [...dirs.values()].sort((a, b) => a.name.localeCompare(b.name, 'zh-Hans-CN'))
  const fileList = files.sort((a, b) => a.name.localeCompare(b.name, 'zh-Hans-CN'))
  document.title = q ? q : (st.curDir ? st.curDir.slice(st.curDir.lastIndexOf('/') + 1) : location.hostname)
  renderCrumbs()
  st.largest = fileList.reduce((m, e) => Math.max(m, e.size), 0)
  const total = fileList.reduce((s, e) => s + e.size, 0)
  let meta = ''
  if (q) {
    const n = dirList.length + fileList.length
    if (n) meta = '<span><b>' + n + '</b> 个匹配</span>'
  } else {
    if (dirList.length) meta += '<span><b>' + dirList.length + '</b> 个目录</span>'
    if (fileList.length) meta += (meta ? ' · ' : '') + '<span><b>' + fileList.length + '</b> 个文件 · <b>' + human(total) + '</b></span>'
  }
  $('summary').innerHTML = meta
  const tb = $('rows')
  tb.innerHTML = ''
  for (const f of st.files.values()) f.tr = null // 旧列表的行已随 DOM 拆除
  if (!dirList.length && !fileList.length) {
    const empty = document.createElement('li')
    empty.className = 'empty'
    if (q) empty.textContent = '没有叫这个名字的'
    else if (st.curDir && !st.entries.some(e => e.path.startsWith(st.curDir + '/'))) {
      // 清单里没有这条路径：不是空目录，是不存在（被移走/链接打错）——给 404 视图
      document.title = '404'
      empty.textContent = '这个路径不存在，可能已被移走'
      empty.appendChild(document.createElement('br'))
      const home = document.createElement('a')
      home.href = '/'
      home.textContent = '回根目录'
      empty.appendChild(home)
    } else empty.textContent = st.entries.length ? '这个目录是空的' : ''
    if (empty.childNodes.length) tb.appendChild(empty)
  }
  for (const d of dirList) tb.appendChild(dirRow(d, hits ? d.path : undefined))
  for (const e of fileList) {
    const f = st.files.get(e.path)
    if (f) tb.appendChild(fileRow(f, !!hits))
  }
  $('dl-dir').hidden = !!(q || !st.curDir || !st.entries.some(e => e.path.startsWith(st.curDir + '/')))
  updateSwarm()
}

/* ---------- 真源探盘：OPFS 里谁块齐谁「已有」，空壳顺手清 ---------- */

// webtorrent 用 FSA store 的前提就是这两样；没有它的浏览器走内存 store，无从探起
function hasFSA() {
  return !!(navigator.storage && navigator.storage.getDirectory &&
    window.FileSystemFileHandle && FileSystemFileHandle.prototype.createWritable)
}

async function probeHave() {
  if (!hasFSA()) return
  try {
    const root = await withTimeout(navigator.storage.getDirectory(), 2000)
    const want = new Set(st.entries.map(e => e.name + ' - ' + e.ih.slice(0, 8)))
    let healed = false
    // 独占锁拿得到=没有别的 share 标签开着 → 中间态即刻清（半截块不留、in/ 全清）；
    // 拿不到 → 可能有标签正在下，维持保守：半截留作续传、in/ 只清超过一天的
    let aggressive = false
    if (navigator.locks) {
      aggressive = await new Promise(res => {
        navigator.locks.request('share-active', { mode: 'exclusive', ifAvailable: true }, lock => { res(!!lock); return Promise.resolve() }).catch(() => res(false))
      })
    }
    const inCutoff = aggressive ? Infinity : Date.now() - 24 * 3600 * 1000
    for (const e of st.entries) {
      const dirName = storeName(e)
      let d
      try { d = await root.getDirectoryHandle(dirName) } catch { continue }
      let sum = 0
      try {
        const data = await d.getFileHandle('data')
        sum = (await data.getFile()).size
      } catch {
        // 旧「一块一文件」目录不再当已有，独占时清掉
        if (aggressive) try { await root.removeEntry(dirName, { recursive: true }) } catch { /* */ }
        continue
      }
      if (sum >= e.size) {
        st.have.add(e.ih)
        if (!st.doneMap.has(e.ih)) { st.doneMap.set(e.ih, Date.now()); healed = true }
      } else if (sum === 0 || aggressive) {
        try { await root.removeEntry(dirName, { recursive: true }) } catch { /* 无所谓 */ }
      }
    }
    // 上次下完只写了 in/、没灌 data：刷新前把中转救进做种库，别当垃圾清掉
    try {
      const dh = await root.getDirectoryHandle('in', { create: false })
      for (const e of st.entries) {
        if (st.have.has(e.ih)) continue
        try {
          const fh = await dh.getFileHandle(e.ih)
          const file = await fh.getFile()
          if (file.size < e.size) continue
          if (await persistSeed(e, file)) {
            st.have.add(e.ih)
            if (!st.doneMap.has(e.ih)) { st.doneMap.set(e.ih, Date.now()); healed = true }
            try { await dh.removeEntry(e.ih) } catch { /* */ }
          }
        } catch { /* 没有这份中转 */ }
      }
    } catch { /* 没有 in/ */ }
    // 清扫：种子 store 形态（「… - 8位hex」）却不在当前清单里的目录一并清掉
    const stale = []
    for await (const [name, h] of root.entries()) {
      if (h.kind === 'directory' && !want.has(name) && / - [0-9a-f]{8}$/.test(name)) stale.push(name)
    }
    for (const name of stale) {
      try { await root.removeEntry(name, { recursive: true }) } catch { /* 无所谓 */ }
    }
    // in/ 中转残留：独占时全清，保守时只清超一天的
    try {
      const dh = await root.getDirectoryHandle('in', { create: false })
      for await (const [name, h] of dh.entries()) {
        if (h.kind !== 'file') continue
        try { if ((await h.getFile()).lastModified < inCutoff) await dh.removeEntry(name) } catch { /* 无所谓 */ }
      }
    } catch { /* 没有 in/ */ }
    if (healed) saveDone()
  } catch { /* OPFS 不可用就算了：本会话下载的照常标绿 */ }
}

/* ---------- 做种池：按需入池。启动只挂本机有数据的种子（续种），
   其余等真正下载时再加——空手访客不向 tracker 报几十个 leech。 ---------- */

let addQ = Promise.resolve()

// 连续「探完无源」计数：源不在线时，文件夹批量下载别每个文件都干等 announce/握手——
// 连空两次，后面的直接走 HTTP；任一 tracker 报「有种」即清零，自动恢复探测。
let deadStreak = 0

function addTorrent(e, cb) {
  addQ = addQ.then(async () => {
    // 就绪窗动态:服务器忙(在直出大文件)时拉长,P2P 建链慢一步也等得起;
    // 空闲维持 8s,HTTP 兜底快进快出。见 shared.js p2pWaitMs。
    const waitMs = await p2pWaitMs()
    return new Promise(done => {
    const buf = b64ToBuf(e.torrent)
    const opts = {
      announce: [TRACKER],
      deselect: true, // 不挂 urlList：webseed Range 会把 USB 打随机，HTTP 源由我们自己 fetch
      store: OneFileStore,
      ...swarmOpts(),
    }
    if (st.have.has(e.ih)) {
      opts.skipVerify = true
    } else {
      const pl = pieceLenFromTorrent(buf)
      if (pl) opts.bitfield = emptyBitfield(e.size, pl)
    }
    let finished = false
    const finish = tor => {
      if (finished) return
      finished = true
      clearInterval(iv)
      if (cb) cb(tor)
      done()
    }
    const t0 = setTimeout(finish, waitMs)
    // 同 ih 并发首投撞车（文件夹下载 3 路并发下必现）：输家吃 duplicate，
    // 等赢家的那份进池后借同一份收尾，就绪窗仍是上限
    const iv = setInterval(() => {
      const win = st.torrents.get(e.ih)
      if (win && !win.destroyed) finish(win)
    }, 50)
    try {
      const t = client.add(buf, opts)
      t.once('metadata', () => {
        const tr = t.discovery && t.discovery.tracker
        if (tr && !t._seedHook) {
          t._seedHook = true
          tr.on('update', data => {
            t._seeds = Number(data.complete) || 0
            if (t._seeds > 0) deadStreak = 0 // 有种了：恢复后续文件的 P2P 探测
          })
        }
      })
      t.once('ready', () => { clearTimeout(t0); st.torrents.set(e.ih, t); finish(t) })
      t.once('error', err => {
        if (!/duplicate/i.test(err && err.message || '')) { clearTimeout(t0); finish() }
      })
    } catch {
      clearTimeout(t0)
      finish()
    }
    })
  }).catch(() => {})
}

async function reseedHave() {
  for (const e of st.entries) {
    if (!st.have.has(e.ih) || st.torrents.has(e.ih)) continue
    await new Promise(res => { addTorrent(e, res) })
    await new Promise(r => setTimeout(r, 0))
  }
}

function updateSwarm() {
  const b = $('swarm-btn')
  if (!b) return
  const n = st.have.size
  if (!n) { b.hidden = true; return }
  b.hidden = false
  b.textContent = '做种 ' + n
  if ($('swarm-panel').open) renderPanel()
}

/* ---------- 存储与做种面板：每份块一行（名目/大小/存入日/连接数），可单删可全清 ---------- */

function openPanel() {
  const d = $('swarm-panel')
  if (!d.open) d.showModal()
  renderPanel() // 先开再渲染：renderPanel 以 open 为闸，顺序反了会开出一个空面板
  refreshJunk() // 盘点账外缓存（半截块+中转），开面板时看一眼
}

function haveDirNames() {
  return new Set(st.entries.filter(e => st.have.has(e.ih)).map(storeName))
}

async function dirBytes(dir) {
  let n = 0
  for await (const [, h] of dir.entries()) {
    try {
      if (h.kind === 'file') n += (await h.getFile()).size
      else if (h.kind === 'directory') n += await dirBytes(h)
    } catch { /* 句柄失效就跳过 */ }
  }
  return n
}

// 账外缓存盘点：OPFS 里除「已存文件的块目录」以外的一切（半截下载、in/ 中转、旧 chunks/）
async function refreshJunk() {
  const el = $('panel-junk')
  if (!el) return
  try {
    const root = await navigator.storage.getDirectory()
    const keep = haveDirNames()
    let junkBytes = 0
    for await (const [name, h] of root.entries()) {
      if (h.kind === 'file') {
        try { junkBytes += (await h.getFile()).size } catch { /* */ }
        continue
      }
      if (keep.has(name)) continue
      junkBytes += await dirBytes(h)
    }
    if (junkBytes < 1024) { el.hidden = true; return }
    el.hidden = false
    el.textContent = ''
    el.append('另有未完成/中转缓存 ' + human(junkBytes) + ' ')
    const b = document.createElement('button')
    b.type = 'button'
    b.className = 'panel-del'
    b.textContent = '立即清理'
    b.disabled = st.active.size > 0
    b.title = b.disabled ? '有下载正在进行，等完成后再清' : '删除全部半截块与中转残留'
    b.addEventListener('click', async () => {
      b.disabled = true
      await sweepJunk()
      updateSwarm()
      refreshJunk()
    })
    el.appendChild(b)
  } catch { el.hidden = true }
}

async function sweepJunk() {
  try {
    const root = await navigator.storage.getDirectory()
    const keep = haveDirNames()
    const doomed = []
    for await (const [name, h] of root.entries()) {
      if (h.kind === 'file') { doomed.push(name); continue }
      if (name === 'in') { // 中转区：清内容留目录，省得下次 getDirectoryHandle 再建
        const dh = await root.getDirectoryHandle('in')
        for await (const [fn] of dh.entries()) {
          try { await dh.removeEntry(fn, { recursive: true }) } catch { /* 无所谓 */ }
        }
        continue
      }
      if (!keep.has(name)) doomed.push(name)
    }
    for (const name of doomed) { try { await root.removeEntry(name, { recursive: true }) } catch { /* 无所谓 */ } }
  } catch { /* 无所谓 */ }
}

function renderPanel() {
  const d = $('swarm-panel')
  if (!d.open) return
  const ul = $('panel-list')
  ul.innerHTML = ''
  const byIh = new Map()
  for (const e of st.entries) {
    if (!st.have.has(e.ih)) continue
    if (!byIh.has(e.ih)) byIh.set(e.ih, [])
    byIh.get(e.ih).push(e)
  }
  // 只报本页做种块合计。navigator.storage.estimate 的 quota 是磁盘剩余的 ~80%，
  // usage 还掺着 HTTP 缓存/其它源私有文件，会画出「已用 11 GB / 配额 291 GB」这种谎。
  let stored = 0
  for (const es of byIh.values()) stored += es[0].size
  $('panel-quota').textContent = stored ? '已存 ' + human(stored) : ''
  if (!byIh.size) {
    ul.innerHTML = '<li class="panel-empty">没有存储的文件</li>'
    return
  }
  for (const [ih, es] of byIh) {
    const t = st.torrents.get(ih)
    const doneAt = st.doneMap.get(ih)
    const busy = [...st.active].some(k => { const f = st.files.get(k); return f && f.entry.ih === ih })
    const li = document.createElement('li')
    const name = document.createElement('span')
    name.className = 'pn'
    name.textContent = es[0].name + (es.length > 1 ? '（另 ' + (es.length - 1) + ' 处同名）' : '')
    name.title = es.map(e => e.path).join('\n')
    const meta = document.createElement('span')
    meta.className = 'pd'
    meta.textContent = human(es[0].size)
      + (doneAt ? ' · ' + new Date(doneAt).toLocaleDateString() + ' 存' : '')
      + (t && t.ready ? ' · ' + t.numPeers + ' 连接' : '')
    const del = document.createElement('button')
    del.type = 'button'
    del.className = 'panel-del'
    del.textContent = '删除'
    del.disabled = busy
    del.title = busy ? '正在下载，等完成后再删' : '删除本机存储的这份文件（停止做种，之后可重新下载）'
    del.addEventListener('click', () => { deleteStored(ih); renderPanel(); updateSwarm() })
    li.append(name, meta, del)
    ul.appendChild(li)
  }
}

function deleteStored(ih) {
  for (const k of st.active) {
    const f = st.files.get(k)
    if (f && f.entry.ih === ih) return // 正在下载的先不动
  }
  const t = st.torrents.get(ih)
  if (t) {
    try { const p = client.remove(t, { destroyStore: true }); if (p && p.catch) p.catch(() => {}) } catch { /* 无所谓 */ }
    st.torrents.delete(ih)
  } else {
    const e = st.entries.find(x => x.ih === ih)
    if (e) purgeStore(e) // 不在池里：静默借道清掉 OPFS 块目录（零网络）
  }
  st.have.delete(ih)
  st.doneMap.delete(ih)
  saveDone()
  for (const f of st.files.values()) if (f.entry.ih === ih) renderRow(f)
}

/* ---------- 做种保留期：下载满 SEED_RETAIN 的种子不再续种，块数据一并清掉。
   做种只发生在页面开着的期间——页面一关客户端就销毁，所以「限制做种时间」
   落地为：加载时清一轮 + 打开期间每 30 分钟巡检一轮。 ---------- */

function sweepExpired() {
  const now = Date.now()
  const live = new Set(st.entries.map(e => e.ih))
  for (const ih of [...st.doneMap.keys()]) { // 清单里已消失的文件：账本键一并清
    if (!live.has(ih)) st.doneMap.delete(ih)
  }
  for (const e of st.entries) {
    const doneAt = st.doneMap.get(e.ih)
    if (doneAt === undefined || now - doneAt < SEED_RETAIN) continue
    st.doneMap.delete(e.ih)
    const t = st.torrents.get(e.ih)
    if (t && t.progress === 1) { // 本会话续种过的：退池 + 连块一起删
      st.torrents.delete(e.ih)
      st.have.delete(e.ih)
      try { const p = client.remove(t, { destroyStore: true }); if (p && p.catch) p.catch(() => {}) } catch { /* 无所谓 */ }
    } else if (!t) {
      purgeStore(e) // 不在池里：静默 add 借 store 初始化删块（种子内烙的 tracker 仍短暂上报）
    } // t 在池里还没下完 → 不动，留给下一轮
  }
  saveDone()
}

// 过期且不在池中的块：静默 add 借 webtorrent 的 store 初始化把块目录原样清掉，
// 随即 remove+destroyStore（种子内烙的 tracker 仍会短暂上报一员，add 完即撤）
function purgeStore(e) {
  try {
    client.add(b64ToBuf(e.torrent), { skipVerify: true }, t => {
      const rm = () => { try { const p = client.remove(t, { destroyStore: true }); if (p && p.catch) p.catch(() => {}) } catch { /* 无所谓 */ } }
      if (t.ready) rm()
      else t.once('ready', rm)
    })
  } catch { /* 清不掉就留给浏览器配额驱逐 */ }
}

/* ---------- 下载 ---------- */

function requestDownload(keys, viaUser) {
  if (viaUser) st.viaUser = true
  for (const k of keys) {
    const f = st.files.get(k)
    if (!f) continue
    st.fail.delete(k)
    // have 行也放行：本地块直接出，等于「再存一份」，不走网络
    if (!st.queue.includes(k) && !st.active.has(k)) {
      st.queue.push(k)
      st.active.add(k)
      f.got = 0
      f.saving = false
      f.rate = f._lt = f._lg = null // 重试/重下重算速率
    }
    renderRow(f)
  }
  pumpDownloads()
}

/* 文件夹下载：递归整棵子树进 P2P 队列，落盘保持目录结构。
   无 showDirectoryPicker 的浏览器不进来（按钮放行 → 服务端 ?zip=1 兜底）。 */
function requestFolder(prefix) {
  const list = st.entries.filter(e => e.path.startsWith(prefix))
  if (!list.length) return
  for (const e of list) {
    const f = st.files.get(e.path)
    if (f) f.base = prefix // makeSink 按相对 base 的路径逐级建目录，保持原结构
  }
  requestDownload(list.map(e => e.path), true)
  setMsg('已入队 ' + list.length + ' 个文件')
}

/* 存储余量：P2P 的块要落 OPFS 配额，装不下就别让用户干等一场注定失败的下载 */
async function storageTooTight(size) {
  try {
    if (!navigator.storage || !navigator.storage.estimate) return false
    const { usage = 0, quota = 0 } = await navigator.storage.estimate()
    if (!quota) return false // 拿不到有效配额的浏览器按充裕处理
    return quota - usage < size * 1.1 + 64 * 1024 * 1024
  } catch { return false }
}

function quotaish(e) {
  return !!e && (e.name === 'QuotaExceededError' || /quota/i.test(e.message || ''))
}

// 直链兜底：交给浏览器自带下载（存储归浏览器管，不经 OPFS 配额）
function triggerDownload(url, name) {
  const a = document.createElement('a')
  a.href = url
  a.download = name
  a.rel = 'noopener'
  a.style.display = 'none'
  document.body.appendChild(a)
  a.click()
  setTimeout(() => a.remove(), 60000)
}

// 流式直取 webseed 写进落盘句柄（不占块配额）
async function directFetch(f) {
  await recvHttp(f)
}

// P2P 装不下的兜底：有落盘句柄就直取 webseed 流式写盘，否则触发浏览器自带下载
async function directFallback(f) {
  if (f.saveHandle || st.dirHandle) {
    await directFetch(f)
    return
  }
  triggerDownload(WEBSEED + encPath(f.entry.path), f.entry.name)
}

// 并发下载：单流吃不满带宽/USB 盘，3 路基本到顶；再高只会在路由器上乱盘
const DL_CONC = 3

async function pumpDownloads() {
  if (st.queueRunning || !st.queue.length) return
  st.queueRunning = true

  if (!TESTDUMP && !st.dirHandle && st.viaUser) {
    try {
      if (canDirPicker() && st.queue.length > 1) {
        st.dirHandle = await window.showDirectoryPicker({ mode: 'readwrite' })
      } else if (canSavePicker() && st.queue.length === 1) {
        const f = st.files.get(st.queue[0])
        if (f && !f.saveHandle) {
          f.saveHandle = await window.showSaveFilePicker({ suggestedName: f.entry.name })
        }
      }
    } catch (e) {
      if (e && e.name === 'AbortError') { // 用户取消：整体放弃
        st.queueRunning = false
        st.viaUser = false
        for (const k of st.queue) {
          st.active.delete(k)
          renderRow(st.files.get(k))
        }
        st.queue = []
        setMsg('')
        return
      }
      // SecurityError（深链等无手势场景）：落 OPFS/内存兜底继续
    }
  }
  st.viaUser = false

  setMsg('')
  const agg = { msg: '', err: false } // 各 worker 的收尾说明，队列空了留最后一条
  await Promise.all(Array.from({ length: Math.min(DL_CONC, st.queue.length) }, () => dlWorker(agg)))
  // 跑的期间 requestDownload 可能又入了队：workers 已散，起下一轮
  st.queueRunning = false
  if (st.queue.length) { pumpDownloads(); return }
  if (agg.msg) setMsg(agg.msg, agg.err ? 'err' : undefined)
}

async function dlWorker(agg) {
  for (;;) {
    const k = st.queue.shift()
    if (k === undefined) return
    const f = st.files.get(k)
    if (!f || !st.active.has(k)) continue // 完成或已放弃的跳过
    try {
      if (!TESTDUMP && await storageTooTight(f.entry.size)) {
        // 块注定放不下：直链总有路（不记 have——没有块可续种）
        await directFallback(f)
        st.active.delete(k)
        renderRow(f)
        agg.msg = f.entry.name + '：存储吃紧，已改走直链'
        setMsg(agg.msg)
        continue
      }
      await recvFile(f)
      st.active.delete(k)
      st.fail.delete(k)
      if (f.tr) {
        renderRow(f)
        f.tr.classList.add('ok-flash')
        setTimeout(() => { if (f.tr) f.tr.classList.remove('ok-flash') }, 1500)
      }
    } catch (e) {
      if (e && e.message === '取消') continue
      if (quotaish(e)) {
        // 半路爆配额（OPFS 块写满）：P2P 装不下，直链总有路
        st.active.delete(k)
        try {
          await directFallback(f)
          renderRow(f)
          agg.msg = f.entry.name + '：存储不足，已改走直链'
          setMsg(agg.msg)
          continue
        } catch (e2) { e = e2 }
      }
      st.active.delete(k)
      st.fail.add(k)
      renderRow(f)
      agg.msg = f.entry.name + '：' + (e && e.message || '失败')
      agg.err = true
      setMsg(agg.msg, 'err')
    }
  }
}

function hasP2P(t) {
  return t.wires.some(w => {
    if (w.type === 'webSeed' || w.destroyed) return false
    const b = w.peerPieces
    const buf = b && (b.buffer || b)
    if (!buf || !buf.length) return false
    for (let i = 0; i < buf.length; i++) if (buf[i]) return true
    return false
  })
}

function seedersOf(t) {
  const tr = t.discovery && t.discovery.tracker
  if (!tr) return Promise.resolve(0)
  return new Promise(res => {
    const onUp = data => { cleanup(); res(Number(data.complete) || 0) }
    const timer = setTimeout(() => { cleanup(); res(0) }, 800)
    const cleanup = () => { clearTimeout(timer); tr.removeListener('update', onUp) }
    tr.once('update', onUp)
  })
}

function waitPeer(t, ms) {
  if (hasP2P(t)) return Promise.resolve(true)
  return new Promise(res => {
    const timer = setTimeout(() => stop(false), ms)
    const stop = ok => {
      clearTimeout(timer)
      t.removeListener('wire', onWire)
      res(ok || hasP2P(t))
    }
    const arm = w => {
      if (!w || w.type === 'webSeed' || w.destroyed) return
      const hit = () => { if (hasP2P(t)) stop(true) }
      w.once('bitfield', hit)
      w.once('have', hit)
      hit()
    }
    const onWire = w => arm(w)
    t.on('wire', onWire)
    for (const w of t.wires) arm(w)
  })
}

async function pickSource(f) {
  if (st.have.has(f.entry.ih)) return 'local'
  let t
  try { t = await addTorrentP(f.entry) } catch { return 'http' }
  if (!t) return 'http' // 等待窗内没等到种子（含 duplicate 输家）：直链兜底
  if (hasP2P(t)) { deadStreak = 0; return 'peer' }
  if (deadStreak >= 2) return 'http' // 同批已连空两次：省掉每文件 2s 探测
  const n = t._seeds != null ? t._seeds : await seedersOf(t)
  if (n > 0 && await waitPeer(t, 1200)) { deadStreak = 0; return 'peer' }
  deadStreak++
  return 'http'
}

// ih 的 have 账目：绿标 + 续保留期 + 申请持久化
function markHave(e) {
  st.have.add(e.ih)
  st.doneMap.set(e.ih, Date.now())
  saveDone()
  requestPersist()
}

let seedQ = Promise.resolve()

// 落盘后的 OPFS 补种拷贝（自选目录路径才有的整份复写）挪出下载队列：
// 磁盘→OPFS 的第二遍写在后台串行做，完成才标 have（绿标稍后亮起）。
function seedLater(e, file) {
  seedQ = seedQ.then(() => markSeeded(e, file)).then(ok => {
    if (!ok) return
    markHave(e)
    for (const f of st.files.values()) if (f.entry.ih === e.ih) renderRow(f)
    updateSwarm()
  }).catch(() => {})
}

async function recvFile(f) {
  const src = await pickSource(f)
  if (src === 'local' || src === 'peer') {
    try {
      await fromTorrent(f, src === 'peer')
      markHave(f.entry) // P2P 的块本来就直接落 OneFileStore，即刻可续种
      return true
    } catch { /* 本机/同学失败就改走 HTTP，用户总能拿到文件 */ }
  }
  const file = await recvHttp(f)
  if (!file) return false
  seedLater(f.entry, file)
  return true
}

async function persistSeed(e, blob) {
  if (!blob || !hasFSA()) return false
  const root = await navigator.storage.getDirectory()
  const dir = await root.getDirectoryHandle(storeName(e), { create: true })
  const fh = await dir.getFileHandle('data', { create: true })
  if ((await fh.getFile()).size >= e.size) return true
  const w = await fh.createWritable({ keepExistingData: false })
  const reader = blob.stream().getReader()
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      await w.write(value)
    }
    await w.close()
  } catch (err) {
    try { await w.abort() } catch { /* */ }
    throw err
  }
  return (await fh.getFile()).size >= e.size
}

async function markSeeded(e, blob) {
  const file = await Promise.resolve(blob)
  try { if (!await persistSeed(e, file)) return false } catch { return false }
  let t = st.torrents.get(e.ih)
  if (!t || t.destroyed) {
    try { t = await addTorrentP(e) } catch { t = null }
  }
  if (t && t.progress < 1) {
    try { t._markAllVerified(); t._checkDone() } catch { /* */ }
  }
  return true
}

async function fromTorrent(f, select) {
  const t = await addTorrentP(f.entry)
  const file = t.files && t.files[0]
  if (!file) throw new Error('失败')
  const sink = await makeSink(f)
  if (!sink) throw new Error('无法保存')
  const stopCtl = makeStop()
  const cur = { id: f.entry.ih, got: 0, stop: stopCtl.stop }
  st.recvs.add(cur)
  const reader = file.stream().getReader()
  let watchdog = 0
  const kick = () => {
    clearTimeout(watchdog)
    watchdog = setTimeout(() => { if (st.recvs.has(cur)) { st.recvs.delete(cur); cur.stop(new Error('超时无数据')) } }, 60000)
  }
  const done = () => clearTimeout(watchdog)
  try {
    if (select) file.select()
    kick()
    let lastDraw = 0
    await drainReader(reader, sink, stopCtl.signal, value => {
      kick()
      cur.got += value.byteLength
      f.got = cur.got
      const now = performance.now()
      if (now - lastDraw > 120) { tickRate(f); renderRow(f); lastDraw = now }
    })
    f.got = cur.got
    renderRow(f)
  } catch (e) {
    done()
    st.recvs.delete(cur)
    try { reader.cancel() } catch { /* 已断 */ }
    try { if (select) file.deselect() } catch { /* */ }
    if (sink.abort) { try { await sink.abort() } catch { /* */ } }
    throw e
  }
  st.recvs.delete(cur)
  done()
  await closeSink(f, sink)
  if (cur.got !== f.entry.size) throw new Error('大小不符（' + human(cur.got) + ' / ' + human(f.entry.size) + '）')
}

async function closeSink(f, sink) {
  f.saving = true
  renderRow(f)
  try { await sink.close() }
  finally { f.saving = false }
}

function addTorrentP(e) {
  const cur = st.torrents.get(e.ih)
  if (cur && !cur.destroyed) {
    if (cur.ready) return Promise.resolve(cur)
    return new Promise((res, rej) => {
      cur.once('ready', () => res(cur))
      cur.once('error', () => rej(new Error('失败')))
    })
  }
  return new Promise((res, rej) => {
    const t0 = setTimeout(() => rej(new Error('超时无数据')), 60000)
    const onErr = err => {
      if (/duplicate/i.test(err && err.message || '')) return
      client.off('error', onErr)
      clearTimeout(t0)
      rej(quotaish(err) ? err : new Error('种子无效'))
    }
    client.on('error', onErr)
    addTorrent(e, t => { client.off('error', onErr); clearTimeout(t0); res(t) })
  })
}

async function recvHttp(f) {
  const ih = f.entry.ih
  const stopCtl = makeStop()
  const cur = { id: ih, got: 0, stop: stopCtl.stop }
  st.recvs.add(cur)
  let watchdog = 0
  const kick = () => {
    clearTimeout(watchdog)
    watchdog = setTimeout(() => { if (st.recvs.has(cur)) { st.recvs.delete(cur); cur.stop(new Error('超时无数据')) } }, 60000)
  }
  const done = () => clearTimeout(watchdog)
  kick()

  let sink
  try {
    sink = await makeSink(f)
  } catch (e) {
    done()
    st.recvs.delete(cur)
    throw e
  }
  if (!sink) {
    done()
    st.recvs.delete(cur)
    triggerDownload(WEBSEED + encPath(f.entry.path), f.entry.name)
    f.got = f.entry.size
    renderRow(f)
    return null
  }

  try {
    const r = await fetch(WEBSEED + encPath(f.entry.path), { signal: stopCtl.signal, cache: 'no-store' })
    if (!r.ok || !r.body) throw new Error('直链不可用（' + r.status + '）')
    const reader = r.body.getReader()
    try {
      let lastDraw = 0
      await drainReader(reader, sink, stopCtl.signal, value => {
        kick()
        cur.got += value.byteLength
        f.got = cur.got
        const now = performance.now()
        if (now - lastDraw > 120) { tickRate(f); renderRow(f); lastDraw = now }
      })
    } finally {
      done()
      try { reader.cancel() } catch { /* 已断 */ }
    }
    f.got = cur.got
    renderRow(f)
  } catch (e) {
    done()
    st.recvs.delete(cur)
    if (sink.abort) { try { await sink.abort() } catch { /* 无所谓 */ } }
    throw e
  }
  st.recvs.delete(cur)
  done()
  await closeSink(f, sink)
  if (cur.got !== f.entry.size) throw new Error('大小不符（' + human(cur.got) + ' / ' + human(f.entry.size) + '）')
  return sink.seedFile ? sink.seedFile() : null
}

function withSeed(s, fn) {
  s.seedFile = fn
  return s
}

async function makeStoreSink(e) {
  const root = await withTimeout(navigator.storage.getDirectory(), 2000)
  const dir = await root.getDirectoryHandle(storeName(e), { create: true })
  const fh = await dir.getFileHandle('data', { create: true })
  const w = await withTimeout(fh.createWritable({ keepExistingData: false }), 2000)
  const sink = fsSink(w)
  return {
    write: c => sink.write(c),
    close: async () => {
      await sink.close()
      saveBlob(await fh.getFile(), e.name)
    },
    seedFile: async () => fh.getFile(),
    abort: async () => {
      try { await sink.abort() } catch { /* */ }
      try { await root.removeEntry(storeName(e), { recursive: true }) } catch { /* */ }
    },
  }
}

async function makeSink(f) {
  if (TESTDUMP) return testSink(f.entry.ih, f.entry.name)
  if (f.saveHandle) {
    const w = await f.saveHandle.createWritable()
    return withSeed(fsSink(w), () => f.saveHandle.getFile())
  }
  if (st.dirHandle) {
    const rel = f.base ? f.entry.path.slice(f.base.length) : f.entry.name
    let h = st.dirHandle
    const seg = rel.split('/')
    for (let i = 0; i < seg.length - 1; i++) h = await h.getDirectoryHandle(seg[i], { create: true })
    const fh = await h.getFileHandle(seg[seg.length - 1], { create: true })
    const w = await fh.createWritable()
    return withSeed(fsSink(w), () => fh.getFile())
  }
  try {
    const s = await makeStoreSink(f.entry)
    if (s) return s
  } catch { /* OPFS/file:// 不可用 */ }
  if (f.entry.size > 32 << 20) return null
  return memSink(f.entry.name)
}

/* ---------- 启动 ---------- */

// 深链：#/路径（或路径本身是清单里的文件）触发一次下载；已有的不重存
function deepLink() {
  let want = decodeURIComponent((location.hash || '').replace(/^#\/?/, ''))
  if (!st.byPath.has(want)) {
    const pp = decodeURIComponent(location.pathname.replace(/^\/+|\/+$/g, ''))
    if (st.byPath.has(pp)) want = pp
  }
  if (!st.byPath.has(want)) return
  const e = st.byPath.get(want)
  if (!st.have.has(e.ih)) requestDownload([e.path], false)
}

async function load() {
  let entries
  try {
    // no-cache 协商缓存：内容没变服务端回 304，浏览器直接用本地副本
    const r = await fetch('/app/manifest.json', { cache: 'no-cache' })
    if (!r.ok) throw new Error('失败')
    entries = await r.json()
    // 清单是自家服务生成的，但坏了（半截 JSON/被代理改写）不能让它把启动链炸成
    // unhandled rejection——校验形状，坏了走同一条「清单不可用」提示
    if (!Array.isArray(entries) || !entries.every(e =>
      e && typeof e.path === 'string' && typeof e.name === 'string' &&
      typeof e.size === 'number' && typeof e.ih === 'string' && typeof e.torrent === 'string')) {
      throw new Error('格式不对')
    }
  } catch {
    render() // 先把空表格骨架画出来，错误进摘要行
    setMsg('清单不可用（服务未运行？）', 'err')
    return
  }
  st.entries = entries
  for (const e of entries) {
    st.byPath.set(e.path, e)
    st.files.set(e.path, { entry: e, tr: null, got: 0, saveHandle: null, base: '' })
  }
  sweepExpired() // 先清过期的（账本 + 池 + 块），再探谁还活着
  st.curDir = dirFromLocation()
  render()
  setInterval(updateSwarm, 3000)
  setInterval(sweepExpired, 30 * 60 * 1000) // 打开期间的保留期巡检
  updateSwarm()
  $('dl-dir').addEventListener('click', () => {
    if (!st.curDir) return
    if (canDirPicker()) requestFolder(st.curDir + '/')
    else location.href = location.pathname + '?zip=1' // 无目录选择能力 → 服务端 zip
  })
  // 探盘完成才知道谁是绿的：探完续种、重画一次、再处理深链
  probeHave().then(() => {
    reseedHave()
    render()
    deepLink()
    // 探盘（可能独占清过一轮）之后再持共享锁：本页存活期间别的标签开页拿不到独占锁，
    // 就不会把本页正在用的 in/ 中转或半截块清掉；页面一关锁自动释放
    if (navigator.locks) navigator.locks.request('share-active', { mode: 'shared' }, () => new Promise(() => {})).catch(() => {})
  })
}

bindFilter()
$('swarm-btn').addEventListener('click', openPanel)
$('panel-close').addEventListener('click', () => $('swarm-panel').close())
$('swarm-panel').addEventListener('click', e => { if (e.target === e.currentTarget) e.currentTarget.close() })
$('panel-clear').addEventListener('click', () => {
  if (!st.have.size) return
  if (!confirm('删除本机存储的全部 ' + st.have.size + ' 份文件？删除后可随时重新下载。')) return
  for (const ih of [...st.have]) deleteStored(ih)
  renderPanel()
  updateSwarm()
})
requestPersist() // 进站就申请持久化：别等第一次下载完成才护住块
load()

// 页面开着就是种：离开时对 tracker 发 stopped，池即散
function bye() {
  try { client.destroy() } catch { /* 已毁 */ }
}
window.addEventListener('beforeunload', bye)
window.addEventListener('pagehide', bye)
// bfcache 复活：client 已在 pagehide 毁掉，页面却原样回来了——重载走正常恢复路径
window.addEventListener('pageshow', e => { if (e.persisted) location.reload() })
