// 浏览器垫片：webtorrent 及其依赖假定 node 全局存在
import process from 'process'
import { Buffer } from 'buffer'
globalThis.process = process
globalThis.Buffer = Buffer
export { process, Buffer }
