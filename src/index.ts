import { Readable } from 'stream'
import EventEmitter from 'events'
import * as crypto from 'crypto'

export interface GetOpts {
  prefix?: string
}

export interface ReadStreamOpts {
  prefix?: string
  gt?: string
  gte?: string
  lt?: string
  lte?: string
  reverse?: boolean
}

export interface WriteOpts {
  prefix?: string
  writer?: SimulatedOplogIface
}

export type Clock = Map<string, number>

export interface SimulatedOp {
  seq: number
  clock: Clock
  op: 'put' | 'del'
  key: string
  value?: any
}

export interface SimulatedEntry {
  seq: number
  key: string
  value: any
  clock: Clock
  conflicts: SimulatedEntry[]
}

export interface SimulatedOplogIface extends EventEmitter {
  key: Buffer
  keyStr: string
  length: number
  writable: boolean
  get (seq: number): Promise<SimulatedOp|undefined>
}

export class SimulatedAutobee {
  length = 0
  key = crypto.randomBytes(32)
  writers: SimulatedOplogIface[] = []
  private clock: Clock = new Map()
  private entries: Map<string, any> = new Map()
  private history: Map<string, SimulatedOp[]> = new Map()

  get _genWritersClock () {
    const clock: Clock = new Map()
    for (const writer of this.writers) {
      clock.set(writer.key.toString('hex'), writer.length)
    }
    return clock
  }

  async _updateIndex () {
    const clock = new Map(this.clock)
    const dstClock = this._genWritersClock
    for (const writer of this.writers) {
      const writerKeyStr = writer.key.toString('hex')
      const oldSeq = clock.get(writerKeyStr) || 0
      const newSeq = dstClock.get(writerKeyStr) || 0
      for (let i = oldSeq; i < newSeq; i++) {
        const op = await writer.get(i)
        if (op) {
          if (op.op === 'put') {
            let conflicts = []
            const current = this.entries.get(op.key)
            if (current) {
              conflicts.push(current)
              if (current.conflicts.length) {
                conflicts = conflicts.concat(current.conflicts)
              }
            }
            conflicts = conflicts.filter(c => !leftDominatesRight(op.clock, c.clock))

            this.entries.set(op.key, {
              seq: this.length,
              key: op.key,
              value: op.value,
              clock: op.clock,
              conflicts
            })
          } else if (op.op === 'del') {
            this.entries.delete(op.key)
          }
          this.length++

          const history = this.history.get(op.key) || []
          history.push(op)
          this.history.set(op.key, history)
        }
      }
    }
    this.clock = dstClock
  }

  get _defaultWriter () {
    return this.writers.find(w => w.writable)
  }

  addWriter (writer: SimulatedOplogIface) {
    this.writers.push(writer)
    writer.on('append', this._updateIndex.bind(this))
  }

  get (key: string, opts?: GetOpts): Promise<SimulatedEntry|undefined> {
    if (opts?.prefix) {
      key = `${opts.prefix}/${key}`
    }
    return Promise.resolve(this.entries.get(key))
  }

  getHistory (key: string, opts?: GetOpts): Promise<SimulatedOp[]> {
    if (opts?.prefix) {
      key = `${opts.prefix}/${key}`
    }
    return Promise.resolve(this.history.get(key) || [])
  }

  createReadStream (opts?: ReadStreamOpts): Readable {
    opts = opts || {}
    if (opts.prefix) {
      if (opts.gt) opts.gt = `${opts.prefix}/${opts.gt}`
      else if (opts.gte) opts.gte = `${opts.prefix}/${opts.gte}`
      else opts.gte = `${opts.prefix}/\x00`
      if (opts.lt) opts.lt = `${opts.prefix}/${opts.lt}`
      else if (opts.lte) opts.lte = `${opts.prefix}/${opts.lte}`
      else opts.lte = `${opts.prefix}/\xff`
    }

    const arr: SimulatedEntry[] = []
    for (const [key, entry] of this.entries.entries()) {
      if (opts?.gt && key.localeCompare(opts.gt) <= 0) continue
      if (opts?.gte && key.localeCompare(opts.gte) < 0) continue
      if (opts?.lt && key.localeCompare(opts.lt) >= 0) continue
      if (opts?.lte && key.localeCompare(opts.lte) > 0) continue
      arr.push(entry)
    }
    arr.sort((a, b) => a.key.localeCompare(b.key))
    if (opts?.reverse) {
      arr.reverse()
    }

    const s = new Readable({objectMode: true})
    process.nextTick(() => {
      for (const entry of arr) {
        s.push(entry)
      }
      s.push(null)
    })
    return s
  }

  async put (key: string, value: any, opts?: WriteOpts): Promise<void> {
    if (opts?.prefix) {
      key = `${opts.prefix}/${key}`
    }
    const writer = opts?.writer || this._defaultWriter
    if (writer) {
      const clock = new Map(this.clock)
      clock.set(writer.keyStr, (clock.get(writer.keyStr) || 0) + 1)
      await (writer as SimulatedOplog).put(key, value, clock)
    } else {
      throw new Error('No writable oplog available')
    }
    return Promise.resolve(undefined)
  }

  async del (key: string, opts?: WriteOpts) {
    if (opts?.prefix) {
      key = `${opts.prefix}/${key}`
    }
    const writer = opts?.writer || this._defaultWriter
    if (writer) {
      const clock = new Map(this.clock)
      clock.set(writer.keyStr, (clock.get(writer.keyStr) || 0) + 1)
      await (writer as SimulatedOplog).del(key, clock)
    } else {
      throw new Error('No writable oplog available')
    }
    return Promise.resolve(undefined)
  }

  sub (prefix: string) {
    return new SimulatedAutobeeSub(this, prefix)
  }
}

export class SimulatedAutobeeSub {
  constructor (public db: SimulatedAutobee, public prefix: string) {
  }

  get (key: string, opts?: GetOpts) {
    opts = opts || {}
    opts.prefix = this.prefix
    return this.db.get(key, opts)
  }

  getHistory (key: string, opts?: GetOpts) {
    opts = opts || {}
    opts.prefix = this.prefix
    return this.db.getHistory(key, opts)
  }

  createReadStream (opts?: ReadStreamOpts) {
    opts = opts || {}
    opts.prefix = this.prefix
    return this.db.createReadStream(opts)
  }

  put (key: string, value: any, opts?: WriteOpts) {
    opts = opts || {}
    opts.prefix = this.prefix
    return this.db.put(key, value, opts)
  }

  del (key: string, opts?: WriteOpts) {
    opts = opts || {}
    opts.prefix = this.prefix
    return this.db.del(key, opts)
  }
}

export class SimulatedOplog extends EventEmitter implements SimulatedOplogIface {
  key = crypto.randomBytes(32)
  writable = true
  public ops: SimulatedOp[] = []
  constructor () {
    super()
  }

  get keyStr () {
    return this.key.toString('hex')
  }

  get length () {
    return this.ops.length
  }

  get (seq: number): Promise<SimulatedOp|undefined> {
    return Promise.resolve(this.ops[seq])
  }

  put (key: string, value: any, clock: Clock): Promise<void> {
    this.ops.push({
      seq: this.ops.length,
      clock,
      op: 'put',
      key,
      value
    })
    this.emit('append')
    return Promise.resolve(undefined)
  }

  del (key: string, clock: Clock): Promise<void> {
    this.ops.push({
      seq: this.ops.length,
      clock,
      op: 'del',
      key
    })
    this.emit('append')
    return Promise.resolve(undefined)
  }
}

export class SimulatedRemoteOplog extends EventEmitter implements SimulatedOplogIface {
  writable = false
  public isConnected = false
  private local: SimulatedOplog
  constructor (private remote: SimulatedOplog) {
    super()
    this.local = captureOplog(remote)
    this.remote.on('append', () => {
      if (this.isConnected) this.emit('append')
    })
  }

  get key () {
    return this.local.key
  }

  get keyStr () {
    return this.key.toString('hex')
  }

  connect () {
    this.isConnected = true
    if (this.local.length < this.remote.length) {
      this.emit('append')
    }
  }

  disconnect () {
    this.isConnected = false
    this.local = captureOplog(this.remote)
  }

  get oplog () {
    return this.isConnected ? this.remote : this.local
  }

  get length () {
    return this.oplog.length
  }

  get (seq: number): Promise<SimulatedOp|undefined> {
    return this.oplog.get(seq)
  }
}

function captureOplog (oplog: SimulatedOplog) {
  const capture = new SimulatedOplog()
  capture.key = oplog.key
  capture.ops = [...oplog.ops]
  return capture
}

function leftDominatesRight (left: Clock, right: Clock) {
  const keys = new Set([...left.keys(), ...right.keys()])
  for (const k of keys) {
    const lv = left.get(k) || 0
    const rv = right.get(k) || 0
    if (lv < rv) return false
  }
  return true
}