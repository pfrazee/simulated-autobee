import { Readable } from 'stream'

interface GetOpts {
  prefix?: string
}

interface ReadStreamOpts {
  prefix?: string
  gt?: string
  gte?: string
  lt?: string
  lte?: string
  reverse?: boolean
}

interface WriteOpts {
  prefix?: string
}

interface SimulatedEntry {
  seq: number
  key: string
  value: any
  conflicts: SimulatedEntry[]
}

export class SimulatedAutobee {
  private clock = 0
  private entries: Map<string, any> = new Map()

  get (key: string, opts?: GetOpts): Promise<SimulatedEntry|undefined> {
    if (opts?.prefix) {
      key = `${opts.prefix}/${key}`
    }
    return Promise.resolve(this.entries.get(key))
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

  put (key: string, value: any, opts?: WriteOpts): Promise<void> {
    if (opts?.prefix) {
      key = `${opts.prefix}/${key}`
    }
    this.entries.set(key, {
      seq: this.clock,
      key,
      value,
      conflicts: []
    })
    this.clock++
    return Promise.resolve(undefined)
  }

  del (key: string, opts?: WriteOpts) {
    if (opts?.prefix) {
      key = `${opts.prefix}/${key}`
    }
    this.entries.delete(key)
    this.clock++
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