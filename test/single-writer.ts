import ava from 'ava'
import concat from 'concat-stream'
import { SimulatedAutobee, SimulatedOplog } from '../src/index.js'

ava('single writer: get, createReadStream, put, del', async t => {
  const db = new SimulatedAutobee()
  db.addWriter(new SimulatedOplog())

  for (let i = 0; i < 10; i++) {
    await db.put(`key${i}`, {test: i})
    await db.sub('sub').put(`key${i}`, {test: i})
  }

  for (let i = 0; i < 10; i++) {
    const entry = await db.get(`key${i}`)
    t.truthy(entry)
    if (entry) {
      t.is(entry.key, `key${i}`)
      t.deepEqual(entry.value, {test: i})
      t.is(typeof entry.seq, 'number')
    }
  }

  for (let i = 0; i < 10; i++) {
    const entry = await db.sub('sub').get(`key${i}`)
    t.truthy(entry)
    if (entry) {
      t.is(entry.key, `sub/key${i}`)
      t.deepEqual(entry.value, {test: i})
      t.is(typeof entry.seq, 'number')
    }
  }

  {
    const res = await concatStream(db.createReadStream())
    for (let i = 0; i < 10; i++) {
      const entry = res[i]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
    for (let i = 0; i < 10; i++) {
      const entry = res[i + 10]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  {
    const res = await concatStream(db.sub('sub').createReadStream())
    for (let i = 0; i < 10; i++) {
      const entry = res[i]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  {
    const res = await concatStream(db.createReadStream({reverse: true}))
    for (let i = 0; i < 10; i++) {
      const entry = res[i]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${9 - i}`)
        t.deepEqual(entry.value, {test: 9 - i})
        t.is(typeof entry.seq, 'number')
      }
    }
    for (let i = 0; i < 10; i++) {
      const entry = res[i + 10]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${9 - i}`)
        t.deepEqual(entry.value, {test: 9 - i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  {
    const res = await concatStream(db.createReadStream({gt: 'key4', lt: 'key7'}))
    for (let i = 5; i < 7; i++) {
      const entry = res[i - 5]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  {
    const res = await concatStream(db.createReadStream({gt: 'key4', lt: 'key7', reverse: true}))
    t.is(res.length, 2)
    let i = 6
    for (const entry of res) {
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
      i--
    }
  }

  {
    const res = await concatStream(db.createReadStream({gte: 'key4', lte: 'key7'}))
    for (let i = 4; i <= 7; i++) {
      const entry = res[i - 4]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  {
    const res = await concatStream(db.createReadStream({gte: 'key4', lte: 'key7', reverse: true}))
    t.is(res.length, 4)
    let i = 7
    for (const entry of res) {
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
      i--
    }
  }

  {
    const res = await concatStream(db.sub('sub').createReadStream({gt: 'key4', lt: 'key7'}))
    for (let i = 5; i < 7; i++) {
      const entry = res[i - 5]
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  for (let i = 0; i < 10; i++) {
    if (i % 2 === 0) {
      await db.del(`key${i}`)
      await db.sub('sub').del(`key${i}`)
    }
  }

  for (let i = 0; i < 10; i++) {
    const entry = await db.get(`key${i}`)
    if (i % 2 === 0) {
      t.falsy(entry)
    } else {
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  for (let i = 0; i < 10; i++) {
    const entry = await db.sub('sub').get(`key${i}`)
    if (i % 2 === 0) {
      t.falsy(entry)
    } else {
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${i}`)
        t.deepEqual(entry.value, {test: i})
        t.is(typeof entry.seq, 'number')
      }
    }
  }
})

function concatStream (s: any): Promise<any[]> {
  return new Promise((resolve, reject) => {
    s.pipe(concat(v => {
      resolve(v as unknown as any[])
    }))
  })
}