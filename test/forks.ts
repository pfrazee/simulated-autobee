import ava from 'ava'
import { SimulatedAutobee, SimulatedOplog, SimulatedRemoteOplog } from '../src/index.js'

import { inspect } from 'util'

ava('dual remote writers: get, createReadStream, put, del', async t => {
  const db1 = new SimulatedAutobee()
  db1.addWriter(new SimulatedOplog())
  const db2 = new SimulatedAutobee()
  db2.addWriter(new SimulatedOplog())
  const dbs = [db1, db2]

  db1.addWriter(new SimulatedRemoteOplog(db2.writers[0] as SimulatedOplog))
  db2.addWriter(new SimulatedRemoteOplog(db1.writers[0] as SimulatedOplog))

  // issue writes while disconnected

  for (let dbi = 0; dbi < 2; dbi++) {
    const db = dbs[dbi]
    for (let i = 0; i < 10; i++) {
      await db.put(`key${i}`, `writer${dbi}`)
      await db.sub('sub').put(`key${i}`, `writer${dbi}`)
    }
  }

  // disconnected reads are isolated

  for (let dbi = 0; dbi < 2; dbi++) {
    const db = dbs[dbi]
    for (let i = 0; i < 10; i++) {
      const entry = await db.get(`key${i}`)
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.deepEqual(entry.value, `writer${dbi}`)
        t.is(typeof entry.seq, 'number')
      }
    }

    for (let i = 0; i < 10; i++) {
      const entry = await db.sub('sub').get(`key${i}`)
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${i}`)
        t.deepEqual(entry.value, `writer${dbi}`)
        t.is(typeof entry.seq, 'number')
      }
    }
  }

  // connect the DBs

  ;(db1.writers[1] as SimulatedRemoteOplog).connect()
  ;(db2.writers[1] as SimulatedRemoteOplog).connect()

  await new Promise(r => setTimeout(r, 500))

  // connected reads are now in conflict
  
  for (let dbi = 0; dbi < 2; dbi++) {
    const db = dbs[dbi]
    for (let i = 0; i < 10; i++) {
      const entry = await db.get(`key${i}`)
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.truthy(['writer1', 'writer0'].includes(entry.value))
        t.is(typeof entry.seq, 'number')
        t.is(entry.conflicts.length, 1)
        t.truthy(['writer1', 'writer0'].includes(entry.conflicts[0].value))
      }
    }

    for (let i = 0; i < 10; i++) {
      const entry = await db.sub('sub').get(`key${i}`)
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${i}`)
        t.truthy(['writer1', 'writer0'].includes(entry.value))
        t.is(typeof entry.seq, 'number')
        t.is(entry.conflicts.length, 1)
        t.truthy(['writer1', 'writer0'].includes(entry.conflicts[0].value))
      }
    }
  }

  // merging writes

  for (let i = 0; i < 10; i++) {
    await db1.put(`key${i}`, `writer0`)
    await db1.sub('sub').put(`key${i}`, `writer0`)
  }

  // reads are no longer in conflict
  
  for (let dbi = 0; dbi < 2; dbi++) {
    const db = dbs[dbi]
    for (let i = 0; i < 10; i++) {
      const entry = await db.get(`key${i}`)
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `key${i}`)
        t.is(entry.value, 'writer0')
        t.is(typeof entry.seq, 'number')
        t.is(entry.conflicts.length, 0)
      }
    }

    for (let i = 0; i < 10; i++) {
      const entry = await db.sub('sub').get(`key${i}`)
      t.truthy(entry)
      if (entry) {
        t.is(entry.key, `sub/key${i}`)
        t.is(entry.value, 'writer0')
        t.is(typeof entry.seq, 'number')
        t.is(entry.conflicts.length, 0)
      }
    }
  }
})

