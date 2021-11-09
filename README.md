# Paul's Simulated Autobee

I created this to work on some applications while Autobee finishes getting written. It probably won't be useful to most people.

```
npm i pauls-simulated-autobee
```

```js
import { SimulatedAutobee, SimulatedOplog, SimulatedRemoteOplog } from 'pauls-simulated-autobee'

// Create an autobee with a single writer
const db = new SimulatedAutobee()
db.addWriter(new SimulatedOplog())

// Create another autobee with its own writer
const db2 = new SimulatedAutobee()
db2.addWriter(new SimulatedOplog())

// Add them as writers to each other
db.addWriter(new SimulatedRemoteOplog(db2.writers[0]))
db2.addWriter(new SimulatedRemoteOplog(db.writers[0]))

// "Connect" them
db.writers[1].connect()
db2.writers[1].connect()

// "Disconnect" them
db.writers[1].disconnect()
db2.writers[1].disconnect()

// the rest of the API works like Hyperbee except there's also a `conflicts` array value on gets
```