/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const pair = require('pull-pair')
const MemoryDatastore = require('interface-datastore').MemoryDatastore
const crypto = require('libp2p-crypto')

const Store = require('../src/collaboration/store')
const Shared = require('../src/collaboration/shared')
const Protocol = require('../src/collaboration/protocol')
const Clocks = require('../src/collaboration/clocks')
const generateKeys = require('../src/keys/generate')

require('./utils/fake-crdt')
const Type = require('../src/collaboration/crdt')('fake')

const _storeOptions = {
  maxDeltaRetention: 0,
  deltaTrimTimeoutMS: 0
}

describe('collaboration protocol', function () {
  let keys
  let storeOptions
  const pusher = {}
  const puller = {}
  const pusher2 = {}
  const pusher3 = {}
  const puller2 = {}

  before(async () => {
    keys = await generateKeys()
  })

  before(() => {
    const key = crypto.randomBytes(16)
    const iv = crypto.randomBytes(16)
    storeOptions = Object.assign({}, _storeOptions, {
      createCipher: () => {
        return new Promise((resolve, reject) => {
          crypto.aes.create(Buffer.from(key), Buffer.from(iv), (err, key) => {
            if (err) {
              return reject(err)
            }
            resolve(key)
          })
        })
      }
    })
  })

  it('pusher can be created', async () => {
    const ipfs = {
      id () {
        return { id: 'pusher' }
      },
      _peerInfo: fakePeerInfoFor('pusher'),
      _repo: {
        datastore: new MemoryDatastore()
      }
    }
    const collaboration = { name: 'collaboration protocol test' }
    pusher.store = new Store(ipfs, collaboration, storeOptions)
    await pusher.store.start()
    const clocks = new Clocks()
    pusher.protocol = Protocol(ipfs, collaboration, pusher.store, null, clocks)
    pusher.shared = await Shared(null, 'pusher', Type, collaboration, pusher.store, keys)
    pusher.store.setShared(pusher.shared)
  })

  it('puller can be created', async () => {
    const ipfs = {
      id () {
        return { id: 'puller' }
      },
      _peerInfo: fakePeerInfoFor('puller'),
      _repo: {
        datastore: new MemoryDatastore()
      }
    }
    const collaboration = { name: 'collaboration protocol test' }
    puller.store = new Store(ipfs, collaboration, storeOptions)
    await puller.store.start()
    const clocks = new Clocks()
    puller.protocol = Protocol(ipfs, collaboration, puller.store, null, clocks, {
      receiveTimeout: 500
    })
    puller.shared = await Shared(null, 'puller', Type, collaboration, puller.store, keys)
    puller.store.setShared(puller.shared)
  })

  it('can connect pusher and puller', () => {
    const p1 = pair()
    const p2 = pair()

    const pullerStream = {
      source: p1.source,
      sink: p2.sink,
      getPeerInfo (cb) {
        setImmediate(() => cb(null, fakePeerInfoFor('pusher')))
      }
    }

    const pusherStream = {
      source: p2.source,
      sink: p1.sink
    }

    pusher.protocol.dialerFor(fakePeerInfoFor('puller'), pusherStream)
    puller.protocol.handler(null, pullerStream)
  })

  it('can save state locally', () => {
    return pusher.shared.add('a')
  })

  it('waits a bit', (done) => setTimeout(done, 500))

  it('puller got new state', () => {
    expect(puller.shared.value()).to.equal('a')
  })

  it('introduces another pusher', async () => {
    const ipfs = {
      id () {
        return { id: 'pusher 2' }
      },
      _peerInfo: fakePeerInfoFor('pusher 2'),
      _repo: {
        datastore: new MemoryDatastore()
      }
    }
    const collaboration = { name: 'collaboration protocol test' }
    pusher2.store = new Store(ipfs, collaboration, storeOptions)
    await pusher2.store.start()
    const clocks = new Clocks()
    pusher2.protocol = Protocol(ipfs, collaboration, pusher2.store, null, clocks)
    pusher2.shared = await Shared(null, 'pusher 2', Type, collaboration, pusher2.store, keys)
    pusher2.store.setShared(pusher2.shared)
  })

  it('connects new pusher to puller', () => {
    const p1 = pair()
    const p2 = pair()

    const pullerStream = {
      source: p1.source,
      sink: p2.sink,
      getPeerInfo (cb) {
        setImmediate(() => cb(null, fakePeerInfoFor('pusher 2')))
      }
    }

    const pusherStream = {
      source: p2.source,
      sink: p1.sink
    }

    pusher2.protocol.dialerFor(fakePeerInfoFor('puller'), pusherStream)
    puller.protocol.handler(null, pullerStream)
  })

  it('pusher2 can save state locally', () => {
    pusher2.shared.add('b')
  })

  it('waits a bit', (done) => setTimeout(done, 500))

  it('puller got new state', () => {
    expect(puller.shared.value()).to.equal('ab')
  })

  it('pusher1 can save state again', () => {
    pusher.shared.add('c')
  })

  it('waits a bit', (done) => setTimeout(done, 500))

  it('puller got new state', () => {
    expect(puller.shared.value()).to.equal('abc')
  })

  it('can create pusher from puller store', async () => {
    const ipfs = {
      id () {
        return { id: 'pusher from puller' }
      },
      _peerInfo: fakePeerInfoFor('pusher from puller'),
      _repo: {
        datastore: new MemoryDatastore()
      }
    }
    const collaboration = { name: 'collaboration protocol test' }
    pusher3.store = puller.store // same store as puller
    const clocks = new Clocks()
    pusher3.protocol = Protocol(ipfs, collaboration, pusher3.store, null, clocks)
    pusher3.shared = await Shared(null, 'pusher from puller', Type, collaboration, pusher3.store, keys)
    pusher3.store.setShared(pusher3.shared)
  })

  it('can create a fresh new puller', async () => {
    const ipfs = {
      id () {
        return { id: 'puller 2' }
      },
      _peerInfo: fakePeerInfoFor('puller 2'),
      _repo: {
        datastore: new MemoryDatastore()
      }
    }
    const collaboration = { name: 'collaboration protocol test' }
    puller2.store = new Store(ipfs, collaboration, storeOptions)
    await puller2.store.start()
    const clocks = new Clocks()
    puller2.protocol = Protocol(ipfs, collaboration, puller2.store, null, clocks, {
      receiveTimeout: 500
    })
    puller2.shared = await Shared(null, 'puller 2', Type, collaboration, puller2.store, keys)
    puller2.store.setShared(puller2.shared)
  })

  it('connects last two', () => {
    const p1 = pair()
    const p2 = pair()

    const pullerStream = {
      source: p1.source,
      sink: p2.sink,
      getPeerInfo (cb) {
        setImmediate(() => cb(null, fakePeerInfoFor('pusher from puller')))
      }
    }

    const pusherStream = {
      source: p2.source,
      sink: p1.sink
    }

    pusher3.protocol.dialerFor(fakePeerInfoFor('puller 2'), pusherStream)
    puller2.protocol.handler(null, pullerStream)
  })

  it('waits a bit', (done) => setTimeout(done, 500))

  it('newest puller got new state', () => {
    expect(puller2.shared.value()).to.equal('abc')
  })

  it('new data', () => {
    pusher2.shared.add('d')
    pusher.shared.add('e')
  })

  it('waits a bit', (done) => setTimeout(done, 500))

  it('puller eventually got the new state', () => {
    expect(puller.shared.value()).to.equal('abcde')
  })

  it('concurrencly, puller and pusher push new data', () => {
    pusher.shared.add('g')
    pusher2.shared.add('f')
  })

  it('waits a bit', (done) => setTimeout(done, 500))

  it('puller eventually got the new state', () => {
    expect(puller.shared.value()).to.equal('abcdefg')
  })
})

function fakePeerInfoFor (id) {
  return {
    id: {
      toB58String () {
        return id
      }
    }
  }
}

process.on('unhandledRejection', (err) => {
  console.error(err)
})
