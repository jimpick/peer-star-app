'use strict'

const debug = require('debug')('peer-star:collaboration:store')
const EventEmitter = require('events')
const NamespaceStore = require('datastore-core').NamespaceDatastore
const Key = require('interface-datastore').Key
const Queue = require('p-queue')
const vectorclock = require('../common/vectorclock')
const leftpad = require('leftpad')
const pull = require('pull-stream')

function jimLogPurple (...args) {
  if (typeof window !== 'undefined') {
    console.log('%cJim store', 'color: white; background: purple', ...args)
  }
}

const { encode, decode } = require('delta-crdts-msgpack-codec')

module.exports = class CollaborationStore extends EventEmitter {
  constructor (ipfs, collaboration, options) {
    super()
    this._ipfs = ipfs
    this._collaboration = collaboration
    this._options = options

    this._cipher = options.createCipher

    this._queue = new Queue({ concurrency: 1 })

    this._shareds = []
  }

  async start () {
    this._store = await datastore(this._ipfs, this._collaboration)
    this._seq = await this.getSequence()
    this._id = (await this._ipfs.id()).id
  }

  setShared (shared) {
    this._shareds.push(shared)
  }

  findShared (name) {
    return this._shareds.find((shared) => shared.name === name)
  }

  stop () {
    // TO DO
  }

  async getSequence () {
    return (await this._get('/seq')) || 0
  }

  async getLatestClock () {
    return (await this._get('/clock')) || {}
  }

  _get (key) {
    return new Promise((resolve, reject) => {
      this._store.get(key, this._parsingResult((err, value) => {
        if (err) {
          return reject(err)
        }
        resolve(value)
      }))
    })
  }

  getClockAndStates () {
    return this._queue.add(() => Promise.all(
      [this.getLatestClock(), this.getStates()]))
  }

  saveDelta ([previousClock, authorClock, delta]) {
    return this._queue.add(async () => {
      debug('%s: save delta: %j', this._id, [previousClock, authorClock, delta])

      let currentClock
      if (!previousClock) {
        previousClock = currentClock = await this.getLatestClock()
      }

      if (!authorClock) {
        authorClock = {}
        authorClock[this._id] = 1
      }

      if (!currentClock) {
        currentClock = await this.getLatestClock()
      }

      if (!vectorclock.isIdentical(previousClock, currentClock)) {
        debug('%s: did not save because previous clock and current clock are not identical', this._id)
        debug('%s previous clock:', this._id, previousClock)
        debug('%s current clock:', this._id, currentClock)
        return false
      }

      const nextClock = vectorclock.merge(currentClock, vectorclock.incrementAll(previousClock, authorClock))
      debug('%s: next clock is', this._id, nextClock)

      const seq = this._seq = this._seq + 1
      const deltaKey = '/d:' + leftpad(seq.toString(16), 20)

      const deltaRecord = [previousClock, authorClock, delta]

      debug('%s: saving delta %j = %j', this._id, deltaKey, deltaRecord)
      jimLogPurple('save delta', deltaRecord)

      const newStateAndName = (await Promise.all(
        this._shareds.map((shared) => shared.apply(nextClock, delta, true)))).filter(Boolean)[0]

      const [name, newState] = newStateAndName || []

      const tasks = [
        this._save(deltaKey, deltaRecord),
        this._save('/clock', nextClock),
        this._save('/seq', seq)
      ]

      if (newStateAndName) {
        tasks.push(this._saveStateName(name))
        tasks.push(this._save('/state/' + name, newState))
      }

      debug('%s: new state is', this._id, newState)

      await Promise.all(tasks)

      debug('%s: saved delta %j', this._id, deltaKey)

      this._scheduleDeltaTrim()

      debug('%s: saved delta and vector clock', this._id)
      this.emit('delta', delta, nextClock)
      this.emit('clock changed', nextClock)
      this.emit('state changed', newState, nextClock)
      debug('%s: emitted state changed event', this._id)
      return nextClock
    })
  }

  async saveStates ([clock, states]) {
    debug('%s: saveStates', this._id, clock, states)
    return this._queue.add(async () => {
      const latest = await this.getLatestClock()
      debug('%s: latest vector clock:', this._id, latest)
      if (!clock) {
        clock = vectorclock.increment(latest, this._id)
        debug('%s: new vector clock is:', this._id, clock)
      } else {
        if (await this._contains(clock)) {
          // we have already seen this state change, so discard it
          return
        }
        clock = vectorclock.merge(latest, clock)
      }

      for (let state of states.values()) {
        await this._saveState(clock, state)
      }

      return clock
    })
  }

  async _saveState (clock, state) {
    if (!Buffer.isBuffer(state)) {
      throw new Error('state should be a buffer: ' + JSON.stringify(state))
    }

    debug('%s: save state', this._id, clock, state)
    // TODO: include parent vector clock
    // to be able to decide whether to ignore this state or not

    const newStateAndName = (await Promise.all(
      this._shareds.map((shared) => shared.apply(clock, state))))[0]

    if (!newStateAndName) {
      return
    }

    const [name, newState] = newStateAndName

    debug('%s: new merged state is %j', this._id, newState)

    await Promise.all([
      this._saveStateName(name),
      this._save('/state/' + name, newState),
      this._save('/clock', clock)])

    debug('%s: saved state and vector clock', this._id)
    this.emit('clock changed', clock)
    this.emit('state changed', newState)
    debug('%s: emitted state changed event', this._id)
    return clock
  }

  async getState (name) {
    if (!name) {
      name = null
    }
    return this._get('/state/' + name)
  }

  async getStates () {
    const stateNames = Array.from(await this._get('/stateNames') || new Set())
    const states = await Promise.all(stateNames.map((stateName) => this._get('/state/' + stateName)))
    return stateNames.reduce((acc, name, index) => {
      acc.set(name, states[index])
      return acc
    }, new Map())
  }

  deltaStream (_since = {}) {
    let since = Object.assign({}, _since)
    debug('%s: delta stream since %j', this._id, since)

    return pull(
      this._store.query({
        prefix: '/d:'
      }),
      pull.asyncMap(({ value }, cb) => this._decode(value, cb)),
      pull.map((d) => {
        debug('%s: delta stream candidate: %j', this._id, d)
        jimLogPurple('deltaStream candidate', d)
        return d
      }),
      pull.asyncMap((entireDelta, callback) => {
        const [previousClock, authorClock] = entireDelta
        if (vectorclock.isIdentical(previousClock, since)) {
          jimLogPurple('deltaStream stream', entireDelta)
          since = vectorclock.incrementAll(previousClock, authorClock)
          callback(null, entireDelta)
        } else {
          callback(null, null)
        }
      }),
      pull.filter(Boolean) // only allow non-null values
    )
  }

  deltaBatch (since = {}) {
    debug('%s: delta batch since %j', this._id, since)
    return new Promise((resolve, reject) => {
      pull(
        this.deltaStream(since),
        pull.reduce(
          (acc, delta) => {
            const encodedDelta = delta[2]
            const [forName] = decode(encodedDelta)
            const shared = this.findShared(forName)
            if (!shared) {
              reject(new Error('could not find share for name', forName))
              return
            }
            return shared.join(acc, delta)
          },
          this._shareds[0].initial(),
          (err, deltaBatch) => {
            if (err) {
              return reject(err)
            }

            deltaBatch
              .then(async (batch) => {
                for (let collabKey of batch.keys()) {
                  const collab = batch.get(collabKey)
                  const [name, type, clock, authorClock, state] = collab
                  const shared = this.findShared(collabKey)
                  const finalBatch = [clock, authorClock, encode([name, type, await shared.signAndEncrypt(encode(state))])]
                  batch.set(collabKey, finalBatch)
                }

                resolve(batch)
              })
              .catch((err) => reject(err))
          }
        )
      )
    })
  }

  contains (clock) {
    return this._queue.add(() => this._contains(clock))
  }

  async _contains (clock) {
    const currentClock = await this.getLatestClock()
    const result = (vectorclock.isIdentical(clock, currentClock) ||
      vectorclock.compare(clock, currentClock) < 0)
    debug('%s: (%j) contains (%j)? : ', this._id, currentClock, clock, result)
    return result
  }

  _save (key, value) {
    return this._encode(value || null)
      .then((encoded) => this._saveEncoded(key, encoded))
  }

  async _saveStateName (name) {
    const stateNames = await this._get('/stateNames') || new Set()
    if (!stateNames.has(name)) {
      stateNames.add(name)
      await this._save('/stateNames', stateNames)
    }
  }

  _saveEncoded (key, value) {
    return new Promise((resolve, reject) => {
      this._store.put(key, value, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  _scheduleDeltaTrim () {
    if (this._deltaTrimTimeout) {
      clearTimeout(this._deltaTrimTimeout)
    }
    this._deltaTrimTimeout = setTimeout(() => {
      this._deltaTrimTimeout = null
      if (this._trimmingDeltas) {
        return
      }
      this._trimDeltas()
    }, this._options.deltaTrimTimeoutMS)
  }

  _trimDeltas () {
    this._trimmingDeltas = true
    return new Promise((resolve, reject) => {
      const seq = this._seq
      const first = Math.max(seq - this._options.maxDeltaRetention, 0)
      pull(
        this._store.query({
          prefix: '/d:',
          keysOnly: true
        }),
        pull.map((d) => d.key),
        pull.asyncMap((key, callback) => {
          const thisSeq = parseInt(key.toString().slice(3), 16)
          if (thisSeq < first) {
            debug('%s: trimming delta with sequence %s', this._id, thisSeq)
            this._store.delete(key, callback)
          } else {
            callback()
          }
        }),
        pull.onEnd((err) => {
          this._trimmingDeltas = false
          if (err) {
            reject(err)
          } else {
            resolve()
          }
        })
      )
    })
  }

  _encode (value) {
    if (!this._cipher) {
      return Promise.resolve(encode(value))
    }
    return this._cipher().then((cipher) => {
      return new Promise((resolve, reject) => {
        cipher.encrypt(encode(value), (err, encrypted) => {
          if (err) {
            return reject(err)
          }
          resolve(encrypted)
        })
      })
    })
  }

  _decode (bytes, callback) {
    if (!this._cipher) {
      let decoded
      try {
        decoded = decode(bytes)
      } catch (err) {
        return callback(err)
      }
      return callback(null, decoded)
    }
    this._cipher().then((cipher) => {
      cipher.decrypt(bytes, (err, decrypted) => {
        if (err) {
          return callback(err)
        }
        const decoded = decode(decrypted)
        callback(null, decoded)
      })
    }).catch(callback)
  }

  _parsingResult (callback) {
    return (err, result) => {
      if (err) {
        if (isNotFoundError(err)) {
          return callback(null, undefined)
        }
        return callback(err)
      }
      this._decode(result, callback)
    }
  }
}

class SimpleDatastore {
  constructor () {
    this.store = new Map()
  }

  put (key, value, cb) {
    this.store.set(key, value)
    cb()
  }

  get (key, cb) {
    const value = this.store.get(key)
    if (typeof value === 'undefined') {
      return cb(new Error('Key not found'))
    }
    cb(null, value)
  }

  delete (key, cb) {
    this.store.delete(key)
    cb()
  }

  query (opts) {
    let result
    if (opts.keysOnly) {
      result = [...this.store.keys()]
        .filter(key => key.startsWith(opts.prefix))
        .map(key => ({key}))
    } else {
      result = [...this.store]
        .filter(([key, _]) => key.startsWith(opts.prefix))
        .map(([key, value]) => ({key, value}))
    }
    return pull.values(result)
  }
}

const dsMap = new Map()

function datastore (ipfs, collaboration) {
  return new Promise((resolve, reject) => {
    let ds = dsMap.get(collaboration.name)
    if (!ds) {
      ds = new SimpleDatastore()
      dsMap.set(collaboration.name, ds)
    }
    resolve(ds)
  })
}

function isNotFoundError (err) {
  return (
    err.message.indexOf('Key not found') >= 0 ||
    err.message.indexOf('No value') >= 0 ||
    err.code === 'ERR_NOT_FOUND'
  )
}
