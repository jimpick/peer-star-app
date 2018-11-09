'use strict'

const fs = require('fs')
const debug = require('debug')('peer-star:pinner')
const EventEmitter = require('events')
const Collaboration = require('../collaboration')
const IPFS = require('../transport/ipfs')
const CID = require('cids')
const PeerCountGuess = require('../peer-count-guess')
const { decode } = require('delta-crdts-msgpack-codec')
const persister = require('../persister')

const defaultOptions = {
  collaborationInnactivityTimeoutMS: 60000
}

class AppPinner extends EventEmitter {
  constructor (name, options) {
    super()
    this.name = name
    this._options = Object.assign({}, defaultOptions, options)
    this._peerCountGuess = new PeerCountGuess(this, options && options.peerCountGuess)
    this._collaborations = new Map()
    this._starting = null

    this._onGossipMessage = this._onGossipMessage.bind(this)
  }

  start () {
    if (this._starting) {
      return this._starting
    }

    this._starting = new Promise((resolve, reject) => {
      const ipfsOptions = (this._options && this._options.ipfs) || {}
      this.ipfs = IPFS(this, ipfsOptions)
      if (this.ipfs.isOnline()) {
        this.ipfs.on('error', (err) => this._handleIPFSError(err))
        resolve()
      } else {
        this.ipfs.once('ready', () => {
          this.ipfs.on('error', (err) => this._handleIPFSError(err))
          resolve()
        })
      }
    }).then(() => {
      this._peerCountGuess.start()
    })

    return this._starting
  }

  gossip (message) {
    if (this._gossip) {
      this._gossip.broadcast(message)
    }
  }

  setGossip (gossip) {
    this._gossip = gossip
    gossip.on('message', this._onGossipMessage)
  }

  setGlobalConnectionManager (globalConnectionManager) {
    this._globalConnectionManager = globalConnectionManager
    this.emit('global connection manager')
  }

  peerCountGuess () {
    return this._peerCountGuess.guess()
  }

  peerCountEstimate () {
    return this.peerCountGuess()
  }

  _onGossipMessage (message, cancel) {
    // debug('gossip message from %s', message.from)
    this.emit('gossip', message)
    let collaborationName, membership, type, timestamp, latency
    try {
      [collaborationName, membership, type, timestamp] = decode(message.data)
      if (!timestamp) {
        console.log(
          'Jim gossip from',
          message.from.slice(-3),
          '(ignored, no timestamp)'
        )
        cancel()
        return
      }
      latency = Math.abs(Date.now() - timestamp)
      if (latency >= 5000) {
        console.log(
          'Jim gossip from',
          message.from.slice(-3),
          latency,
          '(ignored, too old)'
        )
        cancel()
        return
      }
    } catch (err) {
      console.log('error parsing gossip message:', err)
      cancel()
      return
    }

    this.ipfs.id().then((peerInfo) => {
      if (message.from === peerInfo.id) {
        return
      }
      console.log(
        'Jim gossip from',
        message.from.slice(-3),
        latency
      )
      if (this._collaborations.has(collaborationName)) {
        const collaboration = this._collaborations.get(collaborationName)
        collaboration.deliverRemoteMembership(membership)
          .catch((err) => {
            console.error('error delivering remote membership:', err)
          })
      } else {
        debug('new collaboration %s of type %s', collaborationName, type)
        console.log(`New collaboration ${collaborationName} of type ${type}`)
        if (type) {
          const collaboration = this._addCollaboration(collaborationName, type)
          /*
          collaboration._store.start()
          .then(() => {
            return collaboration.start()
          })
          */
          collaboration.start()
          .then(() => {
            collaboration.deliverRemoteMembership(membership)

            const persist = persister(
              this.ipfs,
              collaborationName,
              collaboration._type,
              collaboration._store,
              {
                persistenceHeuristicOptions: {
                  maxDeltas: 100
                },
                naming: {
                  start: async () => {},
                  update: async () => {},
                  fetch: async () => {
                    const encoded = encodeURIComponent(collaborationName)
                    const filename = `head-cid.${encoded}.txt`
                    console.log('Jim fetching', filename)
                    try {
                      const cidText = fs.readFileSync(filename, 'utf8')
                      console.log('Jim', cidText)
                      return new CID(cidText)
                    } catch (e) {
                      console.log('Jim not found')
                      return
                    }
                  }
                },
                decryptAndVerify: async (data) => data,
                signAndEncrypt: async (data) => data
              }
            )
            persist.on('publish', cid => {
              // console.log('Jim publish', cid.toBaseEncodedString())
              const filename = `head-cid.${collaborationName}.txt`
              const encoded = encodeURIComponent(collaborationName)
              fs.writeFileSync(encoded, cid.toBaseEncodedString())
            })
            console.log('Jim starting...')
            persist.start(false)
            console.log('Jim fetching...')
            persist.fetchLatestState()
            .then(latest => {
              if (!latest) return false
              const {clock, state} = latest
              console.log('Jim fetched latest state')
              return collaboration._store.saveStates([clock, new Set([state])])
            })
            .then(merged => {
              if (!merged) return
              console.log('Jim state merged',
              collaboration.shared.value().join(''))
            })
          })
        }
      }
    })
  }

  _addCollaboration (name, type) {
    debug('adding collaboration %j of type %j', name, type)
    console.log(`Adding collaboration ${name} of type ${type}`)
    const options = {
      // replicateOnly: true
      keys: {},
      samplingIntervalMS: 5000,
      maxUnreachableBeforeEviction: 0
    }
    const collaboration = Collaboration(
      true,
      // false,
      this.ipfs,
      this._globalConnectionManager,
      this,
      name,
      type,
      options
    )
    this._collaborations.set(name, collaboration)

    collaboration.on('state changed', () => {
      console.log(`Doc ${name} updated:`)
      const lines = collaboration.shared.value().join('').split('\n')
      if (lines.length > 10) lines.length = 10
      console.log(lines.join('\n'))
    })

    const onInnactivityTimeout = () => {
      debug('collaboration %j timed out. Removing it...', name, type)
      collaboration.removeListener('state changed', onStateChanged)
      this._collaborations.delete(name)
      collaboration.stop().catch((err) => {
        console.error('error stopping collaboration ' + name + ':', err)
      })
    }

    let activityTimeout

    const resetActivityTimeout = () => {
      if (activityTimeout) {
        clearTimeout(activityTimeout)
      }
      // setTimeout(onInnactivityTimeout, this._options.collaborationInnactivityTimeoutMS)
    }

    const onStateChanged = () => {
      debug('state changed in collaboration %s', name)
      resetActivityTimeout()
    }

    collaboration.on('state changed', onStateChanged)

    resetActivityTimeout()

    return collaboration
  }

  _handleIPFSError (err) {
    console.error(err)
  }

  async stop () {
    try {
      await Promise.all(Array.from(this._collaborations.values()).map((collaboration) => collaboration.stop()))
    } catch (err) {
      console.error('error stopping collaborations:', err)
    }

    if (this._gossip) {
      this._gossip.removeListener('message', this._onGossipMessage)
    }
    this._collaborations.clear()
    this._peerCountGuess.stop()
    await this.ipfs.stop()
  }
}

module.exports = (appName, options) => {
  return new AppPinner(appName, options)
}
