'use strict'

const debug = require('debug')('peer-star:discovery')
const EventEmitter = require('events')
const Queue = require('p-queue')
const delay = require('delay')

const HAPPENS_ERRORS = [
  'The libp2p node is not started yet',
  'Stream ended prematurely'
]

const defaultOptions = {
  maxThrottleDelayMS: 5000
}

module.exports = class Discovery extends EventEmitter {
  constructor (appTopic, ipfs, discovery, ring, inboundConnections, outboundConnections, options) {
    super()

    this._options = Object.assign({}, defaultOptions, options)

    this._appTopic = appTopic
    this._discovery = discovery
    this._ring = ring
    this._inboundConnections = inboundConnections
    this._outboundConnections = outboundConnections
    this._ipfs = ipfs

    this._stopped = true

    this._queue = new Queue({concurrency: 1}) // TODO: make this an option
    this._peersPending = {}
    this._peersTesting = {}
    this._peersFailed = {}

    this._peerDiscovered = this._peerDiscovered.bind(this)
  }

  start (callback) {
    debug('starting discovery')
    this._stopped = false
    this._discovery.on('peer', this._peerDiscovered)
    this.emit('start')
    return this._discovery.start(callback)
  }

  stop (callback) {
    debug('stopping discovery')
    this._stopped = true
    this._discovery.removeListener('peer', this._peerDiscovered)
    this._queue.clear()
    this.emit('stop')
    return this._discovery.stop(callback)
  }

  _peerDiscovered (peerInfo) {
    const peerId = peerInfo.id.toB58String()
    if (
      !this._peersPending[peerId] &&
      !this._peersTesting[peerId] &&
      !this._peersFailed[peerId] &&
      !this._ring.has(peerInfo)
     ) {
      this._peersPending[peerId] = peerInfo
      console.log('Jim peer discovered', peerId.slice(-3))
      this._queue.add(() => this._maybeDiscoverOneRandomPeer())
    }
  }

  _maybeDiscoverOneRandomPeer () {
    const peerInfo = this._pickRandomPendingPeer()
    if (peerInfo) {
      const peerId = peerInfo.id.toB58String()
      console.log('Jim picked', peerId.slice(-3))
      if (this._ring.has(peerInfo)) {
        return Promise.resolve()
      }
      this._peersTesting[peerId] = peerInfo
      return this._throttledMaybeDiscoverPeer(peerInfo)
    }
  }

  _throttledMaybeDiscoverPeer (peerInfo) {
    return delay(this._delayTime())
      .then(() => this._maybeDiscoverPeer(peerInfo))
  }

  _maybeDiscoverPeer (peerInfo) {
    if (this._stopped) {
      return Promise.resolve()
    }

    if (this._ring.has(peerInfo)) {
      return Promise.resolve()
    }

    debug('maybe discover peer %j', peerInfo)

    return new Promise((resolve, reject) => {
      this._isInterestedInApp(peerInfo)
        .then((isInterestedInApp) => {
          if (isInterestedInApp) {
            const peerId = peerInfo.id.toB58String()
            debug('peer %s is interested', peerId)
            console.log('Jim add peer to ring', peerId.slice(-3))
            this._ring.add(peerInfo)
            delete this._peersTesting[peerId]
            resolve(peerInfo)
          } else {
            // peer is not interested. maybe disconnect?
            this._ipfs._libp2pNode.hangUp(peerInfo, (err) => {
              if (err) {
                reject(err)
              } else {
                resolve()
              }
            })
          }
        })
        .catch((err) => {
          this._maybeLogError(err)
          const peerId = peerInfo.id.toB58String()
          console.log('Jim failed peer', peerId.slice(-3))
          delete this._peersTesting[peerId]
          this._peersFailed[peerId] = Date.now()
          resolve()
        })
    })
  }

  _isInterestedInApp (peerInfo) {
    if (Buffer.isBuffer(peerInfo) || Array.isArray(peerInfo)) {
      return Promise.reject(new Error('needs peer info!'))
    }

    if (this._stopped) {
      return Promise.resolve()
    }

    // TODO: refactor this, PLEASE!
    return new Promise((resolve, reject) => {
      if (this._ring.has(peerInfo)) {
        return resolve()
      }

      const idB58Str = peerInfo.id.toB58String()

      debug('finding out whether peer %s is interested in app', idB58Str)
      console.log('Jim finding out whether peer %s is interested in app', idB58Str)

      if (!this._inboundConnections.has(peerInfo)) {
        this._outboundConnections.add(peerInfo)
      }

      console.log('Jim discovery dialing', idB58Str)
      this._ipfs._libp2pNode.dial(peerInfo, (err) => {
        if (err) {
          console.log('Jim dialed error', err)
          return reject(err)
        }

        debug('dialed %s', idB58Str)
        console.log('Jim dialed %s', idB58Str)

        // we're connected to the peer
        // let's wait until we know the peer subscriptions

        const pollTimeout = 2000 // TODO: this should go to config
        let tryUntil = Date.now() + 5000 // TODO: this should go to config

        const pollPeer = () => {
          debug('polling %s', idB58Str)
          console.log('Jim polling %s', idB58Str)
          this._ipfs.pubsub.peers(this._appTopic, (err, peers) => {
            if (err) {
              console.log('Jim polling error', err)
              return reject(err)
            }
            if (peers.indexOf(idB58Str) >= 0) {
              debug('peer %s is interested in app', idB58Str)
              console.log('Jim peer %s is interested in app', idB58Str)
              resolve(true)
            } else {
              debug('peer %s not subscribed to app', idB58Str)
              console.log('Jim peer %s not subscribed to app', idB58Str)
              maybeSchedulePeerPoll()
            }
          })
        }

        const maybeSchedulePeerPoll = () => {
          if (!this._stopped && (Date.now() < tryUntil)) {
            debug('scheduling poll to %s in %d ms', idB58Str, pollTimeout)
            setTimeout(pollPeer, pollTimeout)
          } else {
            console.log('Jim finished polling')
            resolve(false)
          }
        }

        maybeSchedulePeerPoll()
      })
    })
  }

  _delayTime () {
    // return 0
    return Math.floor(Math.random() * this._options.maxThrottleDelayMS) // TODO: make this value an option
  }

  _maybeLogError (err) {
    if (HAPPENS_ERRORS.indexOf(err.message) < 0) {
      if (err.message.match(/Circuit not enabled/)) return
      console.error('error caught while finding out if peer is interested in app', err)
    }
  }

  _pickRandomPendingPeer () {
    const keys = Object.keys(this._peersPending)
    const index = Math.floor(Math.random() * keys.length)
    const key = keys[index]
    const peer = this._peersPending[key]
    delete this._peersPending[key]
    return peer
  }
}
