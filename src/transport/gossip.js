'use strict'

const EventEmitter = require('events')
const debug = require('debug')('peer-star:gossip')

module.exports = (...args) => new Gossip(...args)

let oneTime = true

class Gossip extends EventEmitter {
  constructor (appName, ipfs) {
    super()
    this._appName = appName
    this._ipfs = ipfs

    this.start = this.start.bind(this)

    this._pubSubHandler = this._pubSubHandler.bind(this)
    this._propagateError = this._propagateError.bind(this)
  }

  start () {
    this._ipfs.pubsub.subscribe(this._appName, this._pubSubHandler, (err) => {
      if (err) {
        if (err.message.indexOf('not started yet') >= 0) {
          this._ipfs.once('ready', this.start)
        } else {
          this._propagateError(err)
        }
      }
    })
  }

  stop (callback) {
    this._ipfs.pubsub.unsubscribe(this._appName, this._pubSubHandler, callback)
  }

  broadcast (message) {
    debug('%s: broadcast', this._ipfs._peerInfo.id.toB58String(), message.toString())
    if (oneTime) {
      // console.log('Jim broadcast pubsub (one time)', message)
      this._ipfs.pubsub.publish(this._appName, message)
      // oneTime = false
    } else {
      // console.log('Jim broadcast pubsub (disabled)', message)
    }
  }

  _pubSubHandler (message) {
    this.emit('message', message)
  }

  _propagateError (err) {
    this.emit('error', err)
  }
}
