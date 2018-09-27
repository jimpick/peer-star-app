'use strict'

const EventEmitter = require('events')
const debounce = require('lodash/debounce')

const defaultOptions = {
  debounceResetConnectionsMS: 1000
}

module.exports = class ConnectionManager extends EventEmitter {
  constructor (globalConnectionManager, ring, inboundConnections, outboundConnections, options) {
    super()

    this._stopped = true
    this._globalConnectionManager = globalConnectionManager
    this._ring = ring
    this._inboundConnections = inboundConnections
    this._outboundConnections = outboundConnections
    this._options = Object.assign({}, defaultOptions, options)

    this._newPeers = []

    this._onRingChange = this._onRingChange.bind(this)
    this._ring.on('changed', this._onRingChange)

    this._debouncedResetConnectionsAndEmitNewPeers = debounce(
      this._resetConnectionsAndEmitNewPeers.bind(this), this._options.debounceResetConnectionsMS)
  }

  start (diasSet) {
    this._stopped = false
    this._diasSet = diasSet
  }

  stop () {
    this._stopped = true
  }

  _onRingChange (peerInfo) {
    console.log('Jim trans/conn-man _onRingChange', peerInfo)
    console.log('Jim ring:', this._ring)
    if (peerInfo) {
      this._newPeers.push(peerInfo)
    }
    this._debouncedResetConnectionsAndEmitNewPeers()
  }

  _resetConnectionsAndEmitNewPeers () {
    if (this._stopped) {
      return
    }

    const diasSet = this._resetConnections()
    const peers = this._newPeers
    this._newPeers = []
    peers.forEach((peerInfo) => {
      if (diasSet.has(peerInfo)) {
        this.emit('peer', peerInfo)
      }
    })
  }

  _resetConnections () {
    const diasSet = this._diasSet(this._ring)

    // make sure we're connected to every peer of the Dias Peer Set
    for (let peerInfo of diasSet.values()) {
      if (!this._outboundConnections.has(peerInfo)) {
        this._outboundConnections.add(peerInfo)
        // emitting a peer should be enough to make us connected
        // this event will be relayed to the discovery
        // which will make IPFS connect to the peer
        this.emit('peer', peerInfo)
      }
    }

    // make sure we disconnect from peers not in the Dias Peer Set
    for (let peerInfo of this._outboundConnections.values()) {
      if (!diasSet.has(peerInfo)) {
        this._globalConnectionManager.maybeHangUp(peerInfo)
      }
    }

    return diasSet
  }
}
