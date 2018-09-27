'use strict'

const debug = require('debug')('peer-star:app-transport')
const EventEmitter = require('events')
const Ring = require('../common/ring')
const DiasSet = require('../common/dias-peer-set')
const PeerSet = require('../common/peer-set')
const ConnectionManager = require('./connection-manager')
const Gossip = require('./gossip')
const Discovery = require('./discovery')
const GlobalConnectionManager = require('./global-connection-manager')

const defaultOptions = {
  peerIdByteCount: 32,
  preambleByteCount: 2
}

module.exports = (...args) => new AppTransport(...args)

class AppTransport extends EventEmitter {
  constructor (app, ipfs, transport, options) {
    super()
    this._started = false
    this._ipfs = ipfs
    this._transport = transport
    this._app = app
    this._options = Object.assign({}, defaultOptions, options)

    this._ring = Ring(this._options.preambleByteCount)

    this._outboundConnections = new PeerSet()
    this._inboundConnections = new PeerSet()
    this.listeners = []

    this._onPeerDisconnect = this._onPeerDisconnect.bind(this)
    this._onPeerConnect = this._onPeerConnect.bind(this)

    this.discovery = new Discovery(
      this._appTopic(),
      this._ipfs,
      this._transport.discovery,
      this._ring,
      this._inboundConnections,
      this._outboundConnections,
      this._options)

    this.discovery.on('start', () => this._maybeStart())

    this._globalConnectionManager = new GlobalConnectionManager(ipfs, this)

    this._connectionManager = new ConnectionManager(
      this._globalConnectionManager,
      this._ring,
      this._inboundConnections,
      this._outboundConnections,
      this._options)

    this._connectionManager.on('peer', (peerInfo) => {
      this.discovery.emit('peer', peerInfo)
    })

    this._gossip = Gossip(app.name, ipfs)
    this._gossip.on('error', (err) => this.emit('error', err))
    this._app.setGossip(this._gossip)

    this._app.setGlobalConnectionManager(this._globalConnectionManager)
  }

  dial (ma, options, callback) {
    return this._transport.dial(ma, options, callback)
  }

  createListener (options, handler) {
    return this._transport.createListener(options, handler)
  }

  filter (multiaddrs) {
    return this._transport.filter(multiaddrs)
  }

  close (callback) {
    this._connectionManager.stop()
    this._globalConnectionManager.stop()
    this._ipfs._libp2pNode.removeListener('peer:disconnect', this._onPeerDisconnect)
    this._gossip.stop((err) => {
      if (err) {
        debug('error stopping gossip: ', err)
      }
      this._transport.close(callback)
    })
  }

  isOutbound (peerInfo) {
    return this._outboundConnections.has(peerInfo)
  }

  hasPeer (peerInfo) {
    return this._ring.has(peerInfo)
  }

  _maybeStart () {
    if (!this._started) {
      this._started = true
      this._start()
    }
  }

  _start () {
    this._startPeerId()
    this._gossip.start()
    this._connectionManager.start(this._diasSet)
    this._globalConnectionManager.start()
    this._ipfs._libp2pNode.on('peer:disconnect', this._onPeerDisconnect)
    this._ipfs._libp2pNode.on('peer:connect', this._onPeerConnect)
  }

  _startPeerId () {
    if (this._ipfs._peerInfo) {
      this._diasSet = DiasSet(this._options.peerIdByteCount, this._ipfs._peerInfo, this._options.preambleByteCount)
    } else {
      this._ipfs.once('ready', this._startPeerId.bind(this))
    }
  }

  _onPeerDisconnect (peerInfo) {
    debug('peer %s disconnected', peerInfo.id.toB58String())
    const isInbound = this._inboundConnections.has(peerInfo)
    const isOutbound = this._outboundConnections.has(peerInfo)
    if (isInbound) {
      this._inboundConnections.delete(peerInfo)
    }
    if (isOutbound) {
      this._outboundConnections.delete(peerInfo)
    }

    this._ring.remove(peerInfo)
    this.emit('peer disconnected', peerInfo)
    if (isInbound) {
      this.emit('inbound peer disconnected', peerInfo)
    }
    if (isOutbound) {
      this.emit('outbound peer disconnected', peerInfo)
    }
    console.log('Jim AppTransport _onPeerDisconnect', peerInfo.id.toB58String())
    console.log('  AppTransport* Inbound Connections:')
    for (let conn of this._inboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  AppTransport* Outbound Connections:')
    for (let conn of this._outboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  GCM Inbound:')
    for (let conn of this._globalConnectionManager._inbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  GCM Outbound:')
    for (let conn of this._globalConnectionManager._outbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
  }

  _onPeerConnect (peerInfo) {
    debug('peer %s connected', peerInfo.id.toB58String())
    this.emit('peer connected', peerInfo)
    if (this._outboundConnections.has(peerInfo)) {
      this.emit('outbound peer connected', peerInfo)
    } else {
      this._inboundConnections.add(peerInfo)
      this._ring.add(peerInfo)
      this.emit('inbound peer connected', peerInfo)
    }
    console.log('Jim AppTransport _onPeerConnect', peerInfo.id.toB58String())
    console.log('  AppTransport* Inbound Connections:')
    for (let conn of this._inboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  AppTransport* Outbound Connections:')
    for (let conn of this._outboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    /*
    if (!this._globalConnectionManager) {
      console.log('  GCM not connected yet.')
      return
    }
    */
    console.log('  GCM Inbound:')
    for (let conn of this._globalConnectionManager._inbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  GCM Outbound:')
    for (let conn of this._globalConnectionManager._outbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
  }

  _appTopic () {
    return this._app.name
  }
}
