'use strict'

const debug = require('debug')('peer-star:global-connection-manager')
const pull = require('pull-stream')
const EventEmitter = require('events')
const PeerSet = require('../common/peer-set')

module.exports = class GlobalConnectionManager {
  constructor (ipfs, appTransport) {
    this._ipfs = ipfs
    this._appTransport = appTransport

    this._peerCollaborations = new Map()
    this._outbound = new PeerSet()
    this._inbound = new PeerSet()

    this._onPeerConnect = this._onPeerConnect.bind(this)
    this._onPeerDisconnect = this._onPeerDisconnect.bind(this)
  }

  start () {
    this._ipfs._libp2pNode.on('peer:connect', this._onPeerConnect)
    this._ipfs._libp2pNode.on('peer:disconnect', this._onPeerDisconnect)
  }

  stop () {
    this._ipfs._libp2pNode.removeListener('peer:connect', this._onPeerConnect)
    this._ipfs._libp2pNode.removeListener('peer:disconnect', this._onPeerDisconnect)

    // TODO: disconnect all
  }

  connect (peerInfo, protocol) {
    return new Promise((resolve, reject) => {
      this._outbound.add(peerInfo)
      const peerId = peerInfo.id.toB58String()
      debug('connect', peerId, protocol)
      if (!this._peerCollaborations.has(peerId)) {
        this._peerCollaborations.set(peerId, new Set([protocol]))
      } else {
        this._peerCollaborations.get(peerId).add(protocol)
      }

      /*
      console.log('Jim dialProtocol', peerId, protocol, peerInfo.multiaddrs.size,
        this._ipfs._libp2pNode._switch.availableTransports(peerInfo))
      */
      this._ipfs._libp2pNode.dialProtocol(peerInfo, protocol, (err, conn) => {
        // console.log('Jim dialProtocol return', peerId, protocol, err)
        if (err) {
          this._outbound.delete(peerInfo)
          return reject(err)
        }

        if (!conn) {
          this._outbound.delete(peerInfo)
          return reject(new Error('could not connect'))
        }

        const retConn = Object.assign(new EventEmitter(), {
          sink: conn.sink,
          source: pull(
            conn.source,
            pull.through(null, (err) => {
              if (err && err.message !== 'underlying socket has been closed') {
                console.error('connection to %s ended with error', peerId, err.message)
                debug('connection to %s ended with error', peerId, err)
              }
              const peerCollaborations = this._peerCollaborations.get(peerId)
              peerCollaborations && peerCollaborations.delete(protocol)
              this.maybeHangUp(peerInfo)
              retConn.emit('closed', err)
            })
          )
        })

        resolve(retConn)
      })
    })
  }

  disconnect (peerInfo, protocol) {
    // TODO
    const peerId = peerInfo.id.toB58String()
    const collaborations = this._peerCollaborations.get(peerId)
    if (collaborations) {
      collaborations.delete(protocol)
    }

    // TODO: maybe GC peer conn
    this.maybeHangUp(peerInfo)
  }

  handle (protocol, handler) {
    return new Promise((resolve, reject) => {
      if (!this._ipfs._libp2pNode) {
        this._ipfs.once('ready', () => {
          this._ipfs._libp2pNode.handle(protocol, handler)
          resolve()
        })
        return
      }
      this._ipfs._libp2pNode.handle(protocol, handler)
      resolve()
    })
  }

  unhandle (protocol) {
    return this._ipfs._libp2pNode.unhandle(protocol)
  }

  _onPeerConnect (peerInfo) {
    // console.log('Jim GCM _onPeerConnect', peerInfo.id.toB58String())
    // if (!this._outbound.has(peerInfo) && !this._appTransport.isOutbound(peerInfo)) {
      // console.log('  Jim GCM _inbound add', peerInfo.id.toB58String())
      this._inbound.add(peerInfo)
    // }
    /*
    console.log('  AppTransport Inbound Connections:')
    for (let conn of this._appTransport._inboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  AppTransport Outbound Connections:')
    for (let conn of this._appTransport._outboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  GCM* Inbound:')
    for (let conn of this._inbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  GCM* Outbound:')
    for (let conn of this._outbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
    */
  }

  _onPeerDisconnect (peerInfo) {
    this._outbound.delete(peerInfo)
    this._inbound.delete(peerInfo)
    /*
    console.log('Jim GCM _onPeerDisconnect', peerInfo.id.toB58String())
    console.log('  AppTransport Inbound Connections:')
    for (let conn of this._appTransport._inboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  AppTransport Outbound Connections:')
    for (let conn of this._appTransport._outboundConnections.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  GCM* Inbound:')
    for (let conn of this._inbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
    console.log('  GCM* Outbound:')
    for (let conn of this._outbound.values()) {
      console.log('    ', conn.id.toB58String())
    }
    */
    const peerId = peerInfo.id.toB58String()
    this._peerCollaborations.delete(peerId)
  }

  maybeHangUp (peerInfo) {
    if (this._inbound.has(peerInfo) || this._appTransport.isOutbound(peerInfo)) {
      // either there's an inbound connection
      // or we are using at the app layer.
      // Either way let's not close it
      return
    }

    const peerId = peerInfo.id.toB58String()
    const dialedProtocols = this._peerCollaborations.get(peerId)
    const canClose = !dialedProtocols || !dialedProtocols.size
    if (canClose) {
      debug('hanging up %s', peerInfo.id.toB58String())
      try {
        this._ipfs._libp2pNode.hangUp(peerInfo, (err) => {
          if (err) {
            console.error('error hanging up:', err.message)
            debug('error hanging up:', err)
          }
        })
      } catch (err) {
        if (err.message !== 'The libp2p node is not started yet') {
          console.error('error hanging up:', err.message)
        }
        debug('error hanging up:', err)
      }
    }
  }
}
