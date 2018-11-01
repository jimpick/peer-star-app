'use strict'

const debug = require('debug')('peer-star:collaboration:connection-manager')
const debounce = require('lodash/debounce')
const EventEmitter = require('events')
const PeerSet = require('../common/peer-set')
const Protocol = require('./protocol')

module.exports = class ConnectionManager extends EventEmitter {
  constructor (ipfs, globalConnectionManager, ring, collaboration, store, clocks, options) {
    super()

    this._ipfs = ipfs
    this._globalConnectionManager = globalConnectionManager
    this._options = options

    if (!this._options.keys) {
      throw new Error('need options.keys')
    }

    this._stopped = true
    this._unreachables = new Map()

    this._ring = ring
    this._ring.on('changed', this._onRingChange.bind(this))

    this._inboundConnections = new PeerSet()
    this._outboundConnections = new PeerSet()

    this._protocol = Protocol(ipfs, collaboration, store, this._options.keys, clocks, options)

    this._protocol.on('inbound connection', (peerInfo) => {
      this._inboundConnections.add(peerInfo)
      this._ring.add(peerInfo)
    })

    this._protocol.on('inbound connection closed', (peerInfo) => {
      this._inboundConnections.delete(peerInfo)
    })

    this._protocol.on('outbound connection', (peerInfo) => {
      this._outboundConnections.add(peerInfo)
    })

    this._protocol.on('outbound connection closed', (peerInfo) => {
      this._outboundConnections.delete(peerInfo)
    })

    this._protocol.on('error', (err) => {
      collaboration.emit('error', err)
    })

    this._debouncedResetConnections = debounce(
      this._resetConnections.bind(this), this._options.debounceResetConnectionsMS)
  }

  async start (diasSet) {
    this._stopped = false
    this._diasSet = diasSet

    this._resetInterval = setInterval(() => {
      this._resetConnections()
    }, this._options.resetConnectionIntervalMS)

    await this._globalConnectionManager.handle(this._protocol.name(), this._protocol.handler)
  }

  stop () {
    // clearInterval(this._resetInterval)
    this._stopped = true
    if (this._resetInterval) {
      clearInterval(this._resetInterval)
      this._resetInterval = null
    }

    this._globalConnectionManager.unhandle(this._protocol.name())
  }

  observe (observer) {
    const onConnectionChange = () => {
      /*
      console.log('Jim collab/conn-man onConnectionChange')
      console.log('  Inbound collab:')
      for (let conn of this._inboundConnections.values()) {
        console.log('    ', conn.id.toB58String())
      }
      console.log('  Outbound collab:')
      for (let conn of this._outboundConnections.values()) {
        console.log('    ', conn.id.toB58String())
      }
      */
      observer.setInboundPeers(peerIdSetFromPeerSet(this._inboundConnections))
      observer.setOutboundPeers(peerIdSetFromPeerSet(this._outboundConnections))
    }

    this._protocol.on('inbound connection', onConnectionChange)
    this._protocol.on('inbound connection closed', onConnectionChange)
    this._protocol.on('outbound connection', onConnectionChange)
    this._protocol.on('outbound connection closed', onConnectionChange)

    const onInboundMessage = ({ fromPeer, size }) => {
      observer.inboundMessage(fromPeer, size)
    }
    this._protocol.on('inbound message', onInboundMessage)

    const onOutboundMessage = ({ toPeer, size }) => {
      observer.outboundMessage(toPeer, size)
    }
    this._protocol.on('outbound message', onOutboundMessage)

    // return unbind function
    return () => {
      this._protocol.removeListener('inbound connection', onConnectionChange)
      this._protocol.removeListener('inbound connection closed', onConnectionChange)
      this._protocol.removeListener('outbound connection', onConnectionChange)
      this._protocol.removeListener('outbound connection closed', onConnectionChange)
      this._protocol.removeListener('inbound message', onInboundMessage)
      this._protocol.removeListener('outbound message', onOutboundMessage)
    }
  }

  outboundConnectionCount () {
    return this._outboundConnections.size
  }

  outboundConnectedPeers () {
    return Array.from(this._outboundConnections.values()).map(peerInfoToPeerId)
  }

  outboundConnectedPeerInfos () {
    return new PeerSet(this._outboundConnections)
  }

  inboundConnectionCount () {
    return this._inboundConnections.size
  }

  inboundConnectedPeers () {
    return Array.from(this._inboundConnections.values()).map(peerInfoToPeerId)
  }

  vectorClock (peerId) {
    return this._protocol.vectorClock(peerId)
  }

  _onRingChange () {
    this._debouncedResetConnections()
  }

  _resetConnections () {
    return new Promise(async (resolve, reject) => {
      // console.log('Jim collab/conn-man _resetConnections ring', this._ring)
      // console.log('Jim collab/conn-man _resetConnections')
      const diasSet = this._diasSet(this._ring)

      // make sure we're connected to every peer of the Dias Peer Set
      for (let peerInfo of diasSet.values()) {
        /*
        console.log('Jim collab/conn-man _resetConnections',
          peerInfo.id.toB58String().slice(-3),
          this._outboundConnections.has(peerInfo)
        )
        */
        if (!this._outboundConnections.has(peerInfo)) {
          try {
            const self = this
            /*
            console.log('Jim collab/conn-man connecting to',
              peerInfo.id.toB58String(), peerInfo)
            peerInfo.multiaddrs.forEach(addr => {
              console.log('  ', addr.toString())
            })
            console.log('  Transports:',
              this._ipfs._libp2pNode._switch.availableTransports(peerInfo)
            )
            */
            if (peerInfo.multiaddrs.size === 0) {
              // This was added via the membership CRDT, so is missing
              // a multiaddr - try to resolve that and update ring
              console.error('Jim collab/conn-man missing multiaddrs',
                peerInfo.id.toB58String())
            } else {
              await makeConnection(peerInfo)
            }

            async function makeConnection (peerInfo) {
              const connection = await self._globalConnectionManager.connect(
                peerInfo, self._protocol.name())
              self._unreachables.delete(peerInfo.id.toB58String())
              self._protocol.dialerFor(peerInfo, connection)
              self.emit('connected', peerInfo)
              connection.once('closed', () => {
                setTimeout(() => {
                  self.emit('disconnected', peerInfo)
                }, 0)
              })
            }
          } catch (err) {
            console.error('Jim collab/conn-man connecting error', peerInfo.id.toB58String(), err)
            this._peerUnreachable(peerInfo)
            this._ring.remove(peerInfo)
            debug('error connecting:', err)
          }
        }
      }

      // make sure we disconnect from peers not in the Dias Peer Set
      for (let peerInfo of this._outboundConnections.values()) {
        if (!diasSet.has(peerInfo)) {
          try {
            this._globalConnectionManager.disconnect(peerInfo, this._protocol.name())
          } catch (err) {
            debug('error hanging up:', err)
          }
          this._unreachables.delete(peerInfo.id.toB58String())
        }
      }
    }).catch((err) => {
      console.error('error resetting connections:', err.message)
      debug('error resetting connections:', err)
    })
  }

  _peerUnreachable (peerInfo) {
    const peerId = peerInfo.id.toB58String()
    let count = (this._unreachables.get(peerId) || 0) + 1
    this._unreachables.set(peerId, count)
    if (this._options.maxUnreachableBeforeEviction <= count) {
      this._unreachables.delete(peerId)
      this._ring.remove(peerInfo)
      this.emit('should evict', peerInfo)
    }
  }
}

function peerInfoToPeerId (peerInfo) {
  return peerInfo.id.toB58String()
}

function peerIdSetFromPeerSet (peerSet) {
  return new Set(Array.from(peerSet.values()).map((peerInfo) => peerInfo.id.toB58String()))
}
