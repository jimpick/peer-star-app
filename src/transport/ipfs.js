'use strict'

const IPFS = require('ipfs')
const Libp2p = require('libp2p')
const WebSocketStar = require('libp2p-websocket-star')
const WebSockets = require('libp2p-websockets')
const Bootstrap = require('libp2p-bootstrap')
const Multiplex = require('libp2p-mplex')
const SECIO = require('libp2p-secio')
const get = require('lodash/get')
const AppTransport = require('./app-transport')
const Relay = require('./ipfs-relay')

module.exports = (app, options) => {
  const ipfsOptions = {
    repo: options && options.repo,
    EXPERIMENTAL: {
      pubsub: true
    },
    config: {
      Addresses: {
        Swarm: (options && options.swarm) || ['/dns4/ws-star1.par.dwebops.pub/tcp/443/wss/p2p-websocket-star']
      },
      Bootstrap: options.bootstrap
    },
    libp2p: createLibp2p
  }

  if (options.bootstrap) {
    ipfsOptions.config.Bootstrap = options.bootstrap
  }

  if (options.relay) {
    ipfsOptions.relay = {
      enabled: true, // enable relay dialer/listener (STOP)
      hop: {
        enabled: true // make this node a relay (HOP)
      }
    }
  }

  const ipfs = new IPFS(ipfsOptions)

  return ipfs

  function createLibp2p ({
    config,
    options: ipfsOptions,
    peerInfo,
    peerBook
  }) {
    const appTransport = AppTransport(app, ipfs, new WebSocketStar({ id: peerInfo.id }), options && options.transport)
    appTransport.on('error', (err) => app.emit('error', err))
    appTransport.on('peer connected', (peerInfo) => app.emit('peer connected', peerInfo))
    appTransport.on('outbound peer connected', (peerInfo) => app.emit('outbound peer connected', peerInfo))
    appTransport.on('inbound peer connected', (peerInfo) => app.emit('inbound peer connected', peerInfo))
    appTransport.on('peer disconnected', (peerInfo) => app.emit('peer disconnected', peerInfo))
    appTransport.on('outbound peer disconnected', (peerInfo) => app.emit('outbound peer disconnected', peerInfo))
    appTransport.on('inbound peer disconnected', (peerInfo) => app.emit('inbound peer disconnected', peerInfo))

    if (options && options.relay) {
      Relay(ipfs, appTransport, options.relay)
    }

    return new Libp2p({
      peerInfo,
      peerBook,
      modules: {
        transport: [ appTransport, WebSockets ],
        streamMuxer: [ Multiplex ],
        // connEncryption: [ SECIO ],
        peerDiscovery: [ appTransport.discovery, Bootstrap ]
      },
      config: {
        peerDiscovery: {
          bootstrap: {
            list: get(ipfsOptions, 'config.Bootstrap',
              get(config, 'Bootstrap', []))
          },
          websocketStar: {
            enabled: true
          }
        },
        relay: {
          enabled: get(ipfsOptions, 'relay.enabled',
            get(config, 'relay.enabled', false)),
          hop: {
            enabled: get(ipfsOptions, 'relay.hop.enabled',
              get(config, 'relay.hop.enabled', false)),
            active: get(ipfsOptions, 'relay.hop.active',
              get(config, 'relay.hop.active', false))
          }
        },
        EXPERIMENTAL: {
          dht: get(ipfsOptions, 'EXPERIMENTAL.dht', false),
          pubsub: get(ipfsOptions, 'EXPERIMENTAL.pubsub', false)
        }
      }
    })
  }
}
