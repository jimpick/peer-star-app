'use strict'

const debug = require('debug')('peer-star:collaboration:membership')
const EventEmitter = require('events')
const multiaddr = require('multiaddr')
const multihashing = require('multihashing')
const PeerId = require('peer-id')
const PeerInfo = require('peer-info')
const bs58 = require('bs58')
const ORMap = require('delta-crdts')('ormap')
const Ring = require('../common/ring')
const DiasSet = require('../common/dias-peer-set')
const ConnectionManager = require('./connection-manager')
const MembershipGossipFrequencyHeuristic = require('./membership-gossip-frequency-henristic')
const { encode } = require('delta-crdts-msgpack-codec')

module.exports = class Membership extends EventEmitter {
  constructor (ipfs, globalConnectionManager, app, collaboration, store, clocks, options) {
    super()

    this._ipfs = ipfs
    this._app = app
    this._collaboration = collaboration
    this._options = options

    this._members = new Map()
    this._membershipGossipFrequencyHeuristic = new MembershipGossipFrequencyHeuristic(app, this, options)
    this._someoneHasMembershipWrong = false

    this._ring = Ring(this._options.preambleByteCount)
    this.connectionManager = new ConnectionManager(
      ipfs,
      globalConnectionManager,
      this._ring,
      this._collaboration,
      store,
      clocks,
      this._options)

    this._gossipNow = this._gossipNow.bind(this)

    this.connectionManager.on('should evict', (peerInfo) => {
      /*
      const peerId = peerInfo.id.toB58String()
      console.log('%s: evicting %s', this._id, peerId)
      this._memberCRDT.remove(peerId)
      this._members.delete(peerId)
      this.emit('peer left', peerInfo)
      this.emit('changed')
      */
    })
  }

  async start () {
    this._membershipGossipFrequencyHeuristic.on('gossip now', this._gossipNow)
    this._membershipGossipFrequencyHeuristic.start()
    await this._startPeerInfo()
  }

  async _startPeerInfo () {
    const pInfo = this._ipfs._peerInfo
    if (pInfo) {
      const peerId = pInfo.id.toB58String()
      this._id = peerId

      this._memberCRDT = ORMap(peerId)
      let addresses = pInfo.multiaddrs.toArray().map((ma) => ma.toString()).sort()
      if (addresses.length) {
        this._members.set(peerId, pInfo)
      }

      this._diasSet = DiasSet(
        this._options.peerIdByteCount, this._ipfs._peerInfo, this._options.preambleByteCount)
      await this.connectionManager.start(this._diasSet)
    } else {
      return new Promise((resolve, reject) => {
        this._ipfs.once('ready', () => {
          this._startPeerInfo().then(resolve).catch(reject)
        })
      })
    }
  }

  stop () {
    this._membershipGossipFrequencyHeuristic.stop()
    this._membershipGossipFrequencyHeuristic.removeListener('gossip now', this._gossipNow)
    this.connectionManager.stop()
  }

  peerCount () {
    return this._members.size
  }

  peers () {
    return new Set(this._members.keys())
  }

  peerAddresses (peerId) {
    const pInfo = this._members.get(peerId)
    return (pInfo && pInfo.multiaddrs.toArray().map((ma) => ma.toString())) || []
  }

  outboundConnectionCount () {
    return this.connectionManager.outboundConnectionCount()
  }

  outboundConnectedPeers () {
    return this.connectionManager.outboundConnectedPeers()
  }

  inboundConnectionCount () {
    return this.connectionManager.inboundConnectionCount()
  }

  inboundConnectedPeers () {
    return this.connectionManager.inboundConnectedPeers()
  }

  vectorClock (peerId) {
    return this.connectionManager.vectorClock(peerId)
  }

  needsUrgentBroadcast () {
    return this._someoneHasMembershipWrong
  }

  async deliverRemoteMembership (membership) {
    if ((typeof membership) === 'string') {
      const expectedMembershipHash = this._createMembershipSummaryHash()
      this._someoneHasMembershipWrong = (membership !== expectedMembershipHash)
    } else {
      await this._joinMembership(membership)
      const pInfo = this._ipfs._peerInfo
      const id = pInfo.id.toB58String()
      let addresses = pInfo.multiaddrs.toArray().map((ma) => ma.toString()).sort()
      const crdtAddresses = joinAddresses(this._memberCRDT.value()[id]).sort()
      this._someoneHasMembershipWrong = !addressesEqual(addresses, crdtAddresses)
    }
  }

  _gossipNow () {
    return this._ipfs.id()
      .then((peer) => peer.id)
      .then(async (id) => {
        const messages = []
        if (await this.needsUrgentBroadcast()) {
          this._ensurePeerIsInMembershipCRDT(id)
          messages.push(this._createMembershipMessage(id))
          messages.push(this._createMembershipSummaryMessage(id))
        } else {
          messages.push(this._createMembershipSummaryMessage(id))
        }
        this._someoneHasMembershipWrong = false
        messages.forEach((message) => this._app.gossip(message))
      })
  }

  _createMembershipSummaryMessage (selfId) {
    const message = [
      this._membershipTopic(),
      this._createMembershipSummaryHash(),
      this._collaboration.typeName]
    return encode(message)
  }

  _createMembershipSummaryHash () {
    const membership = Array.from(this._members).map(
      ([peerId, pInfo]) => [peerId, pInfo.multiaddrs.toArray().map((ma) => ma.toString()).sort()]).sort(sortMembers)
    debug('%s: membership:', this._ipfs._peerInfo.id.toB58String(), membership)
    return multihashing.digest(
      Buffer.from(JSON.stringify(membership)),
      'sha1').toString('base64')
  }

  _createMembershipMessage (selfId) {
    debug('sending membership', this._memberCRDT.value())
    const message = [this._membershipTopic(), this._memberCRDT.state(), this._collaboration.typeName]
    // TODO: sign and encrypt membership message
    return encode(message)
  }

  _ensurePeerIsInMembershipCRDT (id) {
    const pInfo = this._ipfs._peerInfo
    let addresses = pInfo.multiaddrs.toArray().map((ma) => ma.toString()).sort()
    const crdtAddresses = joinAddresses(this._memberCRDT.value()[id]).sort()
    if (!addressesEqual(addresses, crdtAddresses)) {
      this._members.set(id, pInfo)
      this._memberCRDT.applySub(id, 'mvreg', 'write', addresses)
    }
  }

  _joinMembership (remoteMembership) {
    return this._ipfs.id()
      .then((peer) => peer.id)
      .then((id) => {
        if (this._memberCRDT) {
          let changed = false
          debug('remote membership:', remoteMembership)
          this._memberCRDT.apply(remoteMembership)
          const members = new Map(Object.entries(this._memberCRDT.value()))
          const oldMembers = new Set(this._members.keys())
          debug('local members:', oldMembers)

          const pInfo = this._ipfs._peerInfo
          let myAddresses = pInfo.multiaddrs.toArray().map((ma) => ma.toString()).sort()
          const newAddresses = joinAddresses(members[id])

          if (addressesEqual(myAddresses, newAddresses)) {
            this._memberCRDT.applySub(id, 'mvreg', 'write', myAddresses)
            this._someoneHasMembershipWrong = true
          }

          for (let [peerId, addresses] of members) {
            if (peerId === id) { continue }
            addresses = joinAddresses(addresses)
            debug('remote addresses for %s:', peerId, addresses)

            const oldPeerInfo = this._members.has(peerId) && this._members.get(peerId)
            if (!oldPeerInfo) {
              const peerInfo = new PeerInfo(new PeerId(bs58.decode(peerId)))
              for (let address of addresses) {
                peerInfo.multiaddrs.add(address)
              }
              this._members.set(peerId, peerInfo)
              this._ring.add(peerInfo)
              changed = true
              this.emit('peer joined', peerId)
            } else {
              const oldAddresses = oldPeerInfo.multiaddrs.toArray().map((ma) => ma.toString()).sort()
              debug('local addresses for %s:', peerId, oldAddresses)
              for (let address of addresses) {
                if (!oldPeerInfo.multiaddrs.has(address)) {
                  changed = true
                  oldPeerInfo.multiaddrs.add(address)
                }
              }

              for (let address of oldAddresses) {
                if (addresses.indexOf(address) < 0) {
                  changed = true
                  oldPeerInfo.multiaddrs.delete(address)
                }
              }
              this.emit('peer addresses changed', peerId, addresses)
            }
          }

          for (let oldMember of oldMembers) {
            if (oldMember === id) { continue }
            if (!members.has(oldMember)) {
              this._members.delete(oldMember)
              this._ring.remove(new PeerInfo(new PeerId(bs58.decode(oldMember))))
              changed = true
              this.emit('peer left', oldMember)
            }
          }

          if (changed) {
            debug('MEMBERSHIP CHANGED!')
            this.emit('changed')
          }
        }
      })
  }

  _membershipTopic () {
    return this._collaboration.name
  }
}

function sortMembers (member1, member2) {
  const [id1] = member1
  const [id2] = member2
  if (id1 < id2) {
    return -1
  } else if (id1 > id2) {
    return 1
  }
  return 0
}

function joinAddresses (addresses) {
  debug('joinAddresses:', addresses)
  return (Array.from(addresses || [])).reduce((acc, moreAddresses) => acc.concat(moreAddresses), [])
}

function addressesEqual (addresses1, addresses2) {
  if (addresses1.length !== addresses2.length) {
    return false
  }
  for (let address of addresses1) {
    if (addresses2.indexOf(address) < 0) {
      return false
    }
  }
  for (let address of addresses2) {
    if (addresses1.indexOf(address) < 0) {
      return false
    }
  }

  return true
}
