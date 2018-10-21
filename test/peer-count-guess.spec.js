/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect

const bs58 = require('bs58')
const EventEmitter = require('events')

const randomPeerId = require('./utils/random-peer-id')
const PeerCountGuess = require('../src/peer-count-guess')

describe('peer count guess', () => {
  let app
  let guesser
  let peerCount = 10

  before(() => {
    app = new EventEmitter()
  })

  it('can be created', () => {
    guesser = new PeerCountGuess(app, {
      periodWindowMS: 4000
    })
  })

  it('can be started', () => guesser.start())

  it('gets some gossip', function (done) {
    this.timeout(20000)

    const peers = []
    for (let i = 0; i < peerCount; i++) {
      peers.push(randomPeerId())
    }
    const interval = setInterval(() => {
      const peerIndex = Math.floor(Math.random() * peers.length)
      const peer = peers[peerIndex]

      app.emit('gossip', {
        from: peer,
        message: Buffer.from('hello world!')
      })
    }, 50)

    setTimeout(() => {
      clearInterval(interval)
      expect(guesser.guess()).to.be.at.least(peerCount - 1)
      done()
    }, 19000)
  })

  it('can be stopped', () => guesser.stop())
})
