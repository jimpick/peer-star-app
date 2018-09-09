'use strict'

const CRDT = require('@jimpick/delta-crdts')
const uniq = require('lodash.uniq')

const fake = module.exports = (id) => ({
  initial: () => new Set(),
  join (s1, s2) {
    const all = Array.from(s1).concat(Array.from(s2))
    return new Set(all)
  },
  value: (s) => Array.from(s).sort().join(''),
  mutators: {
    add: (s, str) => {
      return new Set(str)
    }
  }
})

CRDT.define('fake', fake)
