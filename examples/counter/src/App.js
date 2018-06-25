import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';

import PeerStarApp from 'peer-star-app'

class App extends Component {
  constructor () {
    super()
    this._app = PeerStarApp('peer-star-counter-example-app', {
      ipfs: {
        swarm: [ '/ip4/127.0.0.1/tcp/9090/ws/p2p-websocket-star' ]
      }
    })
    this._app.start()
      .then(() => {
        console.log('app started')
        this._app.collaborate('peer-star-counter-example')
          .then((collab) => {
            console.log('collaboration started')
            this._collab = collab
          })
      })
  }

  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h1 className="App-title">Welcome to Peer-Star Counter app</h1>
        </header>
        <p className="App-intro">
          Hello!
        </p>
      </div>
    );
  }
}

export default App;