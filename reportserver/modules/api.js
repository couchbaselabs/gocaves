const bodyParser = require('body-parser')
const express = require('express')
const WebSocket = require('ws')
const { EventEmitter } = require('events')

class EventBus {
  constructor() {
    this._emitter = new EventEmitter()
  }

  publish(msg) {
    this._emitter.emit('msg', msg)
  }

  subscribe(fn) {
    this._emitter.on('msg', fn)
    return fn
  }

  unsubscribe(subId) {
    this._emitter.off('msg', subId)
  }
}

class LimitedArray {
  constructor(opts) {
    if (!opts) opts = {}
    if (!opts.limit) opts.limit = 10

    this._values = []
    this._limit = opts.limit
  }

  push(value) {
    this._values.push(value)
    while (this._values.length > this._limit) {
      this._values.shift()
    }
  }

  forEach(callback, thisArg) {
    this._values.forEach(callback, thisArg)
  }

  get length() {
    return this._values.length
  }
}

export default function ApiModule(moduleOptions) {
  const reportCache = new LimitedArray({ limit: 50 })
  const eventbus = new EventBus()
  const wss = new WebSocket.Server({ noServer: true })
  const app = express()
  const jsonParser = bodyParser.json()

  app.post('/publish_report', jsonParser, (req, resp) => {
    const reportData = req.body

    reportCache.push(reportData)
    eventbus.publish({
      type: 'new_report',
      report: reportData,
    })

    resp.json({ success: true })
  })

  wss.on('connection', (ws) => {
    ws.sendJson = (msg) => ws.send(JSON.stringify(msg))

    reportCache.forEach((reportData) => {
      ws.sendJson({
        type: 'new_report',
        report: reportData,
      })
    })

    const msgSub = eventbus.subscribe((msg) => {
      ws.sendJson(msg)
    })

    ws.on('close', () => {
      eventbus.unsubscribe(msgSub)
    })
  })

  this.addServerMiddleware({ path: '/api', handler: app })

  this.nuxt.hook('listen', (server) => {
    server.on('upgrade', (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws)
      })
    })
  })
}
