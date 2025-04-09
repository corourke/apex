const express = require('express')
const http = require('http')
const WebSocket = require('ws')

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server })

// Broadcast messages to all connected clients
wss.on('connection', (ws) => {
  console.log('Client connected')
  ws.on('message', (message) => {
    // Convert Buffer to string if it’s a Buffer
    const messageString = Buffer.isBuffer(message) ? message.toString('utf8') : message
    console.log('Received message:', messageString)

    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(messageString) // Send as string
      }
    })
  })
  ws.on('close', () => console.log('Client disconnected'))
})

// Serve the React app
app.use(express.static('build')) // Assumes React app is built to 'build' folder

server.listen(8080, () => {
  console.log('Server listening on port 8080')
})
