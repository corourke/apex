import React, { useState, useEffect } from 'react'
import { Tab, Tabs } from 'react-bootstrap'
import { Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import 'bootstrap/dist/css/bootstrap.min.css'

// Register Chart.js components
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend)

// Singleton WebSocket instance
let wsSingleton = null

function App() {
  const [state, setState] = useState({
    timezones: {},
    history: [],
    totalDataVolume: 0,
    totalTransactions: 0,
  })

  useEffect(() => {
    console.log('Component mounted, setting up WebSocket handlers')

    let reconnectTimeout

    const connectWebSocket = () => {
      // Only create a new WebSocket if none exists or the existing one is closed
      if (!wsSingleton || wsSingleton.readyState === WebSocket.CLOSED) {
        wsSingleton = new WebSocket('ws://localhost:8080')
        console.log('Initializing WebSocket...')

        wsSingleton.onopen = () => {
          console.log('Connected to WebSocket server')
          clearTimeout(reconnectTimeout) // Clear any pending reconnect attempts
        }

        wsSingleton.onmessage = (event) => {
          console.log('Raw message received:', event.data)
          try {
            const message = JSON.parse(event.data)
            console.log('Parsed message:', message)
            setState((prevState) => {
              const newTimezones = { ...prevState.timezones }
              let newHistory = [...prevState.history]
              let newTotalDataVolume = prevState.totalDataVolume
              let newTotalTransactions = prevState.totalTransactions

              switch (message.type) {
                case 'start':
                  if (!newTimezones[message.timezone]) {
                    newTimezones[message.timezone] = { cumulativeCount: 0 }
                  }
                  newTimezones[message.timezone].isRunning = true
                  newTimezones[message.timezone].localTime = message.localTime
                  console.log('START:', message.timezone, message.localTime)
                  break
                case 'stop':
                  if (newTimezones[message.timezone]) {
                    newTimezones[message.timezone].isRunning = false
                    newTimezones[message.timezone].localTime = message.localTime
                  }
                  console.log('STOP:', message.timezone, message.localTime)
                  break
                case 'scans':
                  if (!newTimezones[message.timezone]) {
                    newTimezones[message.timezone] = { cumulativeCount: 0, isRunning: true }
                  }
                  newTimezones[message.timezone].lastBatchTime = message.timestamp
                  newTimezones[message.timezone].lastBatchCount = message.batchCount
                  newTimezones[message.timezone].cumulativeCount += message.batchCount
                  newTimezones[message.timezone].localTime = message.localTime
                  newHistory.push({ timestamp: message.timestamp, batchCount: message.batchCount })
                  newTotalDataVolume += message.dataVolume
                  newTotalTransactions += message.batchCount
                  console.log('SCANS:', message.timezone, message.batchCount, message.dataVolume)
                  break
                default:
                  console.log('Unknown message type:', message.type)
                  break
              }

              return {
                timezones: newTimezones,
                history: newHistory,
                totalDataVolume: newTotalDataVolume,
                totalTransactions: newTotalTransactions,
              }
            })
          } catch (error) {
            console.error('Failed to parse message:', error, event.data)
          }
        }

        wsSingleton.onerror = (error) => {
          console.error('WebSocket error:', error)
        }

        wsSingleton.onclose = (event) => {
          console.log('WebSocket connection closed:', event.code, event.reason)
          // Attempt reconnection after a delay if not already reconnecting
          if (!reconnectTimeout) {
            reconnectTimeout = setTimeout(() => {
              connectWebSocket()
            }, 1000)
          }
        }
      } else if (wsSingleton.readyState === WebSocket.CONNECTING) {
        console.log('WebSocket already connecting, skipping reinitialization')
      } else if (wsSingleton.readyState === WebSocket.OPEN) {
        console.log('WebSocket already open, reusing connection')
      }
    }

    // Initial WebSocket connection
    connectWebSocket()

    return () => {
      console.log('Component unmounting')
      // Only clear reconnect timeout, don’t close the WebSocket to maintain singleton
      clearTimeout(reconnectTimeout)
    }
  }, [])

  const chartData = {
    labels: state.history.map((h) => h.timestamp),
    datasets: [
      {
        label: 'Total Transaction Count',
        data: state.history.map((h, i) =>
          state.history.slice(0, i + 1).reduce((sum, curr) => sum + curr.batchCount, 0)
        ),
        borderColor: 'rgba(75, 192, 192, 1)',
        fill: false,
      },
    ],
  }

  return (
    <div className="container mt-4">
      <h1 className="mb-4">APEX Store Activity</h1>
      <Tabs defaultActiveKey="map" className="mb-3">
        <Tab eventKey="map" title="Map">
          <div
            className="p-3"
            style={{
              position: 'relative',
              width: '100%',
              maxWidth: '100vw', // Limit to 90% of viewport width
              maxHeight: '80vh', // Limit to 80% of viewport height
              overflow: 'hidden',
            }}
          >
            <img
              src="/us-timezones.png"
              alt="US Timezone Map"
              style={{
                width: '80vw',
                height: '80vh',
                objectFit: 'contain',
                display: 'block',
                margin: '0 auto',
              }}
            />
            {[
              'EST (GMT-05)',
              'CST (GMT-06)',
              'MST (GMT-07)',
              'PST (GMT-08)',
              'AKST (GMT-09)',
              'HST (GMT-10)',
            ].map((tz) => {
              const data = state.timezones[tz] || {
                cumulativeCount: 0,
                lastBatchCount: 0,
                localTime: 'N/A',
              }
              return (
                <div
                  key={tz}
                  style={{
                    position: 'absolute',
                    backgroundColor: 'rgba(255, 255, 255, 0.8)',
                    padding: '5px',
                    border: '1px solid #000',
                    borderRadius: '6px',
                    top: tz === 'HST (GMT-10)' ? '65%' : tz === 'AKST (GMT-09)' ? '30%' : '30%',
                    left:
                      tz === 'AKST (GMT-09)'
                        ? '10%'
                        : tz === 'EST (GMT-05)'
                        ? '78%'
                        : tz === 'CST (GMT-06)'
                        ? '60%'
                        : tz === 'MST (GMT-07)'
                        ? '43%'
                        : tz === 'PST (GMT-08)'
                        ? '28%'
                        : '20%',
                  }}
                >
                  <strong>{tz}</strong>
                  <br />
                  Cumulative: {data.cumulativeCount}
                  <br />
                  Last Batch: {data.lastBatchCount || 0}
                  <br />
                  Time: {data.localTime}
                </div>
              )
            })}
          </div>
        </Tab>

        <Tab eventKey="overview" title="Overview">
          <div className="p-3">
            <p>
              <strong>Total Cumulative Data Volume:</strong> {state.totalDataVolume} bytes
            </p>
            <p>
              <strong>Total Cumulative Transactions:</strong> {state.totalTransactions}
            </p>
            <Line data={chartData} options={{ responsive: true }} />
          </div>
        </Tab>
      </Tabs>
    </div>
  )
}

export default App
