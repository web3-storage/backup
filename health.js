import http from 'node:http'

export function createHealthCheckServer ({ sourceDataFile, gracePeriodMs }) {
  // Track the timestamp of the last log line. Should be less than REPORT_INTERVAL
  let lastLogged = Date.now()
  const updateLastLogged = (timestamp = Date.now()) => { lastLogged = timestamp }
  const srv = http.createServer((_, res) => {
    const msSinceLastLog = Date.now() - lastLogged
    const status = msSinceLastLog > gracePeriodMs ? 500 : 200
    res.setHeader('Content-Type', 'application/json')
    res.writeHead(status)
    res.end(JSON.stringify({ status, msSinceLastLog, sourceDataFile }))
  })
  return { srv, updateLastLogged }
}
