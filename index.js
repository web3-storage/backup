import debug from 'debug'
import { Readable, Writable } from 'stream'
import { createReadStream, createWriteStream } from 'fs'
import { HeadObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import retry from 'p-retry'
import { transform } from 'streaming-iterables'
import { parse } from 'it-ndjson'
import { pipe } from 'it-pipe'
import formatNumber from 'format-number'
import { CID } from 'multiformats'
import { IpfsClient } from './ipfs-client.js'
import { createHealthCheckServer } from './health.js'

const fmt = formatNumber()

const CONCURRENCY = 5
const BLOCK_TIMEOUT = 1000 * 30 // timeout if we don't receive a block after 30s
const REPORT_INTERVAL = 1000 * 60 // log download progress every minute

/** @typedef {{ cid: string, pinned_peers: string[] }} InputData */

/**
 * @param {Object} config
 * @param {string|URL} config.dataURL
 * @param {string} config.s3Region S3 region.
 * @param {string} config.s3BucketName S3 bucket name.
 * @param {string} [config.s3AccessKeyId]
 * @param {string} [config.s3SecretAccessKey]
 * @param {string} [config.s3Endpoint]
 * @param {number} [config.concurrency]
 * @param {number} [config.healthcheckPort]
 */
export async function startBackup ({ dataURL, s3Region, s3BucketName, s3AccessKeyId, s3SecretAccessKey, s3Endpoint, concurrency, healthcheckPort = 9999 }) {
  const sourceDataFile = dataURL.substring(dataURL.lastIndexOf('/') + 1)
  const gracePeriodMs = REPORT_INTERVAL * 2
  const health = createHealthCheckServer({ sourceDataFile, gracePeriodMs })
  const logger = debug(`backup:${sourceDataFile}`)
  const log = (...args) => {
    logger(...args)
    health.heartbeat()
  }
  health.srv.listen(healthcheckPort, '127.0.0.1', () => {
    log(`healthcheck server listening on ${healthcheckPort}`)
  })
  log('starting IPFS...')
  const ipfs = new IpfsClient()
  await new Promise(resolve => setTimeout(resolve, 1000))
  const identity = await retry(() => ipfs.id(), { onFailedAttempt: console.error })
  log(`IPFS ready: ${identity.ID}`)

  log('configuring S3 client...')
  const s3Conf = { region: s3Region }
  if (s3AccessKeyId && s3SecretAccessKey) {
    s3Conf.credentials = { accessKeyId: s3AccessKeyId, secretAccessKey: s3SecretAccessKey }
  }
  if (s3Endpoint) {
    log('using s3 enpoint', s3Endpoint)
    s3Conf.endpoint = s3Endpoint
    s3Conf.forcePathStyle = true
  }
  const s3 = new S3Client(s3Conf)

  let totalProcessed = 0
  let totalSuccessful = 0

  await pipe(
    fetchCID(dataURL, log),
    filterAlreadyStored(s3, s3BucketName, log),
    transform(concurrency ?? CONCURRENCY, async (item) => {
      log(`processing ${item.cid}`)
      try {
        const size = await retry(async () => {
          let size = 0
          const source = (async function * () {
            for await (const chunk of exportCar(ipfs, item, log)) {
              size += chunk.length
              yield chunk
            }
          })()
          await s3Upload(s3, s3BucketName, item, source, log)
          return size
        }, {
          onFailedAttempt: info => log('failed attempt exportCar: %o', info),
          retries: 2
        })
        totalSuccessful++
        return { sourceDataFile, cid: item.cid, status: 'ok', size }
      } catch (err) {
        log(`failed to backup ${item.cid}`, err)
        return { sourceDataFile, cid: item.cid, status: 'error', error: err.message }
      } finally {
        totalProcessed++
        log(`processed ${totalSuccessful} of ${totalProcessed} CIDs successfully`)
        try {
          for await (const res of ipfs.repoGc()) {
            if (res.err) {
              log(`failed to GC ${res.cid}:`, res.err)
              continue
            }
          }
        } catch (err) {
          log(`GC error ${err.message || err}`)
        }
      }
    }),
    async function (source) {
      for await (const item of source) {
        console.log(JSON.stringify(item))
      }
    }
  )
  log('backup complete 🎉')
  health.done()
}

/**
 * @param {string|URL} url
 * @returns {AsyncIterable<InputData>}
 */
export async function * fetchCID (url, log) {
  const data = await fetchData(url, log)
  // @ts-ignore
  yield * parse(data)
}

/**
 * @param {string|URL} dataURL
 * @param {() => void)} log
 */
export async function fetchData (dataURL, log) {
  log('fetching dataURL %s', dataURL)
  const fileName = 'data'
  for (let i = 0; i < 10; i++) {
    try {
      const res = await fetch(dataURL)
      if (!res.ok || !res.body) {
        const errMessage = `${res.status} ${res.statusText} ${dataURL}`
        throw new Error(errMessage)
      }
      await res.body.pipeTo(Writable.toWeb(createWriteStream(fileName)))
      return createReadStream(fileName)
    } catch (err) {
      log('Error fetchData: %o', err)
    }
  }
  log('fetchData: giving up. could no get %s', dataURL)
}

/** @param {string} cid */
export const bucketKey = cid => `complete/${CID.parse(cid).toV1()}.car`

/**
 * @param {S3Client} s3
 * @param {string} bucket
 */
function filterAlreadyStored (s3, bucket, log) {
  /** @param {import('it-pipe').Source<InputData>} source */
  return async function * (source) {
    yield * pipe(
      source,
      transform(100, async item => {
        const cmd = new HeadObjectCommand({ Bucket: bucket, Key: bucketKey(item.cid) })
        try {
          await s3.send(cmd)
          log(`${item.cid} is already in S3`)
          return null
        } catch {
          return item
        }
      }),
      async function * (source) {
        for await (const item of source) {
          if (item != null) yield item
        }
      }
    )
  }
}

/**
 * @param {IpfsClient} ipfs
 */
export async function * exportCar (ipfs, item, log) {
  let reportInterval
  try {
    let bytesReceived = 0

    reportInterval = setInterval(() => {
      log(`received ${fmt(bytesReceived)} bytes of ${item.cid}`)
    }, REPORT_INTERVAL)

    for await (const chunk of ipfs.dagExport(item.cid, { timeout: BLOCK_TIMEOUT })) {
      bytesReceived += chunk.byteLength
      yield chunk
    }
  } finally {
    clearInterval(reportInterval)
  }
}

/**
 * @param {import('@aws-sdk/client-s3').S3Client} s3
 * @param {string} bucketName
 * @param {InputData} item
 * @param {AsyncIterable<Uint8Array>} content
 */
export async function s3Upload (s3, bucketName, item, content, log) {
  const key = bucketKey(item.cid)
  const upload = new Upload({
    client: s3,
    params: {
      Bucket: bucketName,
      Key: key,
      Body: Readable.from(content),
      Metadata: { structure: 'Complete' }
    }
  })
  await upload.done()
  log(`${item.cid} successfully uploaded to ${bucketName}/${key}`)
}
