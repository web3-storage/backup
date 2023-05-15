import debug from 'debug'
import { S3Client } from '@aws-sdk/client-s3'
import retry from 'p-retry'
import { transform } from 'streaming-iterables'
import { pipe } from 'it-pipe'
import formatNumber from 'format-number'
import { CID } from 'multiformats'
import { request } from 'undici'
import * as raw from 'multiformats/codecs/raw'
import * as dagPB from '@ipld/dag-pb'
import { IpfsClient } from './ipfs-client.js'
import { createHealthCheckServer } from './health.js'
import { fetchCID, exportCar, s3Upload } from './index.js'

const fmt = formatNumber()

const CONCURRENCY = 5
const BLOCK_TIMEOUT = 1000 * 30 // timeout if we don't receive a block after 30s
const REPORT_INTERVAL = 1000 * 10 // log download progress every 10 seconds
/** Max time to verify a DAG (in ms) */
const VERIFIER_TIMEOUT = 1000 * 60 * 15

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
 * @param {string|URL} config.verifierURL
 */
export async function startBackup ({ dataURL, s3Region, s3BucketName, s3AccessKeyId, s3SecretAccessKey, s3Endpoint, concurrency, healthcheckPort = 9999, verifierURL }) {
  const sourceDataFile = String(dataURL).substring(String(dataURL).lastIndexOf('/') + 1)
  const gracePeriodMs = REPORT_INTERVAL * 2
  const health = createHealthCheckServer({ sourceDataFile, gracePeriodMs })
  const logger = debug(`backup:${sourceDataFile}`)
  /** @type {(...args: any[]) => void} */
  const log = (...args) => {
    // @ts-ignore
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
    checkSize(ipfs, concurrency ?? CONCURRENCY, log),
    filterVerifiedComplete(verifierURL, concurrency ?? CONCURRENCY, log),
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
        }, { onFailedAttempt: info => log(info) })
        totalSuccessful++
        return { sourceDataFile, cid: item.cid, status: 'ok', size }
      } catch (err) {
        log(`failed to backup ${item.cid}`, err)
        return { sourceDataFile, cid: item.cid, status: 'error', size: item.size, error: err.message }
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
 * @param {string|URL} url Verifier API URL
 * @param {number} concurrency
 * @param {(...args: any[]) => void} log
 */
function filterVerifiedComplete (url, concurrency, log) {
  /** @param {import('it-pipe').Source<InputData & { size?: number | undefined }>} source */
  return async function * (source) {
    yield * pipe(
      source,
      transform(concurrency, async item => {
        log(`verifying ${item.cid}...`)
        const start = Date.now()
        while (true) {
          try {
            const res = await verifyCID(url, CID.parse(item.cid), log)
            log(`${item.cid} is verified ${res.structure} in S3`)
            if (!res.hashPassed) {
              log(`${item.cid} no hashes verified`)
              return item
            }
            if (res.hashFailed) {
              log(`${item.cid} hash(es) failed verification`)
              return item
            }
            if (res.hashUnknown) {
              log(`${item.cid} unknown hashes`)
              return item
            }
            if (res.structure !== 'Complete') {
              return item
            }
            return null
          } catch (err) {
            if (Date.now() - start < 5000) {
              await new Promise(resolve => setTimeout(resolve, 1000))
              continue
            }
            log(`failed to verify ${item.cid}`, err)
            return null // IDK - not really want to try transfer just incase it is complete and network failure
          }
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
 * @param {number} concurrency
 * @param {(...args: any[]) => void} log
 */
function checkSize (ipfs, concurrency, log) {
  /** @type {import('it-pipe').Transform<InputData, InputData & { size?: number | undefined }>} */
  return async function * (source) {
    yield * pipe(
      source,
      transform(concurrency, async item => {
        log(`checking size of ${item.cid}...`)
        const cid = CID.parse(item.cid)
        const size = await getSize(ipfs, cid)
        log(`${item.cid} size: ${size ? fmt(size) : 'unknown'} bytes`)
        return { ...item, size }
      })
    )
  }
}

/**
 * @param {import('./ipfs-client').IpfsClient} ipfs
 * @param {import('multiformats').CID} cid
 * @returns {Promise<number | undefined>}
 */
async function getSize (ipfs, cid) {
  if (cid.code === raw.code) {
    const block = await ipfs.blockGet(cid, { timeout: BLOCK_TIMEOUT })
    return block.byteLength
  } else if (cid.code === dagPB.code) {
    const stat = await ipfs.objectStat(cid, { timeout: BLOCK_TIMEOUT })
    return stat.CumulativeSize
  }
}

/**
 * @param {URL|string} url
 * @param {import('multiformats').Link} cid
 * @param {(...args: any[]) => void} [log]
 */
export async function verifyCID (url, cid, log = (() => {})) {
  const start = Date.now()
  let i = 0
  const verifyInterval = setInterval(() => log(`still verifying ${cid} after ${++i} minute(s)`), 60_000)
  try {
    const res = await request(new URL(`/cid/${cid}?hash=true`, url), {
      bodyTimeout: 0,
      headersTimeout: VERIFIER_TIMEOUT
    })
    if (res.statusCode !== 200) throw new Error(`verifier API responded with unexpected status: ${res.statusCode}`)
    return /** @type {import('linkdex').Report & import('linkdex/hashing-indexer').HashReport} */(await res.body.json())
  } finally {
    clearInterval(verifyInterval)
  }
}
