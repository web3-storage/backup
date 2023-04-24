import debug from 'debug'
import { Readable, Writable } from 'stream'
import fs from 'fs'
import retry from 'p-retry'
import { transform } from 'streaming-iterables'
import { parse } from 'it-ndjson'
import { pipe } from 'it-pipe'
import * as Link from 'multiformats/link'

const VERIFIER_URL = 'https://linkdex.dag.haus'
const CONCURRENCY = 1

/** @typedef {{ cid: string, pinned_peers: string[] }} InputData */

/**
 * @param {Object} config
 * @param {string|URL} config.dataURL
 * @param {string|URL} [config.verifierURL]
 * @param {number} [config.concurrency]
 */
export async function startVerify ({ dataURL, verifierURL, concurrency }) {
  const sourceDataFile = new URL(dataURL).pathname.split('/').pop()

  const logger = debug(`verify:${sourceDataFile}`)
  const log = logger

  let totalProcessed = 0
  let totalSuccessful = 0

  await pipe(
    fetchCID(dataURL, log),
    transform(concurrency ?? CONCURRENCY, async (item) => {
      log(`processing ${item.cid}`)
      try {
        const report = await retry(() => verifyCID(verifierURL ?? VERIFIER_URL, item.cid), { onFailedAttempt: log })
        if (report.structure !== 'Complete') {
          throw new Error(`unexpected DAG structure: ${report.structure}`, { cause: report })
        }
        totalSuccessful++
        return { sourceDataFile, cid: item.cid, status: 'ok' }
      } catch (err) {
        log(`failed verification: ${item.cid}`, err)
        return { sourceDataFile, cid: item.cid, status: 'error', error: err.message }
      } finally {
        totalProcessed++
        log(`processed ${totalSuccessful} of ${totalProcessed} CIDs successfully`)
      }
    }),
    async function (source) {
      for await (const item of source) {
        console.log(JSON.stringify(item))
      }
    }
  )
  log('verify complete 🎉')
}

/**
 * @param {string|URL} url
 * @returns {AsyncIterable<{ cid: import('multiformats').Link }>}
 */
async function * fetchCID (url, log) {
  const data = await fetchData(url, log)
  // @ts-ignore
  for await (const item of /** @type {AsyncIterableIterator<InputData>} */(parse(data))) {
    yield { ...item, cid: Link.parse(item.cid) }
  }
}

async function fetchData (dataURL, log) {
  dataURL = new URL(dataURL)
  log('fetching dataURL %s', dataURL)
  const dir = 'data'
  await fs.promises.mkdir(dir, { recursive: true })
  const fileName = `${dir}/${dataURL.pathname.split('/').pop()}`
  try {
    await fs.promises.access(fileName, fs.constants.R_OK)
    log('found local cache %s', dataURL)
    return fs.createReadStream(fileName)
  } catch {}

  for (let i = 0; i < 10; i++) {
    try {
      const res = await fetch(dataURL)
      if (!res.ok || !res.body) {
        const errMessage = `${res.status} ${res.statusText} ${dataURL}`
        throw new Error(errMessage)
      }
      log('downloading %s', dataURL)
      await res.body.pipeTo(Writable.toWeb(fs.createWriteStream(fileName)))
      return fs.createReadStream(fileName)
    } catch (err) {
      log('Error fetchData: %o', err)
    }
  }
  log('fetchData: giving up. could no get %s', dataURL)
}

/**
 * @param {URL|string} url
 * @param {import('multiformats').Link} cid 
 */
async function verifyCID (url, cid) {
  const res = await fetch(new URL(`/cid/${cid}`, url))
  if (!res.ok) throw new Error(`verifier API responded with unexpected status: ${res.status}`)
  return /** @type {import('linkdex').Report} */(await res.json())
}
