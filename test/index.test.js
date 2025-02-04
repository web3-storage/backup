import { GenericContainer, Wait, Network } from 'testcontainers'
import { S3Client, CreateBucketCommand } from '@aws-sdk/client-s3'
import http from 'http'
import { fetchData } from '../index.js'
import test from 'ava'

test('backup a dag', async t => {
  t.timeout(60_000 * 5) // first run can take a while if you need to fetch the images. (takes ~14s after that)

  const verifierSrv = await createMockVerifierServer()
  // @ts-ignore
  const verifierURL = `http://host.docker.internal:${verifierSrv.address().port}`
  t.teardown(() => verifierSrv.close())

  const dataSrv = await createMockDataServer()
  // @ts-ignore
  const dataURL = `http://host.docker.internal:${dataSrv.address().port}/nft-0.json`
  t.teardown(() => dataSrv.close())

  const network = await new Network().start() // so backup can find minio

  const minio = await new GenericContainer('quay.io/minio/minio:RELEASE.2023-02-27T18-10-45Z')
    .withName(`minio-${Date.now()}`)
    .withCommand(['server', '/data'])
    .withNetwork(network)
    .withNetworkAliases('minio')
    .withExposedPorts(9000)
    .start()

  const s3Endpoint = 'http://minio:9000' // container to container
  const accessKeyId = 'minioadmin'
  const secretAccessKey = 'minioadmin'
  const bucketName = 'backup-test'
  const region = 'us-east-1'

  const mb = await createBucket(bucketName, minio, accessKeyId, secretAccessKey, region)
  t.is(mb.$metadata.httpStatusCode, 200)

  const img = await GenericContainer.fromDockerfile('.').build()
  img.withName(`backup-${Date.now()}`)
  img.withWaitStrategy(Wait.forLogMessage(/configuring S3 client/))
  img.withNetwork(network)
  img.withExtraHosts([{ host: 'host.docker.internal', ipAddress: 'host-gateway' }])
  img.withEnvironment({
    BATCH_SIZE: '1',
    CONCURRENCY: '1',
    DEBUG: 'backup:*',
    DATA_URL: dataURL,
    VERIFIER_URL: verifierURL,
    S3_ENDPOINT: s3Endpoint,
    S3_REGION: region,
    S3_BUCKET_NAME: bucketName,
    S3_ACCESS_KEY_ID: accessKeyId,
    S3_SECRET_ACCESS_KEY: secretAccessKey
  })

  const container = await img.start()
  const logStream = await container.logs()
  for await (const line of logStream) {
    if (line.includes('"cid":"QmSUF1gaYbDNPQtBgaYwdmBn1dBH8CSQoi8uFPNsFm4S11","status":"error"')) {
      t.fail('failed to fetch QmSUF1gaYbDNPQtBgaYwdmBn1dBH8CSQoi8uFPNsFm4S11')
    }
    if (line.includes('"cid":"QmSUF1gaYbDNPQtBgaYwdmBn1dBH8CSQoi8uFPNsFm4S11","status":"ok"')) {
      t.pass('backed up QmSUF1gaYbDNPQtBgaYwdmBn1dBH8CSQoi8uFPNsFm4S11')
      break
    }
  }
})

async function createBucket (bucketName, minio, accessKeyId, secretAccessKey, region) {
  const endpoint = `http://${minio.getHost()}:${minio.getMappedPort(9000)}` // host to container
  const s3 = new S3Client({
    region,
    endpoint,
    forcePathStyle: true,
    credentials: {
      accessKeyId,
      secretAccessKey
    }
  })
  return s3.send(new CreateBucketCommand({ Bucket: bucketName }))
}

async function createMockVerifierServer () {
  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json')
    res.write(JSON.stringify({ structure: 'Unknown' }))
    res.end()
  })
  await new Promise(resolve => server.listen(resolve))
  return server
}

async function createMockDataServer () {
  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/x-ndjson')
    res.write(JSON.stringify({ cid: 'QmSUF1gaYbDNPQtBgaYwdmBn1dBH8CSQoi8uFPNsFm4S11' }))
    res.end()
  })
  await new Promise(resolve => server.listen(resolve))
  return server
}

test('fetchData', async t => {
  const data = await fetchData('https://bafybeiavn43yjnai7dhtyrxl5n3euhcsyzxz34gpg44nvy7x2ugs5j7fg4.ipfs.w3s.link/x', console.log)
  const chunks = []
  for await (const chunk of data) {
    chunks.push(chunk)
  }
  t.is(chunks.join(''), 'test\n')
})
