require('dotenv').config()

const express = require('express')
const { Client } = require('pg')
const axios = require('axios')

const app = express()
const port = process.env.APP_PORT || 8080
const connectionString = process.env.DB_CONNECTION_STRING
const settings = require('./config/settings')
const remoteLoopDuration = Number(process.env.REMOTE_RELEASE_MINUTES_CHECK) || 15

const memCache = {
  cacheObject: [],
  get () {
    return [...this.cacheObject]
  },
  set (entry) {
    this.cacheObject.push(entry)
  },
  flush () {
    this.cacheObject = []
  }
}

const connectDB = async () => {
  const client = new Client({ connectionString })
  await client.connect()
  return client
}

const getRemoteContent = async (file, format) => {
  const remoteContent = await axios.get(settings[file][format])
  return remoteContent.data
}

const getRemoteReleaseMarker = async () => {
  const remoteReleaseMarker = await axios.get(settings['last-update-dataset'].json)
  return remoteReleaseMarker.data
}

const getCurrentReleaseMarker = async (client) => {
  const latestReleaseMarker = await client.query(`
    SELECT "jsonData"
    FROM "rawFiles"
    WHERE "fileName" = 'last-update-dataset'
    ORDER BY "releaseMarker" DESC
    LIMIT 1
  `)
  return latestReleaseMarker.rows[0].jsonData
}

const checkRemoteUpdates = async (client) => {
  const currentReleaseMarker = await getCurrentReleaseMarker(client)
  const remoteReleaseMarker = await getRemoteReleaseMarker()
  console.log(
    'to update check: ',
    currentReleaseMarker.ultimo_aggiornamento, remoteReleaseMarker.ultimo_aggiornamento,
    currentReleaseMarker.ultimo_aggiornamento !== remoteReleaseMarker.ultimo_aggiornamento
  )
  return (currentReleaseMarker.ultimo_aggiornamento !== remoteReleaseMarker.ultimo_aggiornamento)
}

const updateLoop = async (client) => {
  const areUpdatesAvailable = await checkRemoteUpdates(client)
  if (areUpdatesAvailable) await performUpdate(client)
}

const replaceQuote = (text) => {
  return text.replace(/'/g,`''`)
}

const performUpdate = async (client) => {
  try {
    const remoteReleaseMarker = await getRemoteReleaseMarker()
    const lastUpdateTimestamp = remoteReleaseMarker.ultimo_aggiornamento
    await Promise.all(Object.keys(settings).map(async fileName => {
      let csvContent = ''
      let jsonContent = {}
      if (settings[fileName].csv) csvContent = await getRemoteContent(fileName, 'csv')
      if (settings[fileName].json) jsonContent = await getRemoteContent(fileName, 'json')
      const result = await client.query(`
        INSERT INTO "rawFiles"
        ("jsonData", "csvData", "fileName", "releaseMarker" ) VALUES
        ('${replaceQuote(JSON.stringify(jsonContent))}', '${replaceQuote(csvContent)}', '${fileName}', '${lastUpdateTimestamp}')
      `)
      return result
    }))
    return setMemCache(client)
  } catch (err) {
    console.log(err)
    throw err
  }
}

const getCurrentData = async (client) => {
  const currentReleaseMarker = await getCurrentReleaseMarker(client)
  const lastUpdateTimestamp = currentReleaseMarker.ultimo_aggiornamento
  const data = await client.query(`
    SELECT "uuid", "jsonData", "csvData", "fileName", "releaseMarker" 
    FROM "rawFiles"
    WHERE "releaseMarker" = '${lastUpdateTimestamp}'
  `)
  return data.rows
}

const setMemCache = async (client) => {
  const currentData = await getCurrentData(client)
  // sync assign to memCache
  memCache.flush()
  currentData.map(row => {
    return memCache.set(row)
  })
}

const init = async () => {
  const client = await connectDB()
  await updateLoop(client)
  await setMemCache(client)
  const memoryObject = await memCache.get()
  console.log(memoryObject)
  setInterval(
    updateLoop,
    Number(remoteLoopDuration) * 1000 * 60,
    client
  )
}

const mapOutputJsonFields = (dataSet) => {
  return dataSet.map(row => {
    return {
      uuid: row.uuid,
      jsonData: row.jsonData,
      fileName: row.fileName,
      releaseMarker: row.releaseMarker
    }
  })
}

const filterOutputFile = (dataSet, fileName) => {
  return dataSet.filter(row => {
    return fileName === row.fileName
  })
}

init()

app.get('/', (req, res) => {
  res.send('Hello Vax!')
})

app.get('/v0/latest/json', (req, res) => {
  const dataSet = memCache.get()
  const jsonMapped = mapOutputJsonFields(dataSet)
  return res.json(jsonMapped)
})

app.get('/v0/latest/:fileName/json', (req, res) => {
  const dataSet = memCache.get()
  const jsonMapped = mapOutputJsonFields(dataSet)
  const fileFiltered = filterOutputFile(jsonMapped, req.params.fileName)
  return res.json(fileFiltered)
})

app.listen(port, () => {
  console.log(`Vax data API listening at http://localhost:${port}`)
})
