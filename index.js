require('dotenv').config()

const express = require('express')
const { Client } = require('pg')

const app = express()
const port = 3000
const connectionString = process.env.DB_CONNECTION_STRING


const connectDB = async () => {
  try {
    const client = new Client({ connectionString })
    await client.connect()
    return client
  } catch (err) {
    throw err
  }
}

const getCurrentReleaseMarker = async (client) => {
  try {
    const latestReleaseMarker = await client.query(`
      SELECT "jsonData"
      FROM "rawFiles"
      WHERE "fileName" = 'last-update-dataset'
      ORDER BY "releaseMarker" DESC
      LIMIT 1
    `)  
    return latestReleaseMarker.rows[0].jsonData
  } catch (err) {
    throw err
  }




  client.end()
}


const init = async () => {
  const client = await connectDB()
  const currentReleaseMarker =await getCurrentReleaseMarker(client)
  console.log(currentReleaseMarker)
}

init()

app.get('/', (req, res) => {
  res.send('Hello World!')
})


app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})
