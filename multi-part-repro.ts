import fastify from 'fastify'

const app = fastify()

app.register(require('@fastify/multipart'))

app.post('/object/*', async function (req, reply) {

  // This line makes all the difference....
  // if you comment out this line the app does not crash
  // doing this before req.file() causes the error to throw before the promise resolves resulting in an unhandled error
  await new Promise((resolve) => setImmediate(resolve))

  console.log(' ')
  console.log('...... BEFORE FORM DATA ----- THIS IS BEFORE request.file()')
  console.log(' ')

  const data = await req.file({ limits: { fileSize: 12344 } })

  console.log(' ')
  console.log('...... AFTER FORM DATA ----- THIS IS AFTER request.file()')
  console.log(' ')

  if (!data) {
    console.log('EMPTY DATA!!!')
    return
  }

  // Also, without this error handler the app will crash
  data.file.on('error', (eee: Error) => {
    console.log('WE HIT THE ERROR', eee)
  })

  // show the data we got (only logs for a good request)
  const dataBuffer = await data.toBuffer()
  console.log('data', await dataBuffer.toString())

  reply.send()
})

app.listen({ port: 5000 }, (err) => {
  if (err) throw err
  console.log(`server listening on ${JSON.stringify(app.server.address())}`)
})
