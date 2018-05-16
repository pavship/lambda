const AWS = require('aws-sdk')
const dynamo = new AWS.DynamoDB.DocumentClient({region: 'eu-west-1'})
const isEmptyObj = (o) => Object.keys(o).length === 0 && o.constructor === Object

// utility function to handle dynamodb requests
const withHandler = async (dynamoReq) => {
  const res = await dynamoReq.promise()
  const { operation, params } = dynamoReq
  // console.log('DB operation: ', operation)
  // Define if request should return data
  let dataExpected = true
  switch (operation) {
    case 'deleteItem':
      if (!params.ReturnValues) dataExpected = false
      break;
  }
  // throw error if request returned empty object
  // for 'deleteItem' operation impossible to prove successful deletion if !params.ReturnValues (default)
  if (isEmptyObj(res) && dataExpected) throw new Error(`DB ${operation} operation returned empty object. dbRequestParams: ${JSON.stringify(params, null, 2)}`)
  // return data according to request operation type
  let data
  switch (operation) {
    case 'getItem':
      data = res.Item
      break;
    case 'query':
      data = res.Items
      break;
    case 'deleteItem':
      data = dataExpected ? res.Attributes : null
      break;
    default:
      data = res
  }
  return data
}

exports.handle = async function(e, ctx, cb) {
  console.log(`starting ${process.env.LAMBDA_FUNCTION_NAME} function with event: %j`, e)
  try {
    const getParams = {
      TableName : 'EnremkolWorkTable',
      Key: {
        orgId: 1,
        id: '018-03-30T08:54:39.001Z-2fa10eac04df'
      }
    }
    const delParams = {
      TableName : 'EnremkolWorkTable1',
      Key: {
        orgId: 1,
        id: '2018-03-30T10:28:07.422Z-723967f4a753'
      },
      ReturnValues: 'ALL_OLD'
    }
    let batchParams = {
      RequestItems: { }
    }
    const batchTable = 'EnremkolWorkTable1'
    batchParams.RequestItems[batchTable] = []
    const ids = [ '018-03-30T10:28:07.422Z-723967f4a753',
      '2018-03-30T11:32:16.533Z-6d18af034004',
      '2018-03-30T11:33:50.083Z-abf7327e173a'
    ]
    ids.map(id => {
      batchParams.RequestItems[batchTable].push({
        DeleteRequest: {
          Key: {
            orgId: 1,
            id: id
          }
        }
      })
    })
    // console.log(dynamo.get(getParams));
    // const work = await withHandler(dynamo.batchWrite(batchParams))
    // const work = await withHandler(dynamo.delete(delParams))
    // const work = await withHandler(dynamo.get(getParams))
    const work = await withHandler(dynamo.query({
      TableName: 'EnremkolDayExecStatTable',
      ProjectionExpression: 'id',
      KeyConditionExpression: "orgId = :orgId AND id BETWEEN :from AND :to",
      ExpressionAttributeValues: {
        ":orgId": 1,
        ":from": '2018-04-08T21:00:00.000Z',
        // To make BETWEEN condition work as expected, add 1 millisecond from the right end,
        // because EnremkolDayExecStatTable id attribute has format of <date>_<execId>
        ":to": '2018-04-09T21:00:00.000Z'
      }
    }))

    cb(null, { hello: 'world', work })
    // cb(null, { hello: 'world' })
  }
  catch(err) {
    cb(err, null)
  }
}
