const AWS = require('aws-sdk')
const dynamo = new AWS.DynamoDB.DocumentClient({ region: 'eu-west-1' })
const _ = require('lodash')

// utility functions to handle dynamodb requests
const isEmptyObj = (o) => Object.keys(o).length === 0 && o.constructor === Object
const withHandler = async (dynamoReq) => {
  const res = await dynamoReq.promise()
  const { operation, params } = dynamoReq
  // console.log('DB operation: ', operation)
  // console.log('DB operation params: ', params)
  // console.log('DB operation res: %j', res)
  // Define if request should return data
  let dataExpected = true
  switch (operation) {
    case 'deleteItem':
    case 'updateItem':
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
    case 'scan':
    case 'query':
      data = res.Items || []
      break;
    case 'batchGetItem':
      data = res.Responses[Object.keys(params.RequestItems)[0]] || []
      break;
    case 'deleteItem':
      data = dataExpected ? res.Attributes : null
      break;
    default:
      data = res
  }
  return data
}

exports.handle = async function (e, ctx, cb) {
  console.log(`starting ${process.env.LAMBDA_FUNCTION_NAME} function with event: %j`, e)

  const TableName = 'EnremkolWorkTable1'

  try {
    const works = await withHandler(dynamo.scan({
      TableName,
      // AttributesToGet: ['id'],
      FilterExpression: 'NOT attribute_type (models, :v_sub)',
      ExpressionAttributeValues: { ':v_sub': 'NULL' }
    }))

    console.log('works.length > ', works.length);
    console.log('works[0] > ', works[0]);

    const modelWorks = works.map(w => ({
      id: w.models[0].id,
      name: w.models[0].name,
      workSubType: w.workSubType
    }))

    const uniqWorks = _(modelWorks).uniqBy(function (w) {
      return w.id + w.workSubType
    }).sortBy('name', 'workSubType').value()

    const models = uniqWorks.reduce((models, w) => {
      const foundModel = models.find(m => m.id === w.id)
      foundModel
        ? foundModel.workSubTypes.push({
          name: w.workSubType,
          normative: 0
        })
        : models.push({
          id: w.id,
          name: w.name,
          workSubTypes: [{
            name: w.workSubType,
            normative: 0
          }]
        })
      return models
    }, [])

    console.log('models > %j', models)

    cb(null, { hello: 'world' })
  }
  catch (err) {
    cb(err, null)
  }
}
