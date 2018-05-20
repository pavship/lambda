//  This function copies all records from one DynamoDB table to another
// e.args should contain 'from' and 'to' arguments which are the table names

const AWS = require('aws-sdk')
const dynamo = new AWS.DynamoDB.DocumentClient({ region: 'eu-west-1' })

exports.handle = function (e, ctx, cb) {
  console.log(`starting ${process.env.LAMBDA_FUNCTION_NAME} function with event: %j`, e)
  const { from, to, startId, limit } = e.args
  // 1. Take records from source table
  const queryParams = {
    TableName: from,
    KeyConditionExpression: "orgId = :orgId AND id >= :id",
    ExpressionAttributeValues: {
      ":orgId": 1,
      ":id": startId
    },
    Limit: limit || 200
  }
  dynamo.query(queryParams, (err, data) => {
    if (err) {
      console.log(err)
      cb(`DynamoDB ${from} query operation was unsuccessful`, null)
    } else {
      // Split records into chunks of 25
      let chunks = []
      for (let i = 0; i < data.Items.length; i += 25) {
        chunks.push(data.Items.slice(i, i + 25))
      }
      // 2. BatchWrite Items into target table
      // console.log(data)
      chunks.map(items => {
        let writeParams = {
          RequestItems: {}
        }
        writeParams.RequestItems[to] = []
        items.map(item => {
          writeParams.RequestItems[to].push({
            PutRequest: {
              Item: item
            }
          })
        })
        dynamo.batchWrite(writeParams, (err, data) => {
          if (err) {
            console.log(err);
            cb(`DynamoDB ${from} get operation for item with id ${id} was unsuccessful`, null)
          } else {

          }
        })
      })
      cb(null, { message: `migration from table ${from} to table ${to} completed successfully` })
    }
  })
  cb(null, { hello: 'world yo!' })
}
