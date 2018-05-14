//  This function copies all records from one DynamoDB table to another
// e.args should contain 'from' and 'to' arguments which are the table names

const AWS = require('aws-sdk')
const dynamo = new AWS.DynamoDB.DocumentClient({region: 'eu-west-1'})

exports.handle = function(e, ctx, cb) {
  console.log(`starting ${process.env.LAMBDA_FUNCTION_NAME} function with event: %j`, e)
  const { from, to } = e.args
  // 1. Take all records from source table
  const queryParams = {
    TableName: from,
    KeyConditionExpression: "orgId = :orgId",
    ExpressionAttributeValues: {
        ":orgId": 1
    }
  }
  dynamo.query(queryParams, (err, data) => {
    if (err) {
      console.log(err)
      cb(`DynamoDB ${from} query operation was unsuccessful`, null)
    } else {
      // Split records into chunks of 25
      let chunks = []
      for (let i=0; i<data.Items.length; i+=25) {
          chunks.push(data.Items.slice(i,i+25))
      }
      // 2. BatchWrite Items into target table
      // console.log(data)
      chunks.map(items => {
        let writeParams = {
          RequestItems: { }
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
      cb(null, {message: `migration from table ${from} to table ${to} completed successfully`})
    }
  })
  // // 1. Take all ids from source table
  // const queryParams = {
  //   TableName: from,
  //   ProjectionExpression: 'id',
  //   KeyConditionExpression: "orgId = :orgId",
  //   ExpressionAttributeValues: {
  //       ":orgId": 1
  //   },
  //   Limit: 100
  // }
  // dynamo.query(queryParams, (err, data) => {
  //   if (err) {
  //     console.log(err)
  //     cb(`DynamoDB ${from} query operation was unsuccessful`, null)
  //   } else {
  //     // 2. For each record get Item
  //     console.log(data)
  //     let getParams = {
  //       TableName: from,
  //       Key: {
  //         orgId: 1
  //       }
  //     }
  //     let putParams = {
  //       TableName: to
  //     }
  //     data.Items.map(({ id }) => {
  //       getParams.Key.id = id
  //       dynamo.get(getParams, (err, data) => {
  //         if (err) {
  //           console.log(err);
  //           cb(`DynamoDB ${from} get operation for item with id ${id} was unsuccessful`, null)
  //         } else {
  //           // 3. Put Item into target table
  //           putParams.Item = data.Item
  //           dynamo.put(putParams, (err, data) => {
  //             if (err) {
  //               console.log(err);
  //               cb(`DynamoDB ${to} put operation for item %j ${data.Item} was unsuccessful`, null)
  //             } else {
  //
  //             }
  //           })
  //         }
  //       })
  //     })
  //   }
  // })


  cb(null, { hello: 'world yo!' })
}
