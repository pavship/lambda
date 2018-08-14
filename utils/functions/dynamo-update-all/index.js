// function updates in batches of 100 with cursor stored in updNote attribute

const deleteMode = true

const TableName = 'EnremkolWorkTable1'

const AWS = require('aws-sdk')
const dynamo = new AWS.DynamoDB.DocumentClient({ region: 'eu-west-1' })

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
    // if (isEmptyObj(res) && dataExpected) throw new Error(`DB ${operation} operation returned empty object. dbRequestParams: ${JSON.stringify(params, null, 2)}`)
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
    try {
        // 1. Query last updated record
        const updatedItems = await withHandler(dynamo.query({
            TableName,
            ProjectionExpression: 'id',
            KeyConditionExpression: `updNote = :updNote`,
            ExpressionAttributeValues: {
                ":updNote": 'unprocessed'
            },
            IndexName: 'updNote-id-index',
            ScanIndexForward: deleteMode ? true : false,
            Limit: 1
        }))
        const updatedItem = updatedItems[0]
        console.log('updatedItem > ', updatedItem)

        // // manual input of the last updated item
        // const updatedItem = { id: '2018-05-08T11:01:50.312Z-40175b3255e3' }

        // 2. Get items to update. Start from the beginning if updated record was not found
        const itemsToUpdate = await withHandler(dynamo.query({
            TableName,
            ProjectionExpression: 'id',
            KeyConditionExpression: !updatedItem ? `orgId = :orgId` : deleteMode ? 'orgId = :orgId AND id >= :fromId' : 'orgId = :orgId AND id > :fromId',
            ExpressionAttributeValues: {
                ":orgId": 1,
                ...(updatedItem && { ":fromId": updatedItem.id })
            },
            Limit: 50
        }))
        console.log('itemsToUpdate.length > ', itemsToUpdate.length)

        // 3. Update items
        const res = await Promise.all(
            itemsToUpdate.map(item => {
                return withHandler(dynamo.update({
                    TableName,
                    Key: {
                        orgId: 1,
                        id: item.id
                    },
                    UpdateExpression: deleteMode ? 'REMOVE updNote' : 'SET updNote = :updNote',
                    // UpdateExpression: 'DELETE updNote',
                    ...(!deleteMode && {
                        ExpressionAttributeValues: {
                            ':updNote': 'unprocessed'
                        }
                    })
                    //    ReturnValues: 'NONE'
                }))
            })
        )
        console.log('res.length > ', res.length)


        // // 3. Update all items IMPOSSIBLE BECAUSE OFF DYNAMO WRITE CAPACITY
        // const res = await Promise.all(
        //     items.map(item => {
        //         return withHandler(dynamo.update({
        //             TableName,
        //             Key: {
        //                 orgId: 1,
        //                 id: item.id
        //             },
        //             UpdateExpression: 'SET updNote = :updNote',
        //             ExpressionAttributeValues: {
        //                 ':updNote': 'unprocessed'
        //             }
        //         }))
        //     })
        // )
        // console.log('res.length > ', res.length)

        cb(null, { success: true })
    }
    // handle all possible errors
    catch (err) {
        console.log(err)
        // exit without error to avoid Lambda retries
        cb(null, err)
    }
}