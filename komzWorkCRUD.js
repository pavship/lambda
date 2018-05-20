const AWS = require('aws-sdk')
const documentClient = new AWS.DynamoDB.DocumentClient({region: 'eu-west-1'})
const lambda = new AWS.Lambda({ region: 'eu-west-1' })

exports.handler = ({ command, args }, context, callback) => {
    console.log("Received command: ", command, " with args: ", args)

    // SETTINGS
    // timezone offset is needed for this function. Hard coded GMT+3 Moscow. TODO place tz into DB table with org settings
    const tz = 3

    switch(command) {
        case "edit":
            // basic DynamoDB request params
            const params = {
              TableName: 'EnremkolWorkTable1',
              Key: {
                orgId: 1,
                id: args.id
              },
              ReturnValues: 'ALL_OLD'
            }

            // 1. Define function to regenerate statistics for days affected by this edit:
            // affStart and affFin define time period, which was affected by the editing
            let affStart, affFin, execId
            const regenStats = (affStart, affFin, execId) => {
                console.log('regenStats function with args > ', affStart, affFin, execId)

                // 1.1. Find first and last affected dates to pass into komzDayStatGen Lambda
                // const first = items[0].id.slice(0, 24)
                const first = new Date(new Date(new Date(affStart).getTime()+tz*3600000).setHours(-tz,0,0,0))
                // const first = new Date(new Date(affStart).setHours(-tz,0,0,0))
                // const last = items.pop().id.slice(0, 24)
                const last = new Date(new Date(new Date(affFin).getTime()+tz*3600000).setHours(-tz,0,0,0))
                // const last = new Date(new Date(affFin).setHours(-tz,0,0,0))
                console.log('first, last > ', first, last);

                // 1.4. Invoke komzDayStatGen Lambda function
                const payload = {args: { first, last, execId }}
                const lambdaParams = {
                    FunctionName: 'komzDayStatGen',
                    Payload: JSON.stringify(payload, null, 2) // pass params
                }
                lambda.invoke(lambdaParams, (err, data) => {
                    if (err) {
                        console.log(err)
                        callback(`Lambda DayExecStatTable regen was unsuccessful for dates from ${first} to ${last}`, null)
                    } else {
                        const dataIsEmpty = Object.keys(data).length === 0 && data.constructor === Object
                        if ( dataIsEmpty ) callback("Lambda DayExecStatTable regen operation returned empty object", null)
                    }
                })
            }

            // 2. Define function to delete edited work from DB
            const deleteWork = (callback) => {
                documentClient.delete(params, function(err, data) {
                    if (err) {
                        console.log(err)
                        callback("DynamoDB deleteWork operation was unsuccessful -> " + args.id, null)
                    } else {
                        const dataIsEmpty = Object.keys(data).length === 0 && data.constructor === Object
                        if ( dataIsEmpty ) callback("DynamoDB deleteWork operation returned empty object", null)
                        affStart = data.Attributes.start
                        affFin = data.Attributes.fin
                        execId = data.Attributes.exec
                        callback(data)
                    }
                })
            }
            if (args.delete) {
                // Handle DELETE request
                deleteWork((data) => {
                    regenStats(affStart, affFin, execId)
                    callback(null, {...data.Attributes, deleted: data.Attributes.id})
                })
            } else {
                // Handle EDIT request
                // 3. Reevaluate work duration and write into 'time' var, if the work is finished
                let time = null
                if (args.fin) {
                    // start and fin in epoch
                    const start_ep = Date.parse(args.start)
                    const fin_ep = Date.parse(args.fin)
                    // VALIDATION
                    // TODO dates validation with regards to adjacent works
                    const diff = fin_ep - start_ep
                    if (diff > 0) {
                        // work duration in seconds
                        time = Math.round(diff/1000)
                    } else {
                        callback("wrong 'start' and 'fin' datetimes provided for updateWork operation", null)
                    }
                }

                // 4. Delete work before edit (need to substitute work record in DB table, because it's impossible to update id-key attribute)
                deleteWork((data) => {
                    const oldId = data.Attributes.id
                    const id = args.start + oldId.slice(24)
                    // new item to write into DB
                    const item = {
                        ...data.Attributes,
                        start: args.start,
                        fin: args.fin || null,
                        time,
                        id
                    }
                    // 3. Put altered item into DB
                    const putParams = {
                        TableName: 'EnremkolWorkTable1',
                        Item: item
                    }
                    documentClient.put(putParams, function(err, data) {
                        if (err) {
                            console.log(err)
                            callback("WARNING! DynamoDB putWork operation was unsuccessful, but edited work was deleted! -> " + oldId, null)
                        } else {
                            // console.log('> work successfully put into DB')
                            // 4. Get written item (dynamodb.put method isn't able to return new item)
                            const getParams = {
                                TableName: 'EnremkolWorkTable1',
                                Key: {
                                    orgId: 1,
                                    id: id
                                }
                            }
                            documentClient.get(getParams, function(err, data) {
                                if (err) {
                                    console.log(err)
                                    callback("DynamoDB getWork operation was unsuccessful after edited work was put into DB -> " + id, null)
                                } else {
                                    // console.log('> fresh work received from DB')
                                    const dataIsEmpty = Object.keys(data).length === 0 && data.constructor === Object
                                    if ( dataIsEmpty ) callback("DynamoDB getWork operation returned empty object after edited work was put into DB -> " + id, null)
                                    // 'deleted' attribute is used in frontend to handle edited works received within subscription
                                    // deleted attr equals oldId of edited item. If id hasn't changed, deleted is set to null
                                    // console.log(oldId, id, oldId === id)
                                    // 5. Extend affected time frame if needed and regenStats
                                    affStart = item.start < affStart ? item.start : affStart
                                    affFin = affFin < item.fin ? item.fin : affFin
                                    regenStats(affStart, affFin, execId)
                                    callback(null, { ...data.Item, deleted: oldId === id ? null : oldId })
                                }
                            })

                        }
                    })
                })
                // UPDATE DynamoDB op not used in this Lambda
                // const extendedParams = {
                //     ...params,
                //     UpdateExpression: 'SET #start = :start, fin = :fin, #time = :time',
                //     ExpressionAttributeNames: {'#start' : 'start', '#time' : 'time'},
                //     ExpressionAttributeValues: {
                //         ':start': args.start,
                //         ':fin': args.fin || null,
                //         ':time': time
                //     },
                //     ReturnValues: 'ALL_NEW'
                // }
                // documentClient.update(extendedParams, function(err, data) {
                //   if (err) {
                //       console.log(err)
                //       callback("DynamoDB updateWork operation was unsuccessful -> " + args.id, null)
                //   } else {
                //       const dataIsEmpty = Object.keys(data).length === 0 && data.constructor === Object
                //       if ( dataIsEmpty ) callback("DynamoDB updateWork operation returned empty object", null)
                //       callback(null, {...data.Attributes})
                //   }
                // })
            }

            break
        default:
            callback("Unknown command <- " + command, null)
            break
    }
}
