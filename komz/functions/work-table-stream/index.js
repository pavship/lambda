// this function handles logic upon work finish

const AWS = require('aws-sdk')
const dynamo = new AWS.DynamoDB.DocumentClient({ region: 'eu-west-1' })

// utility functions to handle dynamodb requests
const isEmptyObj = (o) => Object.keys(o).length === 0 && o.constructor === Object
const withHandler = async (dynamoReq) => {
    const res = await dynamoReq.promise()
    const { operation, params } = dynamoReq
    console.log('DB operation: ', operation)
    console.log('DB operation params: ', params)
    console.log('DB operation res: %j', res)
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
// util to split array into chunks
const toChunks = (arr, size) => {
    let chunks = []
    for (let i = 0; i < arr.length; i += size) {
        chunks.push(arr.slice(i, i + size))
    }
    return chunks
}

exports.handle = async function (e, ctx, cb) {
    console.log(`starting ${process.env.LAMBDA_FUNCTION_NAME} function with event: %j`, e)
    try {
        handleRecord = async (record) => {
            // 1. Classify event and extract work object
            let work, oldWork, workEdit
            const event = record.eventName
            switch (event) {
                case 'INSERT':
                    work = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage)
                    break;
                case 'MODIFY':
                    oldWork = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage)
                    work = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage)
                    break;
                case 'REMOVE':
                    work = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage)
                    break;
                default:
                    console.log(`${record.eventName} event is not supposed to be handled by this stream. Unhandled without errors.`)
                    return null
            }
            console.log('work > %j', work)

            const { id, start, fin, execName, workType, workSubType, tag, updNote } = work
            const model = work.models && work.models[0]

            // 2. Object validation and further event classification
            // work should be of the main workTypeClass
            if (workType !== 'Прямые') {
                console.log(`Work of '${workType}' workType is not supposed to be handled by this stream. Unhandled without errors.`)
                return null
            }
            // accept MODIFY when the work was just finished by executor OR work's update was triggered by developer
            if (event === 'MODIFY' && (!tag && oldWork.tag === 'current' || !updNote && oldWork.updNote === 'unprocessed')
                // accept INSERT of finished works only (INSERTS after DELETE within editing work by an Admin). Unaccept new INSERTs made by Execs.
                || event === 'INSERT' && fin
                // accept all REMOVEs
                || event === 'REMOVE') {
                // Accepted
            } else {
                console.log('This type of work record update is not supposed to be handled by this stream. Unhandled without errors.')
                return null
            }
            // Validation passed

            // 3. Split products into chunks of 100 and BatchGet from DB
            const dbProdsChunks = await Promise.all(
                toChunks(model.prods, 10).map(prods => {
                    return withHandler(dynamo.batchGet({
                        RequestItems: {
                            EnremkolProdTable: {
                                Keys: [
                                    ...prods.map(prod => ({
                                        orgId: 1,
                                        id: `${model.id}-${prod.id}`
                                    }))
                                ]
                            }
                        }
                    }))
                })
            )
            // flatten array of chunks
            const dbProds = dbProdsChunks.reduce((prods, chunk) => [...prods, ...chunk], [])
            console.log('dbProds.length > ', dbProds.length)
            // 4.Merge prods found in DB and products not registered yet
            const prods = [
                ...model.prods.filter(prod => !dbProds.some(dbProd => dbProd.id === `${model.id}-${prod.id}`)),
                ...dbProds
            ]

            // 5. Prepare altered products array
            // define work to push into prod.op.works array
            const opWork = {
                id,
                // work time devided by the quantity of products
                // TODO refactor app to handle work.time attribute in milliseconds
                time: (Date.parse(fin) - Date.parse(start)) / model.prods.length,
                start,
                fin,
                execName
            }
            // define reusable newOp object with just new work
            const newOpObject = {
                workSubType,
                start,
                fin,
                time: opWork.time,
                works: [
                    opWork
                ]
            }
            // define reusable function to modify op by adding new work to the end
            const opWithNewWorkAtTheEnd = (op) => ({
                ...op,
                fin: opWork.fin,
                time: op.time + opWork.time,
                works: [
                    ...op.works,
                    opWork
                ]
            })
            // define reusable function that assumes that new work is the most recent work and is added to the end for this product
            addMostRecentWork = (prod) => {
                const { id, fullnumber, time, ops } = prod
                const lastOp = ops && ops[ops.length - 1]
                // newOp is either existing last op extended with the work or new op
                // new op is created if workSubType has changed
                const sameWst = lastOp && (workSubType === lastOp.workSubType)
                const newOp = (ops && sameWst)
                    ? opWithNewWorkAtTheEnd(lastOp)
                    : newOpObject
                return {
                    orgId: 1,
                    id: (id.indexOf('-') < 0) ? `${model.id}-${id}` : id,
                    fullnumber,
                    model: {
                        name: model.name
                    },
                    time: time ? time + opWork.time : opWork.time,
                    ops: [
                        ...(ops ? (sameWst ? ops.slice(0, -1) : ops) : []),
                        newOp
                    ]
                }
            }
            let newProds
            switch (event) {
                case 'INSERT':
                    newProds = prods.map(prod => {
                        const { id, fullnumber, time, ops } = prod
                        // find op that contains the first work which started later than new work and index of this work
                        let nextWorkIndex
                        let opIndex = ops.findIndex(op => (nextWorkIndex = op.works.findIndex(w => w.start > start)) !== -1)
                        // if no such op found, work is the most recent
                        if (opIndex === -1) return addMostRecentWork(prod)
                        let op = ops[opIndex]
                        // utility function to sum up times from array of works
                        sumTime = (ws) => ws.reduce((sum, { time }) => sum += time, 0)
                        // if nextWork is found, newWork will
                        if (nextWorkIndex >= 0) {
                            // a) be added to the op if workSubType is the same
                            if (workSubType === op.workSubType) {
                                op.time += opWork.time
                                op.works.splice(nextWorkIndex, 0, opWork)
                                // also change beginning time of the work is added to the beginning
                                if (nextWorkIndex === 0) op.start = start
                            }
                            // b) break the op into two parts if workSubTypes are different and if nextWork is not the first in the op
                            else if (nextWorkIndex !== 0) {
                                const prevWorks = op.works.slice(0, nextWorkIndex)
                                const prevOp = {
                                    ...op,
                                    fin: prevWorks[prevWorks.length - 1].fin,
                                    time: sumTime(prevWorks),
                                    works: prevWorks
                                }
                                const nextWorks = op.works.slice(nextWorkIndex)
                                const nextOp = {
                                    ...op,
                                    start: nextWorks[0].start,
                                    time: sumTime(nextWorks),
                                    works: nextWorks
                                }
                                ops.splice(opIndex, 1, prevOp, newOpObject, nextOp)
                                // if nextWork is the first in the op and workSubTypes are different, newWork will
                            } else if (nextWorkIndex === 0) {
                                // c) form new op if prevOp does not exist or has different workSubType with the new work
                                if (opIndex === 0 || workSubType !== ops[opIndex - 1]) {
                                    ops.splice(opIndex, 0, newOpObject)
                                    // d) be added to the end of the prevOp if it has same workSubType with prevOp
                                } else if (workSubType === ops[opIndex - 1]) {
                                    const newOp = opWithNewWorkAtTheEnd(ops[opIndex - 1])
                                    ops.splice(opIndex - 1, 1, newOp)
                                }
                            }
                            // modify prod's time
                            prod.time += opWork.time
                            return prod
                        }
                    })
                    break;
                case 'MODIFY':
                    newProds = prods.map(prod => addMostRecentWork(prod))
                    break;
                case 'REMOVE':
                    newProds = prods.map(prod => {
                        const { time, ops } = prod
                        // find op that contains the work and the work index
                        let workIndex
                        let opIndex = ops.findIndex(op => (workIndex = op.works.findIndex(w => w.id === id)) !== -1)
                        const op = ops[opIndex]
                        const removedWork = op.works.splice(workIndex, 1)[0]
                        // if no works left in the op
                        if (!op.works.length) {
                            // a) delete the op
                            ops.splice(opIndex, 1)
                            // b) if op had adjascent ops
                            if (opIndex !== 0 && opIndex !== ops.length) {
                                const prevOp = ops[opIndex - 1]
                                const nextOp = ops[opIndex]
                                // join them if they have same workSubType
                                if (prevOp.workSubType === nextOp.workSubType) {
                                    const newOp = {
                                        ...prevOp,
                                        fin: nextOp.fin,
                                        time: prevOp.time + nextOp.time,
                                        works: [
                                            ...prevOp.works,
                                            ...nextOp.works
                                        ]
                                    }
                                    ops.splice(opIndex - 1, 2, newOp)
                                }
                            }
                        }
                        else {
                            op.time -= removedWork.time
                            op.start = op.works[0].start
                            op.fin = op.works[op.works.length - 1].fin
                        }
                        // prod will be removed from db if it has no ops left
                        return (!ops.length)
                            ? { ...prod, delete: true }
                            : { ...prod, time: time - removedWork.time }
                    })
                    break;
                default:
                    console.log(`${record.eventName} event is not supposed to be handled by this stream. Unhandled without errors.`)
                    return null
            }
            console.log('newProds -> %j', newProds)

            // 6. Split products into chunks of 10 and BatchWrite into DB
            await Promise.all(
                toChunks(newProds, 10).map(newProds => {
                    return withHandler(dynamo.batchWrite({
                        RequestItems: {
                            EnremkolProdTable: newProds.map(prod => {
                                return prod.delete
                                    ? {
                                        DeleteRequest: {
                                            Key: { orgId: 1, id: prod.id }
                                        }
                                    }
                                    : {
                                        PutRequest: {
                                            Item: prod
                                        }
                                    }
                            })
                        }
                    }))
                })
            )
        }
        // handle REMOVEs first
        const delRes = await Promise.all(e.Records.filter(r => r.eventName === 'REMOVE').map(record => handleRecord(record)))
        console.log('res del > ', delRes)
        // then handle other event types
        const otherRes = await Promise.all(e.Records.filter(r => r.eventName !== 'REMOVE').map(record => handleRecord(record)))
        console.log('res other > ', otherRes)

        // // handle REMOVEs first
        // e.Records.filter(r => r.eventName === 'REMOVE').forEach(async (record) => {
        //     res = await handleRecord(record)
        //     console.log('res del > ', res);
        // })
        // // then handle other event types
        // e.Records.filter(r => r.eventName !== 'REMOVE').forEach(async (record) => {
        //     res = await handleRecord(record)
        //     console.log('res other > ', res);
        // })


        cb(null, { success: true })
    }
    // handle all possible errors
    catch (err) {
        console.log(err)
        // exit without error to avoid Lambda retries
        cb(null, err)
    }
}
