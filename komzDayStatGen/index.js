// This function returns labour statistics by executors for each day
// in the period between "first" and "last" day arguments.
// Arguments may not be provided when invoked by scheduled WatchLog event.
// In this case function evals stats for yesterday.
// This function can evaluate stats for any day before today.
// In case specified period includes inappropriate dates, such dates are excluded.
// Default date format used in this function is epoch.

// 0. Imports, utils
const AWS = require('aws-sdk')
const dynamo = new AWS.DynamoDB.DocumentClient({ region: 'eu-west-1' })
const _ = require('lodash')

// utility functions to handle dynamodb requests
const isEmptyObj = (o) => Object.keys(o).length === 0 && o.constructor === Object
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
// util to split array into chunks
const toChunks = (arr, size) => {
	let chunks = []
	for (let i = 0; i < arr.length; i += size) {
		chunks.push(arr.slice(i, i + size))
	}
	return chunks
}

exports.handler = async (e, ctx, cb) => {
	try {
		console.log(`starting komzDayStatGen function with event: %j`, e)
		// timezone offset is needed for this function. Hard coded GMT+3 Moscow.
		// TODO place tz into DB table with org settings
		const tz = 3
		const yesterday = new Date(Date.now() + tz * 3600000).setHours(-24 - tz, 0, 0, 0)
		let first, last, execId, cleanMode, statItems
		if (e.args) {
			first = Date.parse(e.args.first)
			last = Date.parse(e.args.last)
			// if execId provided, function will only handle stats of this executor
			execId = e.args.execId
			// in clean mode function deletes stat items from DB before writing (TODO). For single exec, cleanMode is by default
			cleanMode = execId ? true : e.args.cleanMode
		}
		// In case args are not provided, they are equal to yesterday
		else {
			first = last = yesterday
			cleanMode = true
		}

		// 1. INPUT VALIDATION
		// 1.1. Last date should be later than first
		if (last - first < 0) {
			throw new Error('Provided last day of period should be later than the first day.')
		}
		// 1.2. Time period between first to last should represent a number of complete days
		const residue = (last - first) % (24 * 3600000)
		if (residue) {
			throw new Error(`Time period between first and last date should be Integer >= 0.`)
		}
		// 1.3. This function accepts only days before today. So check first day
		if (Date.now() < first + 24 * 3600000) {
			throw new Error(`Cannot run komzDayStatGen for dates later than yesterday. Invalid first date input > ${e.args.first}.`)
			// and cut off inappropriate days
		} else if (Date.now() < last + 24 * 3600000) {
			last = yesterday
		}
		// 1.4. Function will accept maximum 10 days or 5 days for cleanMode with multiple execs, because of lambda execution time limit (3sec)
		const maxDays = (cleanMode && !execId) ? 5 : 10
		if (last - first > maxDays * 24 * 3600000) {
			throw new Error(`Time period between first to last days should be maximum 10 days or 5 days for cleanMode with multiple execs.`)
		}
		// VALIDATION PASSED

		// 2. Array of dates
		let dates = []
		for (let i = first; i < last + 1; i += 24 * 3600000) {
			dates.push(i)
		}
		// array of dates in ISO format
		const dates_iso = dates.map(date => new Date(date).toISOString())
		// log out stat dates to be evaluated
		console.log(dates_iso)
		// 3. Delete old records if in cleanMode
		if (cleanMode) {
			// 3.1. Get a list of ids to delete
			let statIds = []
			// for single exec execution
			if (execId) {
				statIds = dates_iso.map(date_iso => `${date_iso}_${execId}`)
			}
			// for multi exec execution
			else {
				statItems = await withHandler(dynamo.query({
					TableName: 'EnremkolDayExecStatTable',
					ProjectionExpression: 'id',
					KeyConditionExpression: "orgId = :orgId AND id BETWEEN :from AND :to",
					ExpressionAttributeValues: {
						":orgId": 1,
						":from": new Date(first).toISOString(),
						// To make BETWEEN condition work as expected, add 1 millisecond from the right end,
						// because EnremkolDayExecStatTable id attribute has format of <date>_<execId>
						":to": new Date(last + 1).toISOString()
					}
				}))
				statIds = statItems.map(item => item.id)
			}
			// 3.2. Split records into chunks of 10 and BatchDelete
			await Promise.all(
				toChunks(statIds, 10).map(statIds => {
					return withHandler(dynamo.batchWrite({
						RequestItems: {
							EnremkolDayExecStatTable: statIds.map(id => ({
								DeleteRequest: {
									Key: { orgId: 1, id }
								}
							}))
						}
					}))
				})
			)
		}

		statItems = []
		// 4. Evaluate stat for each date for each executor and push it into statItems
		// 4.1. Get works from DB
		const dayWorks = await Promise.all(
			dates.map(date => {
				let queryParams = {
					TableName: 'EnremkolWorkTable1',
					KeyConditionExpression: "orgId = :orgId AND id BETWEEN :qf AND :to",
					ExpressionAttributeValues: {
						":orgId": 1,
						// define dates for DB query in ISO format
						// queryFrom is 1 day before targeted day to get works which started earlier
						":qf": new Date(date - 24 * 3600000).toISOString(),
						":from": new Date(date).toISOString(),
						":to": new Date(date + 24 * 3600000).toISOString()
					},
					// filter out works started earlier and finished before the date, keep all works in progress
					FilterExpression: "attribute_not_exists(fin) OR fin > :from"
				}
				if (execId) {
					queryParams = {
						...queryParams,
						FilterExpression: "#exec = :execId AND (attribute_not_exists(fin) OR fin > :from)",
						ExpressionAttributeNames: { "#exec": "exec" },
						ExpressionAttributeValues: {
							...queryParams.ExpressionAttributeValues,
							":execId": execId
						}
					}
				}
				return withHandler(dynamo.query(queryParams))
			})
		)
		// console.log(dayWorks);

		// 4.2. Eval stats
		dates.map(async (date, i) => {
			const date_iso = new Date(date).toISOString()
			const from = date
			const to = from + 24 * 3600000
			const works = dayWorks[i]
			// prepare elements
			const preparedWorks = works.map(work => {
				const wStart = Date.parse(work.start)
				const wFin = Date.parse(work.fin) || null
				// labels in case work starts before the date or finishes later
				const early = wStart < from
				const late = to < wFin || (!wFin && to < Date.now())
				// start, finish and time of the work interval which is within the date
				const start = early ? from : wStart
				const fin = late ? to + 1 : wFin
				const time = fin - start
				return {
					...work,
					time
				}
			})
			// console.log(preparedWorks)
			// define helper function to count sum time
			const aggregateTime = (col) => {
				return col.reduce((sum, { time }) => sum + time, 0)
			}
			const worksByExec = _(preparedWorks).sortBy('execName').groupBy('execName').value()
			console.log(Object.keys(worksByExec))
			Object.keys(worksByExec).map(execName => {
				const works = worksByExec[execName]
				const execId = works[0].exec
				// evaluate stat item
				const item = {
					orgId: 1,
					id: `${date_iso}_${execId}`,
					execName,
					time: aggregateTime(works),
					workTypes: _(works).groupBy('workType').reduce(
						function (workTypes, works, workType) {
							workTypes.push({
								workType,
								time: aggregateTime(works),
								workTypeClass: (workType === 'Прямые') ? 'main' :
									(workType === 'Косвенные') ? 'aux' :
										(workType === 'Побочные') ? 'aside' :
											(workType === 'Отдых') ? 'rest' : 'negative',
								workSubTypes: _(works).groupBy('workSubType').reduce(
									function (workSubTypes, works, workSubType) {
										// console.log(works);
										// reject null subTypes
										workSubType !== 'null' && workSubTypes.push({
											workSubType,
											time: aggregateTime(works),
											models: _(works).groupBy('models[0].article').reduce(
												function (models, works, article) {
													// reject works with undefined models
													// console.log(models, works, article);
													if (article !== 'undefined') {
														const { name, article } = works[0].models[0]
														const time = aggregateTime(works)
														models.push({
															name,
															article,
															time,
															prods: _(works.map(({ time, models }) => {
																const length = models[0].prods.length
																return models[0].prods.map(({ id, fullnumber }) => ({
																	id,
																	fullnumber,
																	time: time / length
																}))
															})).flatten().groupBy('id').reduce(
																function (prods, value, id) {
																	// console.log(prods, value, id);
																	prods.push({
																		id,
																		fullnumber: value[0].fullnumber,
																		time: aggregateTime(value)
																	})
																	// console.log(prods)
																	return prods
																}, []
															)
														})
													}
													// console.log(models)
													return models
												}, []
											)
										})
										//console.log(workSubTypes)
										return workSubTypes
									}, []
								)
							})
							return workTypes
						}, []
					)
				}
				// console.log(item);
				// 4.3. Push stats into array
				statItems.push(item)
			})
		})
		// console.log(statItems);

		// 5. Split statItems into chunks of 10 BatchWrite into DB
		await Promise.all(
			toChunks(statItems, 10).map(statItems => {
				return withHandler(dynamo.batchWrite({
					RequestItems: {
						EnremkolDayExecStatTable: statItems.map(item => ({
							PutRequest: {
								Item: item
							}
						}))
					}
				}))
			})
		)
		cb(null, { success: true })
	}
	// handle all possible errors
	catch (err) {
		cb(err, null)
	}
}
