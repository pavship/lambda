const AWS = require('aws-sdk')
const documentClient = new AWS.DynamoDB.DocumentClient({region: 'eu-west-1'})

const _ = require('lodash')

// This function returns labour statistics by executors for each day
// between provided "first" and "last" day
// This function can evaluate stats for any day before today
// Selected day is reffered futher as 'period'
// Default date format used in this function is epoch

exports.handler = (event, context, callback) => {
	console.log('Lambda started with args > ', event.args)
	const first = Date.parse(event.args.first)
	const last = Date.parse(event.args.last)
	// if execId provided, function will only handle stats of this executor
	const { execId } = event.args
	// in clean mode function deletes stat items from DB before writing (TODO). For single exec, old items are deleted by editWorkFunction in advance
	const cleanMode = execId ? true : event.args.cleanMode

	// 1. INPUT VALIDATION
	let errMessage = ''
	// 1.1. Last date should be later than first
	if (last - first < 0) {
		errMessage += ' Provided last day of period should be later than the first day.'
	}
	// 1.2. This function accepts only days before today. So check last day
	// TODO function should cut off inappropriate days and run anyways
	if (Date.now() <= last + 24 * 3600000 - 1) {
		errMessage += ` Cannot run komzDayStatGen for dates later than yesterday. Invalid last date input > ${event.args.last}.`
	}
	// 1.3. Time period between first to last should represent a number of complete days
	const residue = (last - first) % (24 * 3600000)
	if (residue) {
		// errMessage = errMessage.concat(` Time period between first to last should represent a number of complete days.`)
		errMessage += ` Time period between first to last should represent a number of complete days.`
	}
	// 1.4. Function will accept maximum 10 days or 4 days for cleanMode with multiple execs, because of lambda execution time limit (3sec)
	const maxDays = (cleanMode && !execId) ? 4 : 10
	if ( last - first > maxDays*24*3600000 ) {
		errMessage += ` Time period between first to last days should be maximum 10 days or 4 days for cleanMode with multiple execs.`
	}
	if (errMessage) {
		callback(errMessage, null)
	}
	// VALIDATION PASSED
	else {

		// 2. Array of dates
		let dates = []
		for (let i = first; i < last + 1; i += 24 * 3600000) {
			dates.push(i)
		}
		const dates_iso = dates.map(date => new Date(date).toISOString())
		console.log(dates_iso)

		// 3. Delete old records if in cleanMode

		if (cleanMode) {
			let deleteParams = {
				TableName: 'EnremkolDayExecStatTable',
				Key: {
					orgId: 1
				},
				ReturnValues: 'NONE'
			}
			// 3.1. Array of ids to delete
			let statIds = []
			if (execId) {
				statIds = dates_iso.map(date_iso => `${date_iso}_${execId}`)
				statIds.map(id => {
					deleteParams.Key.id = id
					documentClient.delete(deleteParams, (err, data) => {
						if (err) {
							console.log(err)
							callback(`DynamoDB DayExecStat delete operation was unsuccessful for date_execId -> ${item.id}`, null)
						}
					})
				})
			} else {
				// 3.1.1 Get items' ids from DB
				// prepare arguments for the query
				// To make BETWEEN op work as expected, add 1 millisecond from both ends,
				// because EnremkolDayExecStatTable has format of "date_execId"
				const queryStart_iso = new Date(Date.parse(affStart) - 24*3600000).toISOString()
				affFin = new Date(Date.parse(affFin) + 1).toISOString()
				const queryParams = {
						TableName: 'EnremkolDayExecStatTable',
						ProjectionExpression: 'id',
						KeyConditionExpression: "orgId = :orgId AND id BETWEEN :from AND :to",
						ExpressionAttributeValues: {
								":orgId": 1,
								":from": queryStart_iso,
								":to": affFin
						}
				}
				documentClient.query(queryParams, (err, data) => {
					if (err) {
						console.log(err)
						callback(`DynamoDB DayExecStatTable operation for dates between ${queryStart_iso} and ${affFin} was unsuccessful`, null)
					} else {
						const dataIsEmpty = Object.keys(data).length === 0 && data.constructor === Object
						if ( dataIsEmpty ) callback("DynamoDB DayExecStatTable query operation returned empty object", null)
						// take only stat records of edited work executor
						const items = data.Items.filter(item => item.id.indexOf(execId) > 0)
						console.log(items)

						// 1.2. Delete stat records from DB
						let deleteParams = {
						    TableName: 'EnremkolDayExecStatTable',
						    Key: {
								orgId: 1
							},
							ReturnValues: 'NONE'
						}
						items.map(item => {
						    deleteParams.Key.id = item.id
						    documentClient.delete(deleteParams, (err, data) => {
						        if (err) {
								    	console.log(err)
								    	callback(`DynamoDB DayExecStat delete operation was unsuccessful for date_execId -> ${item.id}`, null)
							    	} else {
									    // TODO wrap delete requests into Promise.All
								    }
						    })
						})
					}
				})
			}
		}

		// 4. Map through all dates
		dates.map(date => {

			// 5. Get works from DB
			// 5.1. Prepare arguments for query
			const date_iso = new Date(date).toISOString()
			const from = date
			const to = from + 24 * 3600000
			// define dates for DB query in ISO format
			// queryFrom is 1 day before targeted day to get works which started earlier
			const queryFrom_iso = new Date(from - 24 * 3600000).toISOString()
			const to_iso = new Date(to).toISOString()
			let queryParams = {
				TableName: 'EnremkolWorkTable',
				KeyConditionExpression: "orgId = :orgId AND id BETWEEN :qf AND :to",
				ExpressionAttributeValues: {
					":orgId": 1,
					":qf": queryFrom_iso,
					":from": date_iso,
					":to": to_iso
				},
				// filter out works started earlier and finished before the date, keep all works in progress
				FilterExpression: "attribute_not_exists(fin) OR fin > :from"
			}
			if (execId) {
				queryParams = {
					...queryParams,
					FilterExpression: "#exec = :execId AND (attribute_not_exists(fin) OR fin > :from)",
					ExpressionAttributeNames: {"#exec": "exec"},
					ExpressionAttributeValues: {
						...queryParams.ExpressionAttributeValues,
						":execId": execId
					}
				}
			}
			documentClient.query(queryParams, (err, data) => {
				if (err) {
					console.log(err)
					callback(`DynamoDB queryWorks operation for date ${date_iso} was unsuccessful`, null)
				}
				else {
					const works = data.Items
					// 6. Prepare elements
					const preparedWorks = works.map(work => {
						const wStart = Date.parse(work.start)
						const wFin = Date.parse(work.fin) || null
						// labels in case work starts before period or finishes later
						const early = wStart < from
						const late = to < wFin || (!wFin && to < Date.now())
						// start, finish and time of the work interval which is within period
						const start = early ? from : wStart
						const fin = late ? to + 1 : wFin
						const time = fin - start
						return {
							...work,
							time
						}
					})
					// console.log(preparedWorks)
					// 7. Eval stats
					// helper function to count sum time
					const aggregateTime = (col) => {
						return col.reduce((sum, { time }) => sum + time, 0)
					}
					const worksByExec = _(preparedWorks).sortBy('execName').groupBy('execName').value()
					console.log(Object.keys(worksByExec))
					Object.keys(worksByExec).map(execName => {
						const works = worksByExec[execName]
						const execId = works[0].exec
						// define item to write into DB
						let item = {
							orgId: 1,
							id: `${date_iso}_${execId}`,
							execName,
							time: aggregateTime(works),
							workTypes: _(works).groupBy('workType').reduce(
								function(workTypes, works, workType) {
									workTypes.push({
										workType,
										time: aggregateTime(works),
										workTypeClass: (workType === 'Прямые') ? 'main' :
											(workType === 'Косвенные') ? 'aux' :
											(workType === 'Побочные') ? 'aside' :
											(workType === 'Отдых') ? 'rest' : 'negative',
										workSubTypes: _(works).groupBy('workSubType').reduce(
											function(workSubTypes, works, workSubType) {
												// console.log(works);
												// reject null subTypes
												workSubType !== 'null' && workSubTypes.push({
													workSubType,
													time: aggregateTime(works),
													models: _(works).groupBy('models[0].article').reduce(
														function(models, works, article) {
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
																		function(prods, value, id) {
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
						// Define putFunction
						const putItem = () => {
							const putParams = {
								TableName: 'EnremkolDayExecStatTable',
								Item: item
							}
							documentClient.put(putParams, function(err, data) {
								if (err) {
									console.log(err)
									callback("DynamoDB putDayExecStat operation was unsuccessful for day_exec -> " + item.id, null)
								}
								else {
									callback(null, { success: true })
								}
							})
						}
						// 9. Update (or create) stat record into DB
						putItem()
					})
				}
			})
		})
	}

}
