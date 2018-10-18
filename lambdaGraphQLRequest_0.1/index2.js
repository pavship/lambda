const AWS = require('aws-sdk')
const documentClient = new AWS.DynamoDB.DocumentClient({region: 'eu-west-1'})

const { GraphQLClient  } = require('graphql-request')

const client = new GraphQLClient('https://api.graph.cool/simple/v1/cjcgfcs363v5p0110vjauvz03', {
  headers: {
    Authorization: `Bearer ${process.env.GQ_TOKEN}`
  }
})
const client2 = new GraphQLClient('https://now-advanced.now.sh', {
  headers: {
    Authorization: `Bearer ${process.env.PRISMA_DEV_TOKEN}`
  }
})

exports.handler = (event, context, callback) => {
    console.log("Received event {}", JSON.stringify(event, 3))
    //perform GraphQL queries
    let field = event.field
    let query
    let request = (query) => client.request(query).then(data => callback(null, data[field]))
    let request2 = (query) => client2.request(query).then(data => callback(null, data[field]))
    let deptId = ''
    switch(field) {
        case "allModels":
            query = `{
              allModels {
                id
                article
                name
              }
            }`
            request(query)
            break;
        case "deptModels":
            field = 'allDeptModels'
            const DynamoQueryParams = {
              TableName: 'EnremkolUsersTable',
              key: {
                id: event.arguments.userId
              }
            }
            deptId = event.arguments.deptId
            query = `{
              allDeptModels (filter: {
                dept: {
                  id: "${deptId}"
                }
              }) {
                id
                model {
                  id
                  article
                  name
                }
                prods {
                  id
                  fullnumber
                  hasDefect
                  isSpoiled
                }
              }
            }`
            request(query)
            break;
        case "deptProds":
            field = 'deptProds'
            deptId = event.arguments.deptId
            query = `{
              deptProds(
                deptId: "cjbuuvk0v4s3p0162u894mowc"
              ) {
                id
                fullnumber
                hasDefect
                isSpoiled
                model {
                  id
                  article
                  name
                }
              }
            }`
            request2(query)
            break;
        default:
            // Error handling: attached additional error information to the post
            // result.errorMessage = 'Error with the mutation, data has changed';
            // result.errorType = 'MUTATION_ERROR';
            // callback(null, result);
            callback("Unknown field, unable to resolve " + field, null);
            break;
    }
    
    
}
