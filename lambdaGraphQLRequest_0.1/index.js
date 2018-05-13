const { GraphQLClient  } = require('graphql-request')

const client = new GraphQLClient('https://api.graph.cool/simple/v1/cjcgfcs363v5p0110vjauvz03', {
  headers: {
    Authorization: `Bearer ${process.env.GQ_TOKEN}`
  }
})

exports.handler = (event, context, callback) => {
    console.log("Received event {}", JSON.stringify(event, 3))
    const field = event.field
    let query
    switch(field) {
        case "allModels":
            query = `{
              allModels {
                id
                article
                name
              }
            }`
            break;
        case "allDeptModels":
            const deptId = event.arguments.deptId
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
            break;
        default:
            // Error handling: attached additional error information to the post
            // result.errorMessage = 'Error with the mutation, data has changed';
            // result.errorType = 'MUTATION_ERROR';
            // callback(null, result);
            callback("Unknown field, unable to resolve " + field, null);
            break;
    }
    client.request(query).then(data => callback(null, data[field]))

}
