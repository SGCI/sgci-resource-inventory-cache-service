const express = require('express');
const graphqlHTTP = require('express-graphql');
const schema = require('./schema/schema')
const mongoose = require('mongoose');

const app = express();

mongoose.connect("mongo-conn-url");
mongoose.connection.once('open', () => {
    console.log("Connected to database .....");
});

app.use('/graphql', graphqlHTTP({
    schema,
    graphiql: true
}));

app.listen(4000, () => {
    console.log("API is listening over port 4000")
})
