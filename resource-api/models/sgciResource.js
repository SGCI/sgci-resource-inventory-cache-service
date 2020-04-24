const mongoose = require('mongoose')
const Schema = mongoose.Schema;

const sgciResourceSchema = new Schema({
    "id": String,
    "name": String,
    "description": String,
    "resourceType": String,
    "resource": Schema.Types.Mixed
});

module.exports = mongoose.model('resources', sgciResourceSchema)