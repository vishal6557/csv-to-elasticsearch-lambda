let AWS = require('aws-sdk');

// create AWS config
AWS.config.update({
    accessKeyId: process.env.ACCESS_KEY,
    secretAccessKey: process.env.SECRET_KEY,
    region: process.env.REGION
});

// create new AWS S3 object
let s3 = new AWS.S3();

module.exports = s3