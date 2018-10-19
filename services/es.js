let AWS = require('aws-sdk');
let connectionClass = require('http-aws-es');
let elasticsearch = require('elasticsearch');

AWS.config.region = process.env.REGION;

let elasticClient = new elasticsearch.Client({
    host: process.env.HOST,
    log: 'error',
    requestTimeout: 600000,
    connectionClass: connectionClass,
    amazonES: {
      region: process.env.REGION,
      accessKey: process.env.ACCESS_KEY,
      secretKey: process.env.SECRET_KEY
    }
});

module.exports = elasticClient;