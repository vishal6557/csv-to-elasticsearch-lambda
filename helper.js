
// const BUCKET = 'lambda-test-upload123';
const S3 = require('./services/s3');
const searchClient = require('./services/es');
const sns = require('./services/sns');

// check connection of elasticsearch service
async function connection() {
    return new Promise((resolve, reject) => {
        searchClient.ping({
            // ping usually has a 3000ms timeout
            requestTimeout: 4000
        }, function (error) {
            if (error) {
                return reject(error);
            } else {
                console.log("Elasticsearch service is alive")
                return resolve(true);
            }
        });
    });
}

// get Object from s3 is wrape with promise
async function getObject(Bucket, key) {
try{
    let params = { Bucket: Bucket, Key: key };
    return new Promise((resolve, reject) => {
        S3.getObject(params, function (err, data) {
            if (err) {
                return reject(err);
            }

            return resolve(data);
        });
    });}
    catch(e){
        throw e;
    }
}

// create a bulk Index in ES
async function bulkIndex(jsonBody) {
    try {

        // keep track of time that elastic search takes while doing bulk insert
        console.time("BulkInsert-Elasticsearch");

        await connection();  // checking connection of elasticsearch

        await wrapeBulk(jsonBody);  // calling wrape bulk

        console.log("Inserting into elasticSearch with record count ", jsonBody.length);

        //empty array as we already inserted data in elasticsearch
        jsonBody.length = 0;
        console.timeEnd('BulkInsert-Elasticsearch');

        return jsonBody;
    } catch (e) {
        throw e;
    }
}

// wrapePromise of bulk insert of elasticsearch
async function wrapeBulk(body) {
    try {
        return new Promise((resolve, reject) => {
            searchClient.bulk({
                body: body
            }, function (err, resp) {
                if (err) {
                    return reject(err);
                }
                if (resp) {
                    return resolve(resp);
                }
            });
        });
    }
    catch (e) {
        throw e;
    }
}

// send sns notification to call lambda function with bucket, key, start byte and header
async function snsPublish(bucket, key, byte, header) {
    
    let params = {
        Message: JSON.stringify({
            bucket: bucket,
            key: key,
            byte: byte,
            header: header
        }),
        Subject: "SNS From Lambda",
        TopicArn: process.env.TopicArn
    };

    return new Promise((resolve, reject) => {
        sns.publish(params, function (err, data) {
            if (err) {
                return reject(err);
            }
            else {
                return resolve(data);
            }
        });
    });
}

// check is string is json or not
function IsJsonString(str) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}

// convert CSV record to JSON array for elasticsearch
function convertToJSON(header, line, elasticBody) {
    let body = {};
    let currentline = line.split(",");

    let indexics = { create: { _index: 'myindex', _type: 'mytype', _id: currentline[0] } }

    for (let j = 1; j < header.length; j++) {
        body[header[j]] = currentline[j];
    }

    elasticBody.push(indexics);
    elasticBody.push(body);

    return elasticBody;
}

module.exports = {
    getObject,
    bulkIndex,
    snsPublish,
    IsJsonString,
    convertToJSON
}