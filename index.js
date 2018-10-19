const HelperController = require('./helper');
const s3 = require('./services/s3');
const es = require('event-stream');

exports.handler = async (event, context) => {
    try {
        let bucket;
        let key;
        let elasticBody = []; // for bulk Insert body of elasticsearch
        let header; // header of CSV to map with other line record
        let lineNumber = 0; // keep track of line
        let completedByte = 0; // keep track of parse of byte to elastic search
        let callSNS = false; // keep track if need to call SNS
        let startByte = 0; // gives the starting point to fetch byte from s3 object.

        if (event) {
            bucket = event.bucket;
            key = event.key;
        }
        else if (event.Records[0] && event.Records[0].Sns) {

            let message = event.Records[0].Sns.Message;
            console.log(`SNS is called with payload ${message}`);

            // string not json convert to json if json don't do anything
            if (HelperController.IsJsonString(message)) {
                message = JSON.parse(message);
            }

            bucket = message.bucket
            key = message.key;
            startByte = message.byte;
            header = message.header;
        }


        console.log(`Calling bucket ${bucket} with key ${key} byte starting from ${startByte} with header ${header}`);

        const params = {
            Bucket: bucket,
            Key: key,
            Range: `bytes=${startByte}-`
        };

        // get object from s3 using stream
        let readStream = await s3.getObject(params).createReadStream();

        return new Promise(async (resolve, reject) => {

            readStream.pipe(es.split()) // split each line of csv 
                .pipe(es.through(async function write(line) {
                    try {
                        startByte += line.length; // keeping track of byte proccessed
                        
                        if (line) {
                            // first line of csv it will come here to get the header if header is empty 
                            if (!header) {
                                header = line.split(",");
                            }
                            else {

                                let elasticReturn = HelperController.convertToJSON(header, line, elasticBody);
                                elasticBody = elasticReturn;
                                
                                lineNumber++;

                                if (lineNumber != 0 && lineNumber % 20000 == 0) {

                                    console.log("Processing line number ", lineNumber, "Byte ", startByte);

                                    this.pause() // pause the stream and wait for bulkIndex of elasticsearch to complete 
                                    let elasticInsert = await HelperController.bulkIndex(elasticBody, true);
                                    elasticBody = elasticInsert; // here elasticInsert will be empty array as we already everything in array to elasticsearch
                                    completedByte = startByte; // track of byte that is been processed till now
                                    this.resume() // resuming the stream
                                } 

                                // if lambda function remaining time is less than 30 seconds than call SNS to continue process 
                                if (context.getRemainingTimeInMillis() < 30000 && (!callSNS)) {
                                    console.log(`Calling SNS with line ${lineNumber}`);
                                    callSNS = true;
                                    this.end();
                                }
                            }
                        }
                    } catch (e) {
                        return reject(true);
                    }
                }))
                .on('end', async function (end) {
                    try {
                        if (elasticBody.length != 0 && (!callSNS)) {
                            console.log('Completed processing file ' + key + ' with length ', elasticBody.length, 'complete ');
                            let elasticComplete = await HelperController.bulkIndex(elasticBody, true);
                            elasticBody = elasticComplete;
                            return resolve(true);
                        } else {
                            console.log(`Calling SNS with line ${lineNumber} bucket ${bucket} key ${key} and completed byte ${completedByte}`);
                            await HelperController.snsPublish(bucket, key, completedByte, header);
                            return resolve(true);
                        }
                    } catch (e) {
                        return reject(true)
                    }
                })
                .on('error', function (err) {
                    return reject(err);
                })
        })
    } catch (error) {
        console.error(error);
        throw error;
    }
};