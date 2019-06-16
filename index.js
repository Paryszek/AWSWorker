const express = require('express')
var AWS = require('aws-sdk')
const multer = require('multer');
const credentials = require("./credentials.json");

const app = express()

const port = 80;

AWS.config.update({region: 'eu-central-1'});

const BUCKET_NAME = credentials.BUCKET_NAME;
const USER_KEY = credentials.USER_KEY;
const USER_SECRET = credentials.USER_SECRET;
const QUEUEURL = credentials.QUEUEURL;

const s3Client = new AWS.S3({
    accessKeyId: USER_KEY,
    secretAccessKey: USER_SECRET,
    Bucket: BUCKET_NAME
});

const sqs = new AWS.SQS({
    apiVersion: '2012-11-05', 
    accessKeyId: USER_KEY,
    secretAccessKey: USER_SECRET
});

var receiveSQSParams = {
    AttributeNames: [
       "All"
    ],
    MaxNumberOfMessages: 10,
    MessageAttributeNames: [
       "All"
    ],
    QueueUrl: QUEUEURL,
    VisibilityTimeout: 20,
    WaitTimeSeconds: 5
};

const uploadS3Params = {
    Bucket: BUCKET_NAME,
    Key: '',
    Body: null
};

const downloadS3Params = {
    Bucket: BUCKET_NAME,
    Key: ''
}

const deleteSQSMessageParams = {
    QueueUrl: QUEUEURL, 
    ReceiptHandle: ''
}

const listfilesParams = {
    Bucket: BUCKET_NAME,
};

initSqsListeningOnQueue();

function process(file) {
    downloadS3Params.Key = file.Body;
    s3Client.getObject(downloadS3Params, (err, data) => {
        if (err) console.log(err, err.stack);
        else {
            console.log(data);
            let copyOfFile = new Buffer(data.Body.toString());    
            let key = "Copy_" + file.Body;
            uploadObjectS3(copyOfFile, key, file.ReceiptHandle);
        }
    })
}

function initSqsListeningOnQueue() {
    setInterval(() => {
        sqs.receiveMessage(receiveSQSParams, function(err, data) {
            if (err) {
                console.log("Receive Error", err);
            } else {
                console.log(data); 
                if (data.Messages)
                    data.Messages.forEach((file) => process(file))                                                     
            }
        });
     }, 30000)
}

function deleteMessageFromSQS(receiptHandle) {
    deleteSQSMessageParams.ReceiptHandle = receiptHandle;
    sqs.deleteMessage(deleteSQSMessageParams, function(err, data) {
        if (err) console.log(err, err.stack); 
        else     console.log(data);           
    });
}

function listObjectsS3() {
    s3Client.listObjects(listfilesS3Params, (err, data) => {
        console.log(data);
    });
}
function uploadObjectS3(file, key, receiptHandle) {
    uploadS3Params.Key = key;
    uploadS3Params.Body = file;
    s3Client.upload(uploadS3Params, (err, data) => {        
        if (err) console.log(err);
        console.log(data); 
        deleteMessageFromSQS(receiptHandle)    
    })
}



app.listen(port, () => console.log(`worker is running on port number ${port}!`))