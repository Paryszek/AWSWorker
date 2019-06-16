const express = require('express')
var AWS = require('aws-sdk')
const multer = require('multer');

const app = express()

const port = 4000

AWS.config.update({region: 'eu-central-1'});

const BUCKET_NAME = 's3bucketforlabs';
const USER_KEY = '';
const USER_SECRET = '';
const QUEUEURL = '';

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

var sqsSendMessageParams = {
    DelaySeconds: 10,
    MessageAttributes: {
        "Title": {
        DataType: "String",
        StringValue: "Process"
        },
        "Author": {
        DataType: "String",
        StringValue: "AWS test"
        },
        "WeeksOn": {
        DataType: "Number",
        StringValue: "6"
        }
    },
    MessageBody: "dupa",
    QueueUrl: QUEUEURL
};

var paramsRec = {
    AttributeNames: [
       "All"
    ],
    MaxNumberOfMessages: 10,
    MessageAttributeNames: [
       "All"
    ],
    QueueUrl: "https://sqs.eu-central-1.amazonaws.com/393673436463/awsprocessqueue",
    VisibilityTimeout: 20,
    WaitTimeSeconds: 5
};

const uploadParams = {
    Bucket: BUCKET_NAME,
    Key: '',
    Body: null
};

const listfilesParams = {
    Bucket: BUCKET_NAME,
};

initSqsListeningOnQueue();

function initSqsListeningOnQueue() {
    setInterval(() => {
        sqs.receiveMessage(paramsRec, function(err, data) {
            if (err) {
                console.log("Receive Error", err);
            } else {
                console.log(data);
            }
        });
     }, 30000)
}

function listObjectsS3() {
    s3Client.listObjects(listfilesParams, (err, data) => {
        console.log(data);
    });
}
function uploadObjectS3(file) {
    uploadParams.Key = file.originalname;
    uploadParams.Body = file.buffer;
    s3Client.upload(uploadParams, (err, data) => {        
        if (err) console.log(err);
    
        res.json({message: 'File uploaded successfully','filename': 
        req.file.originalname, 'location': data.Location});
    })
}



app.listen(port, () => console.log(`worker is running on port number ${port}!`))