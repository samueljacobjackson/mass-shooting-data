var google = require('googleapis');
var key = require('./key.json');
var Promise = require('bluebird');
var csvjson = require('csvjson');
var AWS = require('aws-sdk');
var _ = require('lodash');

AWS.config.update({
    region: process.env.AWS_REGION || 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT || null
});

var getData = function() {
    return new Promise(function(resolve, reject) {
        var year = 2017;
        var endYear = new Date().getFullYear();
        return promiseWhile(function () {return year <= endYear;}, function(){
            return getFile(year)
            .then(function(result){
                try {
                    var dataString = result.replace(/,(?!(?:[^"]*"[^"]*")*[^"]*$)/mg,"").replace(/['"]+/g, "");
                    var options = {
                        delimiter : ',', 
                        quote : '"'
                    };
                    var driveDataObject = csvjson.toObject(dataString, options);

                    var dynamodbDoc = new AWS.DynamoDB.DocumentClient();
                    var get = Promise.promisify(dynamodbDoc.get, {context: dynamodbDoc});
                    var update = Promise.promisify(dynamodbDoc.update, {context: dynamodbDoc});

                    var DATA_TABLE_NAME = 'MassShootingData';

                    var getDataParams = {
                        TableName : DATA_TABLE_NAME,
                        Key: { 'Year' : year }
                    }

                    return get(getDataParams)
                    .then(function(dbDataObject){

                        var difference = _.differenceBy(driveDataObject, dbDataObject, 'imported')
                        _.forEach(difference, function(o) {
                            o.imported = true;
                        });

                        var filtered = _.filter(dbDataObject, function(o){
                            return o.imported;
                        });

                        var newDataObject = _.concat(filtered, difference);

                        var saveDataParams = {
                            TableName: DATA_TABLE_NAME,
                            Key:{ 'Year': year++ },
                            UpdateExpression: 'set DataSet = :d',
                            ExpressionAttributeValues:{
                                ':d': newDataObject
                            },
                            ReturnValues:'UPDATED_NEW'
                        }
                        return update(saveDataParams)
                        .then(function(data) {
                            if(!data){
                                console.log('Could not save data');
                                reject('Could not save data');
                            }
                            else {resolve(data);}
                        })
                        .catch(function(err) {
                            console.log(err)
                            reject(err);
                        });
                    });
                } catch (err) {
                    console.log(err);
                    reject(err);
                }
            })
        });
    });
}

var getFile = function(year){
    return new Promise(function(resolve, reject){
        var auth = new google.auth.JWT(
            key.client_email,
            null,
            key.private_key,
            ['https://www.googleapis.com/auth/drive.readonly'],
            null
        );

        var drive = google.drive({
            version: 'v3',
            auth: auth
        });

        var years = require('./fileIds.json');

        var fileId = years[year.toString()];
        var respData = '';
        drive.files.get({
            fileId: fileId
        }, function (err, metadata) {
            if (err) {
                return reject(err);
            }
            drive.files.export({
                fileId: fileId,
                mimeType: 'text/csv'
            })
            .on('data', function(chunk){
                respData += chunk;
            })
            .on('end', function(){
                return resolve(respData);
            })
            .on('error', function (err) {
                return reject(err);
            })
        });
    });
}

var promiseWhile = Promise.method(function(condition, action) {
    if (!condition()) {return;};
    return action().then(promiseWhile.bind(null, condition, action));
});

module.exports.getData = getData;
