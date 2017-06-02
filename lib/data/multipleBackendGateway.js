const { errors } = require('arsenal');
const UUID = require('node-uuid');

const werelogs = require('werelogs');
const async = require('async');

const { config } = require('../Config');
const parseLC = require('./locationConstraintParser');

const logger = new werelogs.Logger('MultipleBackendGateway', {
    logLevel: config.log.logLevel,
    dumpLevel: config.log.dumpLevel,
});

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

const clients = parseLC(config);

function _createAwsKey(requestBucketName, requestObjectKey,
    bucketMatch) {
    if (bucketMatch) {
        return requestObjectKey;
    }
    return `${requestBucketName}/${requestObjectKey}`;
}

function checkAWSHealth(client, location, multBackendResp, callback) {
    client.headBucket({ Bucket: client.awsBucketName },
    err => {
        /* eslint-disable no-param-reassign */
        if (err) {
            multBackendResp[location] = { error: err };
            return callback(null, multBackendResp);
        }
        return client.getBucketVersioning({
            Bucket: client.awsBucketName },
        (err, data) => {
            if (err) {
                multBackendResp[location] = { error: err };
            } else if (!data.Status ||
                data.Status === 'Suspended') {
                multBackendResp[location] = {
                    versioningStatus: data.Status,
                    error: 'Versioning must be enabled',
                };
            } else {
                multBackendResp[location] = {
                    versioningStatus: data.Status,
                    message: 'Congrats! You own the bucket',
                };
            }
            return callback(null, multBackendResp);
        });
    });
}

/* eslint-enable no-param-reassign */
const multipleBackendGateway = {
    put: (stream, size, keyContext, backendInfo,
    reqUids, callback) => {
        const controllingLocationConstraint =
            backendInfo.getControllingLocationConstraint();
        const client = clients[controllingLocationConstraint];
        if (!client) {
            const log = createLogger(reqUids);
            log.error('no data backend matching controlling locationConstraint',
            { controllingLocationConstraint });
            return process.nextTick(() => {
                callback(errors.InternalError);
            });
        }
        // client is AWS SDK
        if (client.clientType === 'aws_s3') {
            const awsKey = _createAwsKey(keyContext.bucketName,
               keyContext.objectKey, client.bucketMatch);
            // TODO: if object to be encrypted, use encryption
            // on AWS
            return client.upload({
                Bucket: client.awsBucketName,
                Key: awsKey,
                Body: stream,
                Metadata: keyContext.metaHeaders,
                ContentLength: size,
            },
               (err, data) => {
                   if (err) {
                       const log = createLogger(reqUids);
                       log.error('err from data backend',
                       { error: err, dataStoreName: client.dataStoreName });
                       return callback(errors.InternalError);
                   }
                   const dataRetrievalInfo = {
                       key: awsKey,
                       dataStoreName: client.dataStoreName,
                       dataStoreType: client.clientType,
                       // because of encryption the ETag here could be
                       // different from our metadata so let's store it
                       // TODO: let AWS handle encryption
                       dataStoreETag: data.ETag,
                   };
                   return callback(null, dataRetrievalInfo);
               });
        }
        return client.put(stream, size, keyContext,
            reqUids, (err, key) => {
                if (err) {
                    const log = createLogger(reqUids);
                    log.error('error from datastore',
                    { error: err, implName: client.clientType });
                    return callback(errors.InternalError);
                }
                const dataRetrievalInfo = {
                    key,
                    dataStoreName: controllingLocationConstraint,
                    dataStoreType: client.clientType,
                };
                return callback(null, dataRetrievalInfo);
            });
    },

    get: (objectGetInfo, range, reqUids, callback) => {
        let key;
        let client;
        // for backwards compatibility
        if (typeof(objectGetInfo) === 'string') {
            key = objectGetInfo;
            client = clients.legacy;
        } else {
            key = objectGetInfo.key;
            client = clients[objectGetInfo.dataStoreName];
        }
        if (client.clientType === 'scality') {
            return client.get(key, range, reqUids, callback);
        }
        if (client.clientType === 'aws_s3') {
            const stream = client.getObject({
                Bucket: client.awsBucketName,
                Key: key,
                Range: range,
            }).createReadStream().on('error', err => {
                const log = createLogger(reqUids);
                log.error('error creating stream',
                { error: err, implName: client.clientType });
                return callback(errors.InternalError);
            });
            return callback(null, stream);
        }
        return client.get(objectGetInfo, range, reqUids, callback);
    },

    delete: (objectGetInfo, reqUids, callback) => {
        let key;
        let client;
        // for backwards compatibility
        if (typeof(objectGetInfo) === 'string') {
            key = objectGetInfo;
            client = clients.legacy;
        } else {
            key = objectGetInfo.key;
            client = clients[objectGetInfo.dataStoreName];
        }
        if (client.clientType === 'aws_s3') {
            const params = {
                Bucket: client.awsBucketName,
                Key: key,
            };
            return client.deleteObject(params, err => {
                if (err) {
                    const log = createLogger(reqUids);
                    log.error('error deleting object from datastore',
                    { error: err, implName: client.clientType });
                    return callback(errors.InternalError);
                }
                return callback();
            });
        }
        if (client.clientType === 'scality') {
            return client.delete(key, reqUids, callback);
        }
        return client.delete(objectGetInfo, reqUids, callback);
    },

    healthcheck: (log, callback) => {
        const multBackendResp = {};
        const awsArray = [];
        async.each(Object.keys(clients), (location, cb) => {
            const client = clients[location];
            if (client.clientType === 'scality') {
                return client.healthcheck(log, (err, res) => {
                    if (err) {
                        multBackendResp[location] = { error: err };
                    } else {
                        multBackendResp[location] = { code: res.statusCode,
                            message: res.statusMessage };
                    }
                    return cb();
                });
            } else if (client.clientType === 'aws_s3') {
                awsArray.push(location);
                return cb();
            }
            // if backend type isn't 'scality' or 'aws_s3', it will be
            //  'mem' or 'file', for which the default response is 200 OK
            multBackendResp[location] = { code: 200, message: 'OK' };
            return cb();
        }, () => {
            if (awsArray.length > 0) {
                const randomAWS = awsArray[Math.floor(Math.random() *
                    awsArray.length)];
                const checkThisOne = clients[randomAWS];
                return checkAWSHealth(checkThisOne, randomAWS, multBackendResp,
                    callback);
            }
            return callback(null, multBackendResp);
        });
    },

    createMPU: (key, bucket, metaHeaders, websiteRedirectHeader,
    location, log, cb) => {
        const client = clients[location];
        const awsKey = _createAwsKey(bucket, key, client.bucketMatch);
        if (client.clientType === 'aws_s3') {
            const params = { Bucket: client.awsBucketName, Key: awsKey,
                WebsiteRedirectLocation: websiteRedirectHeader,
                Metadata: metaHeaders };
            return client.createMultipartUpload(params, (err, data) => {
                if (err) {
                    log.error('err from data backend',
                    { error: err, dataStore: client.dataStoreName });
                    return cb(errors.InternalError);
                }
                return cb(null, data);
            });
        }
        const scalResponse = {};
        scalResponse.UploadId = UUID.v4().replace(/-/g, '');
        return cb(null, scalResponse);
    },

    uploadPart: (bucket, location, key, uploadId, partNumber, log, cb) => {
        const client = clients[location];
        if (client.clientType === 'aws_s3') {
            const awsKey = _createAwsKey(bucket, key, client.bucketMatch);
            const params = { Bucket: client.awsBucketName, Key: awsKey,
                UploadId: uploadId, PartNumber: partNumber };
            return client.uploadPart(params, (err, data) => {
                if (err) {
                    log.error('err from data backend',
                    { error: err, dataStoreName: client.dataStoreName });
                    return cb(errors.InternalError);
                }
                const dataRetrievalInfo = {
                    key,
                    dataStoreName: client.dataStoreName,
                    dataStoreETag: data.ETag,
                };
                return cb(null, dataRetrievalInfo);
            });
        }
        return cb();
    },

    completeMPU: (key, uploadId, location, jsonList, log, cb) => {
        const client = clients[location];
        const partList = jsonList.Part;
        if (client.clientType === 'aws_s3') {
            const bucket = client.awsBucketName;
            const partArray = [];
            return async.each(partList, (partObj, callback) => {
                const partParams = { PartNumber: partObj.PartNumber[0],
                ETag: partObj.ETag[0] };
                partArray.push(partParams);
                return callback();
            }, () => {
                const mpuParams = {
                    Bucket: bucket, Key: key, UploadId: uploadId,
                    MultipartUpload: {
                        Parts: partArray,
                    },
                };
                return client.completeMultipartUpload(mpuParams,
                (err, data) => {
                    if (err) {
                        log.error('err from data backend',
                        { error: err, dataStoreName: client.dataStoreName });
                        if (err.InvalidPart) {
                            return cb(errors.InvalidPart);
                        } else if (err.InvalidPartOrder) {
                            return cb(errors.InvalidPartOrder);
                        } else if (err.EntityTooSmall) {
                            return cb(errors.InvalidPartOrder);
                        }
                        return cb(errors.InternalError);
                    }
                    return cb(null, data);
                });
            });
        }
        return cb();
    },
};

module.exports = multipleBackendGateway;
