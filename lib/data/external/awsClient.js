const { errors } = require('arsenal');

function _createAwsKey(requestBucketName, requestObjectKey,
    bucketMatch) {
    if (bucketMatch) {
        return requestObjectKey;
    }
    return `${requestBucketName}/${requestObjectKey}`;
}

const awsClient = {
    put: (client, stream, size, keyContext, log, callback) => {
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
    },
    get: (client, key, range, log, callback) => {
        const stream = client.getObject({
            Bucket: client.awsBucketName,
            Key: key,
            Range: range,
        }).createReadStream().on('error', err => {
            log.error('error streaming data', { error: err,
                dataStoreName: client.dataStoreName });
            return callback(errors.InternalError);
        });
        return callback(null, stream);
    },
    delete: (client, key, log, callback) => {
        const params = {
            Bucket: client.awsBucketName,
            Key: key,
        };
        return client.deleteObject(params, err => {
            if (err) {
                log.error('error deleting object from datastore',
                { error: err, implName: client.clientType });
                return callback(errors.InternalError);
            }
            return callback();
        });
    },
};

module.exports = awsClient;
