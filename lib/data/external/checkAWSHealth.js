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

module.exports = checkAWSHealth;
