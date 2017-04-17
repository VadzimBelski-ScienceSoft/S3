import { waterfall } from 'async';
import { errors } from 'arsenal';

import metadata from '../metadata/wrapper';
import { metadataValidateBucket } from '../metadata/metadataUtils';
import { pushMetric } from '../utapi/utilities';
import { getReplicationConfiguration } from
    './apiUtils/bucket/bucketReplication';
import collectCorsHeaders from '../utilities/collectCorsHeaders';

// Check that versioning is 'Enabled' on the given bucket.
function isVersioningEnabled(bucket) {
    const versioningConfig = bucket.getVersioningConfiguration();
    return versioningConfig ? versioningConfig.Status === 'Enabled' : false;
}

// The error response when a bucket does not have versioning 'Enabled'.
const invalidVersioningError = errors.InvalidRequest.customizeDescription(
    'Versioning must be \'Enabled\' on the bucket to apply a replication ' +
    'configuration');

/**
 * bucketPutReplication - Create or update bucket replication configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPutReplication(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutReplication' });
    const { bucketName, post, headers, method } = request;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutReplication',
    };
    return waterfall([
        // Validate the request XML and return the replication configuration.
        next => getReplicationConfiguration(post, log, next),
        // Check bucket user privileges and ensure versioning is 'Enabled'.
        (config, next) =>
            metadataValidateBucket(metadataValParams, log, (err, bucket) => {
                if (err) {
                    return next(err);
                }
                // Replication requires that versioning is 'Enabled'.
                if (!isVersioningEnabled(bucket)) {
                    return next(invalidVersioningError);
                }
                return next(null, config, bucket);
            }),
        // Set the replication configuration and update the bucket metadata.
        (config, bucket, next) => {
            bucket.setReplicationConfiguration(config);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(headers.origin, method, bucket);
        if (err) {
            log.trace('error processing request', {
                error: err,
                method: 'bucketPutReplication',
            });
            return callback(err, corsHeaders);
        }
        pushMetric('bucketPutReplication', log, {
            authInfo,
            bucket: bucketName,
        });
        return callback(null, corsHeaders);
    });
}
