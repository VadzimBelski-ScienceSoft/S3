const async = require('async');
const assert = require('assert');
const { S3 } = require('aws-sdk');

const getConfig = require('../../test/support/config');
const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const versioningEnabled = { Status: 'Enabled' };
const versioningSuspended = { Status: 'Suspended' };

function _deleteVersionList(versionList, bucket, callback) {
    if (versionList === undefined || versionList.length === 0) {
        return callback();
    }
    const params = { Bucket: bucket, Delete: { Objects: [] } };
    versionList.forEach(version => {
        params.Delete.Objects.push({
            Key: version.Key, VersionId: version.VersionId });
    });

    return s3.deleteObjects(params, callback);
}

function checkOneVersion(data, versionId) {
    assert.strictEqual(data.Versions.length, 1);
    assert.strictEqual(data.Versions[0].VersionId, versionId);
    assert.strictEqual(data.DeleteMarkers.length, 0);
}

function removeAllVersions(params, callback) {
    const bucket = params.Bucket;
    async.waterfall([
        cb => s3.listObjectVersions(params, cb),
        (data, cb) => _deleteVersionList(data.DeleteMarkers, bucket,
            err => cb(err, data)),
        (data, cb) => _deleteVersionList(data.Versions, bucket,
            err => cb(err, data)),
        (data, cb) => {
            if (data.IsTruncated) {
                const params = {
                    Bucket: bucket,
                    KeyMarker: data.NextKeyMarker,
                    VersionIdMarker: data.NextVersionIdMarker,
                };
                return removeAllVersions(params, cb);
            }
            return cb();
        },
    ], callback);
}

function suspendVersioning(bucket, callback) {
    s3.putBucketVersioning({
        Bucket: bucket,
        VersioningConfiguration: versioningSuspended,
    }, callback);
}

function enableVersioning(bucket, callback) {
    s3.putBucketVersioning({
        Bucket: bucket,
        VersioningConfiguration: versioningEnabled,
    }, callback);
}

function enableVersioningThenPutObject(bucket, object, callback) {
    enableVersioning(bucket, err => {
        if (err) {
            callback(err);
        }
        s3.putObject({ Bucket: bucket, Key: object }, callback);
    });
}

/** createDualNullVersion - create a null version that is stored in metadata
 *  both in the master version and a separate version
 *  @param bucketName - name of bucket in versioning suspended state
 *  @param keyName - name of key
 *  @param cb - callback
 */
function createDualNullVersion(bucketName, keyName, cb) {
    async.waterfall([
        // put null version
        next => s3.putObject({ Bucket: bucketName, Key: keyName },
            err => next(err)),
        next => enableVersioning(bucketName, err => next(err)),
        // should store null version as separate version before
        // putting new version
        next => s3.putObject({ Bucket: bucketName, Key: keyName },
            (err, data) => {
                assert.strictEqual(err, null,
                    'Unexpected err putting new version');
                assert(data.VersionId);
                next(null, data.VersionId);
            }),
        // delete version we just created, master version should be updated
        // with value of next most recent version: null version previously put
        (versionId, next) => s3.deleteObject({
            Bucket: bucketName,
            Key: keyName,
            VersionId: versionId,
        }, err => next(err)),
        // getting object should return null version now
        next => s3.getObject({ Bucket: bucketName, Key: keyName },
            (err, data) => {
                assert.strictEqual(err, null,
                    'Unexpected err getting latest version');
                assert.strictEqual(data.VersionId, 'null');
                next();
            }),
    ], err => cb(err));
}

module.exports = {
    checkOneVersion,
    versioningEnabled,
    versioningSuspended,
    suspendVersioning,
    removeAllVersions,
    enableVersioningThenPutObject,
    createDualNullVersion,
};
