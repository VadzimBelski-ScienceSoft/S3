const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { checkOneVersion } = require('../../lib/utility/versioning-util');

const {
    removeAllVersions,
    versioningEnabled,
} = require('../../lib/utility/versioning-util');

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';

function _checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.statusCode, statusCode);
}


describe('Put object tagging with versioning', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _assertOneNullVersion(callback) {
            return s3.listObjectVersions({ Bucket: bucketName },
                (err, data) => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(data.Versions.length, 1);
                    assert.strictEqual(data.Versions[0].VersionId,
                        'null');
                    callback();
                });
        }

        beforeEach(done => s3.createBucket({ Bucket: bucketName }, done));
        afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, done);
            });
        });

        it('should be able to put tag with versioning', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  (err, data) => next(err, data.VersionId)),
                (versionId, next) => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data, versionId)),
            ], (err, data, versionId) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(data.VersionId, versionId);
                done();
            });
        });

        it('should not create version puting object tags on a ' +
        ' version-enabled bucket where no version id is specified ', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  (err, data) => next(err, data.VersionId)),
                (versionId, next) => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, err => next(err, versionId)),
                (versionId, next) => s3.listObjectVersions({
                    Bucket: bucketName,
                }, (err, data) => next(err, data, versionId)),
            ], (err, data, versionId) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                checkOneVersion(data, versionId);
                done();
            });
        });

        it('should be able to put tag with a version of id "null"', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'null',
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], (err, data) => {
                assert.ifError(err, `Found unexpected err ${err}`);
                assert.strictEqual(data.VersionId, 'null');
                done();
            });
        });

        it('putting tag with a version of id "null" should not create ' +
        'extra null version if master version is null', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => _assertOneNullVersion(next),
                next => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'null',
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
                // in case putting tagging created extra version in metadata,
                // we would not see it in s3 listing if the version id is the
                // same as the master version id. putting another null version
                // replaces master version id.
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => _assertOneNullVersion(next),
            ], done);
        });

        it('should return InvalidArgument putting tag with a non existing ' +
        'version id', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                err => next(err)),
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: 'notexisting',
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'InvalidArgument', 400);
                done();
            });
        });

        it('should return 405 MethodNotAllowed putting tag without ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });

        it('should return 405 MethodNotAllowed putting tag with ' +
         'version id if version specified is a delete marker', done => {
            async.waterfall([
                next => s3.putBucketVersioning({ Bucket: bucketName,
                  VersioningConfiguration: versioningEnabled },
                  err => next(err)),
                next => s3.putObject({ Bucket: bucketName, Key: objectName },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName, Key: objectName },
                  (err, data) => next(err, data.VersionId)),
                (versionId, next) => s3.putObjectTagging({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    Tagging: { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        }] },
                }, (err, data) => next(err, data)),
            ], err => {
                _checkError(err, 'MethodNotAllowed', 405);
                done();
            });
        });
    });
});
