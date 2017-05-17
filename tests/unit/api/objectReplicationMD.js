const assert = require('assert');
const async = require('async');

const { cleanup, DummyRequestLogger, makeAuthInfo, TaggingConfigTester } =
    require('../helpers');
const { metadata } = require('../../../lib/metadata/in_memory/metadata');
const DummyRequest = require('../DummyRequest');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectDelete = require('../../../lib/api/objectDelete');
const bucketPutReplication = require('../../../lib/api/bucketPutReplication');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const objectPut = require('../../../lib/api/objectPut');
const objectPutACL = require('../../../lib/api/objectPutACL');
const objectPutTagging = require('../../../lib/api/objectPutTagging');
const objectDeleteTagging = require('../../../lib/api/objectDeleteTagging');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const ownerID = authInfo.getCanonicalID();
const namespace = 'default';
const bucketName = 'source-bucket';
const bucketARN = `arn:aws:s3:::${bucketName}`;
const keyA = 'key-A';
const keyB = 'key-B';

const putBucketReq = new DummyRequest({
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}`,
});

const replicationReq = new DummyRequest({
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}?replication`,
    query: { replication: '' },
    post:
    '<ReplicationConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<Role>arn:partition:service::account-id:resourcetype/resource</Role>' +
        '<Rule>' +
            `<Prefix>${keyA}</Prefix>` +
            '<Status>Enabled</Status>' +
            '<Destination>' +
                `<Bucket>${bucketARN}</Bucket>` +
                '<StorageClass>STANDARD</StorageClass>' +
            '</Destination>' +
        '</Rule>' +
    '</ReplicationConfiguration>',
});

const versioningReq = new DummyRequest({
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}?versioning`,
    query: { versioning: '' },
    post:
    '<VersioningConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<Status>Enabled</Status>' +
    '</VersioningConfiguration>',
});

const deleteReq = new DummyRequest({
    bucketName,
    namespace,
    objectKey: keyA,
    headers: {},
    url: `/${bucketName}/${keyA}`,
});

const objectACLReq = {
    bucketName,
    namespace,
    objectKey: keyA,
    headers: {
        'x-amz-grant-read': `id=${ownerID}`,
        'x-amz-grant-read-acp': `id=${ownerID}`,
    },
    url: `/${bucketName}/${keyA}?acl`,
    query: { acl: '' },
};

// Get an object request with the given key.
function getObjectRequest(key) {
    return new DummyRequest({
        bucketName,
        namespace,
        objectKey: key,
        headers: {},
        url: `/${bucketName}/${key}`,
    }, Buffer.from('body content', 'utf8'));
}

const taggingPutReq = new TaggingConfigTester()
    .createObjectTaggingRequest('PUT', bucketName, keyA);
const taggingDeleteReq = new TaggingConfigTester()
    .createObjectTaggingRequest('DELETE', bucketName, keyA);

const emptyReplicationMD = {
    status: '',
    content: [],
    destination: '',
    storageClass: '',
};

const newReplicationMD = {
    status: 'PENDING',
    content: ['DATA', 'METADATA'],
    destination: bucketARN,
    storageClass: 'STANDARD',
};

const replicateMetadataOnly =
    Object.assign({}, newReplicationMD, { content: ['METADATA'] });

// Check that the object key has the expected replication information.
function checkObjectMD(key, expected) {
    const objectMD = metadata.keyMaps.get(bucketName).get(key);
    assert.deepStrictEqual(objectMD.replicationInfo, expected);
}

// Put the object key and check the replication information.
function putObjectAndCheckMD(key, expected, cb) {
    const request = getObjectRequest(key);
    return objectPut(authInfo, request, undefined, log, err => {
        if (err) {
            return cb(err);
        }
        checkObjectMD(key, expected);
        return cb();
    });
}

describe('Replication object MD without bucket replication config', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, putBucketReq, log, done);
    });

    afterEach(() => cleanup());

    it('should not update object metadata', done =>
        putObjectAndCheckMD(keyA, emptyReplicationMD, done));

    it('should not update object metadata if putting object ACL', done =>
        async.series([
            next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
            next => objectPutACL(authInfo, objectACLReq, log, next),
        ], err => {
            if (err) {
                return done(err);
            }
            checkObjectMD(keyA, emptyReplicationMD);
            return done();
        }));

    it('should not update object metadata if putting a delete marker', done =>
        async.series([
            next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
            next => objectDelete(authInfo, deleteReq, log, next),
        ], err => {
            if (err) {
                return done(err);
            }
            assert.strictEqual(err, null);
            const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
            assert.strictEqual(objectMD, undefined, 'Expected object to be ' +
                `deleted, but found object metadata ${objectMD}`);
            return done();
        }));

    describe('Object tagging', () => {
        beforeEach(done => async.series([
            next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
            next => objectPutTagging(authInfo, taggingPutReq, log, next),
        ], err => done(err)));

        it('should not update object metadata if putting tag', done => {
            checkObjectMD(keyA, emptyReplicationMD);
            return done();
        });

        it('should not update object metadata if deleting tag', done =>
            async.series([
                // Put a new version to update replication MD content array.
                next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
                next => objectDeleteTagging(authInfo, taggingDeleteReq, log,
                    next),
            ], err => {
                if (err) {
                    return done(err);
                }
                checkObjectMD(keyA, emptyReplicationMD);
                return done();
            }));
    });
});

describe('Replication object MD with bucket replication config', () => {
    beforeEach(done => {
        cleanup();
        async.series([
            next => bucketPut(authInfo, putBucketReq, log, next),
            next => bucketPutVersioning(authInfo, versioningReq, log, next),
            next => bucketPutReplication(authInfo, replicationReq, log, next),
        ], err => done(err));
    });

    afterEach(() => cleanup());

    it('should update metadata when replication config prefix applies', done =>
        putObjectAndCheckMD(keyA, newReplicationMD, done));

    it('should not update metadata when replication config prefix does not ' +
        'apply', done => putObjectAndCheckMD(keyB, emptyReplicationMD, done));

    it("should update status to 'PENDING' if putting a new version", done =>
        putObjectAndCheckMD(keyA, newReplicationMD, err => {
            if (err) {
                return done(err);
            }
            const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
            // Update metadata to a status after replication has occurred.
            objectMD.replicationInfo.status = 'COMPLETED';
            return putObjectAndCheckMD(keyA, newReplicationMD, done);
        }));

    it("should update status to 'PENDING' and content to '['METADATA']' " +
        'if putting object ACL', done =>
        async.series([
            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
            next => objectPutACL(authInfo, objectACLReq, log, next),
        ], err => {
            if (err) {
                return done(err);
            }
            checkObjectMD(keyA, replicateMetadataOnly);
            return done();
        }));

    it('should update metadata if putting a delete marker', done =>
        async.series([
            next => putObjectAndCheckMD(keyA, newReplicationMD, err => {
                if (err) {
                    return next(err);
                }
                const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                // Update metadata to a status after replication has occurred.
                objectMD.replicationInfo.status = 'COMPLETED';
                return next();
            }),
            next => objectDelete(authInfo, deleteReq, log, next),
        ], err => {
            if (err) {
                return done(err);
            }
            const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
            assert.strictEqual(objectMD.isDeleteMarker, true);
            checkObjectMD(keyA, replicateMetadataOnly);
            return done();
        }));

    describe('Object tagging', () => {
        beforeEach(done => async.series([
            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
            next => objectPutTagging(authInfo, taggingPutReq, log, next),
        ], err => done(err)));

        it("should update status to 'PENDING' and content to '['METADATA']' " +
            "if putting tag", done => {
                checkObjectMD(keyA, replicateMetadataOnly);
                return done();
            });

        it("should update status to 'PENDING' and content to '['METADATA']' " +
            "if deleting tag", done =>
            async.series([
                // Put a new version to update replication MD content array.
                next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                next => objectDeleteTagging(authInfo, taggingDeleteReq, log,
                    next),
            ], err => {
                if (err) {
                    return done(err);
                }
                checkObjectMD(keyA, replicateMetadataOnly);
                return done();
            }));
    });
});
