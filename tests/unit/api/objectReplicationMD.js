import assert from 'assert';
import async from 'async';
import { errors } from 'arsenal';

import { cleanup, DummyRequestLogger, makeAuthInfo, TaggingConfigTester } from
    '../helpers';
import { metadata } from '../../../lib/metadata/in_memory/metadata';
import DummyRequest from '../DummyRequest';
import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import bucketPutReplication from '../../../lib/api/bucketPutReplication';
import bucketPutVersioning from '../../../lib/api/bucketPutVersioning';
import objectPut from '../../../lib/api/objectPut';
import objectPutACL from '../../../lib/api/objectPutACL';
import objectPutTagging from '../../../lib/api/objectPutTagging';
import objectDeleteTagging from '../../../lib/api/objectDeleteTagging';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const ownerID = authInfo.getCanonicalID();
const anotherID = '79a59df900b949e55d96a1e698fba' +
    'cedfd6e09d98eacf8f8d5218e7cd47ef2bf';
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

function objectACLReq(readId, readACPId) {
    return {
        bucketName,
        namespace,
        objectKey: keyA,
        headers: {
            'x-amz-grant-read': `id=${readId}`,
            'x-amz-grant-read-acp': `id=${readACPId}`,
        },
        url: `/${bucketName}/${keyA}?acl`,
        query: { acl: '' },
    };
}

const taggingPutReq = new TaggingConfigTester()
    .createObjectTaggingRequest('PUT', bucketName, keyA);
const taggingDeleteReq = new TaggingConfigTester()
    .createObjectTaggingRequest('DELETE', bucketName, keyA);

function objectRequest(key) {
    return new DummyRequest({
        bucketName,
        namespace,
        objectKey: key,
        headers: {},
        url: `/${bucketName}/${key}`,
    }, Buffer.from('body content', 'utf8'));
}

const newReplicationMD = {
    status: 'PENDING',
    content: ['DATA', 'METADATA'],
    destination: bucketARN,
};

const replicateMetadataOnly = Object.assign({}, newReplicationMD,
    { content: ['METADATA'] });

const noReplicationMD = {
    status: '',
    content: [],
    destination: '',
    storageClass: '',
};

function checkObjectMD(key, expected) {
    const objectMD = metadata.keyMaps.get(bucketName).get(key);
    assert.deepStrictEqual(objectMD.replication, expected,
        'Got unexpected replication object metadata');
}

function putObjectAndCheckMD(key, expected, cb) {
    const request = objectRequest(key);

    objectPut(authInfo, request, undefined, log, err => {
        if (err) {
            return cb(err);
        }
        checkObjectMD(key, expected);
        return cb();
    });
}

describe('Replication object MD without replication configuration', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, putBucketReq, log, done);
    });

    it('should not update when bucket does not have replication config',
        done => putObjectAndCheckMD(keyA, noReplicationMD, done));

    it('should not update update metadata if putting object ACL',
        done => async.series([
            next => putObjectAndCheckMD(keyA, noReplicationMD, next),
            next => objectPutACL(authInfo, objectACLReq(ownerID, ownerID), log,
                next),
        ], err => {
            if (err) {
                return done(err);
            }
            checkObjectMD(keyA, noReplicationMD);
            return done();
        }));

    describe('Object tagging', () => {
        beforeEach(done => async.series([
            next => putObjectAndCheckMD(keyA, noReplicationMD, next),
            next => objectPutTagging(authInfo, taggingPutReq, log, next),
        ], err => done(err)));

        it('should not update metadata if putting tag',
            done => {
                checkObjectMD(keyA, noReplicationMD);
                return done();
            });

        it('should not update metadata if deleting tag',
            done => async.series([
                // Put a new version to update replication MD content array.
                next => putObjectAndCheckMD(keyA, noReplicationMD, next),
                next => objectDeleteTagging(authInfo, taggingDeleteReq, log,
                    next),
            ], err => {
                if (err) {
                    return done(err);
                }
                checkObjectMD(keyA, noReplicationMD);
                return done();
            }));
    });
});

describe('Replication object MD with bucket replication configuration', () => {
    beforeEach(done => {
        cleanup();
        async.series([
            next => bucketPut(authInfo, putBucketReq, log, next),
            next => bucketPutVersioning(authInfo, versioningReq, log, next),
            next => bucketPutReplication(authInfo, replicationReq, log, next),
        ], err => done(err));
    });

    afterEach(() => cleanup());

    it('should update when replication config prefix applies',
        done => putObjectAndCheckMD(keyA, newReplicationMD, done));

    it('should not update when replication config prefix does not apply',
        done => putObjectAndCheckMD(keyB, noReplicationMD, done));

    it('should update status to \'PENDING\' if putting a new version',
        done => putObjectAndCheckMD(keyA, newReplicationMD, err => {
            if (err) {
                return done(err);
            }
            // Update metadata to status after replication has occurred.
            const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
            objectMD.replication.status = 'COMPLETED';
            return putObjectAndCheckMD(keyA, newReplicationMD, done);
        }));

    it('should update status to \'PENDING\' and content to ' +
        '\'[\'METADATA\']\' if putting object ACL',
        done => async.series([
            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
            next => objectPutACL(authInfo, objectACLReq(ownerID, ownerID), log,
                next),
        ], err => {
            if (err) {
                return done(err);
            }
            checkObjectMD(keyA, replicateMetadataOnly);
            return done();
        }));

    it('should not update metadata if owner cannot access object resource',
        done => async.series([
            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
            next => objectPutACL(authInfo, objectACLReq(anotherID, ownerID),
                log, next),
        ], err => {
            if (err) {
                return done(err);
            }
            checkObjectMD(keyA, newReplicationMD);
            return done();
        }));

    it('should not update metadata if owner cannot access object ACP',
        done => async.series([
            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
            next => objectPutACL(authInfo, objectACLReq(ownerID, anotherID),
                log, next),
        ], err => {
            if (err) {
                return done(err);
            }
            checkObjectMD(keyA, newReplicationMD);
            return done();
        }));

    describe('Object tagging', () => {
        beforeEach(done => async.series([
            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
            next => objectPutTagging(authInfo, taggingPutReq, log, next),
        ], err => done(err)));

        it('should update status to \'PENDING\' and content to ' +
            '\'[\'METADATA\']\' if putting tag',
            done => {
                checkObjectMD(keyA, replicateMetadataOnly);
                return done();
            });

        it('should update status to \'PENDING\' and content to ' +
            '\'[\'METADATA\']\' if deleting tag',
            done => async.series([
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
