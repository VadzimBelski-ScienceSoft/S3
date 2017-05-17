import assert from 'assert';
import async from 'async';
import { errors } from 'arsenal';

import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import { metadata } from '../../../lib/metadata/in_memory/metadata';
import DummyRequest from '../DummyRequest';
import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import bucketPutReplication from '../../../lib/api/bucketPutReplication';
import bucketPutVersioning from '../../../lib/api/bucketPutVersioning';
import objectPut from '../../../lib/api/objectPut';
import objectPutACL from '../../../lib/api/objectPutACL';

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

const objectACLReq = {
    bucketName,
    namespace,
    objectKey: keyA,
    headers: {
        'x-amz-grant-full-control':
            'emailaddress="sampleaccount1@sampling.com"' +
            ',emailaddress="sampleaccount2@sampling.com"',
        'x-amz-grant-read': `uri=${constants.logId}`,
        'x-amz-grant-read-acp': `id=${ownerID}`,
        'x-amz-grant-write-acp': `id=${anotherID}`,
    },
    url: `/${bucketName}/${keyA}?acl`,
    query: { acl: '' },
};

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

const noReplicationMD = {
    status: '',
    content: [],
    destination: '',
    storageClass: '',
};

function checkObjectMetadata(key, expected, cb) {
    const request = objectRequest(key);

    objectPut(authInfo, request, undefined, log, err => {
        if (err) {
            return cb(err);
        }
        const objectMD = metadata.keyMaps.get(bucketName).get(key);
        assert.deepStrictEqual(objectMD.replication, expected,
            'Got unexpected replication object metadata');
        return cb();
    });
}

describe.only('Replication object metdata', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, putBucketReq, log, done);
    });

    it('should not update when bucket does not have replication config',
        done => checkObjectMetadata(keyA, noReplicationMD, done));

    describe('After put bucket replication configuration', () => {
        beforeEach(done => async.series([
            next => bucketPutVersioning(authInfo, versioningReq, log, next),
            next => bucketPutReplication(authInfo, replicationReq, log, next),
        ], err => done(err)));

        afterEach(() => cleanup());

        it('should update when replication config prefix applies',
            done => checkObjectMetadata(keyA, newReplicationMD, done));

        it('should not update when replication config prefix does not apply',
            done => checkObjectMetadata(keyB, noReplicationMD, done));

        it('should update status to \'PENDING\' if putting a new version',
            done => checkObjectMetadata(keyA, newReplicationMD, err => {
                if (err) {
                    return done(err);
                }
                // Update metadata to status after replication has occurred.
                const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                objectMD.replication.status = 'COMPLETED';
                return checkObjectMetadata(keyA, newReplicationMD, done);
            }));

        it('should update status to \'PENDING\' and content to ' +
            '\'[\'METADATA\']\' if putting object ACL',
            done => checkObjectMetadata(keyA, newReplicationMD, err => {
                if (err) {
                    return done(err);
                }
                return objectPutACL(authInfo, objectACLReq, log, err => {
                    if (err) {
                        return done(err);
                    }
                    return checkObjectMetadata(keyA, newReplicationMD, done);
                });
            }));
    });
});

// it('should update status to \'PENDING\' and content to ' +
//     '\'[\'METADATA\']\' if putting object tagging',
//     done => checkObjectMetadata(keyA, newReplicationMD, err => {
//         if (err) {
//             return done(err);
//         }
//         // Update metadata to status after replication has occurred.
//         const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
//         objectMD.replication.status = 'COMPLETED';
//         return checkObjectMetadata(keyA, newReplicationMD, done);
//     }));
