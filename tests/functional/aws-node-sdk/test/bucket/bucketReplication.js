import assert from 'assert';
import { S3 } from 'aws-sdk';
import { series } from 'async';

import getConfig from '../support/config';
import { replicationUtils } from '../../lib/utility/replication';

const bucket = 'bennett-src-bucket';
const destinationBucket = 'bennett-dest-bucket';

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    const expectedStatusCode = expectedErr === 'NoSuchBucket' ? 404 : 400;
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.code}'`);
        assert.strictEqual(err.statusCode, expectedStatusCode, 'incorrect ' +
            `error status code: should be 400 but got '${err.statusCode}'`);
    }
}

// Get parameters for putBucketReplication.
function getReplicationParams(config) {
    return {
        Bucket: bucket,
        ReplicationConfiguration: config,
    };
}

// Get parameters for putBucketVersioning.
function getVersioningParams(status) {
    return {
        Bucket: bucket,
        VersioningConfiguration: {
            Status: status,
        },
    };
}

// Get a complete replication configuration, or remove the specified property.
const replicationConfig = {
    Role: 'arn:partition:service::account-id:resourcetype/resource',
    Rules: [
        {
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: 'STANDARD',
            },
            Prefix: 'test-prefix',
            Status: 'Enabled',
            ID: 'test-id',
        },
    ],
};

// Set the rules array of a configuration or a property of the first rule.
function setConfigRules(val) {
    const config = Object.assign({}, replicationConfig);
    config.Rules = Array.isArray(val) ? val :
        [Object.assign({}, config.Rules[0], val)];
    return config;
}

describe('aws-node-sdk test putBucketReplication bucket status', () => {
    let s3;
    const replicationParams = getReplicationParams(replicationConfig);

    function checkVersioningError(versioningStatus, expectedErr, cb) {
        const versioningParams = getVersioningParams(versioningStatus);
        return series([
            next => s3.putBucketVersioning(versioningParams, next),
            next => s3.putBucketReplication(replicationParams, next),
        ], err => {
            assertError(err, expectedErr);
            return cb();
        });
    }

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        return done();
    });

    it('should return \'NoSuchBucket\' error if bucket does not exist', done =>
        s3.putBucketReplication(replicationParams, err => {
            assertError(err, 'NoSuchBucket');
            return done();
        }));

    describe('test putBucketReplication bucket versioning status', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should not put configuration on bucket without versioning', done =>
            s3.putBucketReplication(replicationParams, err => {
                assertError(err, 'InvalidRequest');
                return done();
            }));

        it('should not put configuration on bucket with \'Suspended\'' +
            'versioning', done =>
            checkVersioningError('Suspended', 'InvalidRequest', done));

        it('should put configuration on a bucket with versioning', done =>
            checkVersioningError('Enabled', null, done));
    });
});

describe('aws-node-sdk test putBucketReplication configuration rules', () => {
    let s3;

    function checkError(config, expectedErr, cb) {
        const replicationParams = getReplicationParams(config);
        s3.putBucketReplication(replicationParams, err => {
            assertError(err, expectedErr);
            return cb();
        });
    }

    beforeEach(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        return series([
            next => s3.createBucket({ Bucket: bucket }, next),
            next =>
                s3.putBucketVersioning(getVersioningParams('Enabled'), next),
        ], err => done(err));
    });

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    replicationUtils.invalidRoleARNs.forEach(ARN => {
        const config = Object.assign({}, replicationConfig, { Role: ARN });

        it('should not accept configuration when \'Role\' is not a ' +
            `valid Amazon Resource Name format: ${ARN}`, done =>
            checkError(config, 'InvalidArgument', done));
    });

    replicationUtils.validRoleARNs.forEach(ARN => {
        const config = Object.assign({}, replicationConfig, { Role: ARN });

        it('should accept configuration when \'Role\' is a valid Amazon ' +
            `Resource Name format: ${ARN}`, done =>
            checkError(config, null, done));
    });

    replicationUtils.invalidBucketARNs.forEach(ARN => {
        const config = setConfigRules({ Destination: { Bucket: ARN } });

        it('should not accept configuration when \'Bucket\' is not a ' +
            `valid Amazon Resource Name format: ${ARN}`, done =>
            checkError(config, 'InvalidArgument', done));
    });

    it('should not accept configuration when \'Rules\' is empty ', done => {
        const config = Object.assign({}, replicationConfig, { Rules: [] });
        return checkError(config, 'MalformedXML', done);
    });

    it('should not accept configuration when \'Rules\' is > 1000', done => {
        const arr = [];
        for (let i = 0; i < 1001; i++) {
            arr.push({
                Destination: { Bucket: destinationBucket },
                Prefix: `prefix-${i}`,
                Status: 'Enabled',
            });
        }
        const config = setConfigRules(arr);
        return checkError(config, 'InvalidRequest', done);
    });

    it('should not accept configuration when \'ID\' length is > 255', done => {
        // Set ID to a string of length 256.
        const config = setConfigRules({ ID: new Array(257).join('x') });
        return checkError(config, 'InvalidArgument', done);
    });

    it('should not accept configuration when \'ID\' is not unique', done => {
        const rule1 = replicationConfig.Rules[0];
        // Prefix is unique, but not the ID.
        const rule2 = Object.assign({}, rule1, { Prefix: 'bar' });
        const config = setConfigRules([rule1, rule2]);
        return checkError(config, 'InvalidRequest', done);
    });

    replicationUtils.validStatuses.forEach(status => {
        const config = setConfigRules({ Status: status });

        it(`should accept configuration when \'Role\' is ${status}`, done =>
            checkError(config, null, done));
    });

    it('should not accept configuration when \'Status\' is invalid', done => {
        // Status must either be 'Enabled' or 'Disabled'.
        const config = setConfigRules({ Status: 'Invalid' });
        return checkError(config, 'MalformedXML', done);
    });

    it('should not accept configuration when \'Prefix\' length is > 1024',
        done => {
            // Set Prefix to a string of length of 1025.
            const config = setConfigRules({
                Prefix: new Array(1026).join('x'),
            });
            return checkError(config, 'InvalidArgument', done);
        });

    it('should not accept configuration when two rules contain the same ' +
        '\'Prefix\' value', done => {
        const rule = replicationConfig.Rules[0];
        const config = setConfigRules([rule, rule]);
        return checkError(config, 'InvalidRequest', done);
    });

    it('should not accept configuration when \'Destination\' properties of ' +
        'two or more rules specify different buckets', done => {
        const config = setConfigRules([replicationConfig.Rules[0], {
            Destination: { Bucket: `arn:aws:s3:::${destinationBucket}-1` },
            Prefix: 'bar',
            Status: 'Enabled',
        }]);
        return checkError(config, 'InvalidRequest', done);
    });

    replicationUtils.validStorageClasses.forEach(storageClass => {
        const config = setConfigRules({
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: storageClass,
            },
        });

        it('should accept configuration when \'StorageClass\' is ' +
            `${storageClass}`, done => checkError(config, null, done));
    });

    it('should not accept configuration when \'StorageClass\' is invalid',
        done => {
            const config = setConfigRules({
                Destination: {
                    Bucket: `arn:aws:s3:::${destinationBucket}`,
                    StorageClass: 'INVALID',
                },
            });
            return checkError(config, 'MalformedXML', done);
        });
});
