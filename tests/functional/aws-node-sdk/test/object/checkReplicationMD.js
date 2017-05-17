import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import { metadata } from '../../../../../lib/metadata/bucketfile/backend';
import { removeAllVersions, versioningEnabled } from
    '../../lib/utility/versioning-util.js';

const sourceBucket = 'source-bucket';
const destinationBucket = 'destination-bucket';
const keyA = 'key-A';

describe('PUT object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        const versioningParams = {
            Bucket: sourceBucket,
            VersioningConfiguration: versioningEnabled,
        };

        const replicationParams = {
            Bucket: sourceBucket,
            ReplicationConfiguration: {
                Role: 'arn:partition:service::account-id:resourcetype/resource',
                Rules: [
                    {
                        Destination: {
                            Bucket: `arn:aws:s3:::${destinationBucket}`,
                        },
                        Prefix: keyA,
                        Status: 'Enabled',
                    },
                ],
            },
        };

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            return s3.createBucketAsync({ Bucket: sourceBucket })
                .then(() => s3.putBucketVersioningAsync(versioningParams))
                .then(() => s3.putBucketReplicationAsync(replicationParams))
                .catch(err => {
                    throw err;
                });
        });

        afterEach(done =>
            removeAllVersions({ Bucket: sourceBucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: sourceBucket }, done);
            }));

        it.only('should put set object replication status to \'PENDING\'', done =>
            s3.putObject({
                Bucket: sourceBucket,
                Key: keyA,
            }, err => {
                const objectReplicationMD = metadata.keyMaps
                    // .get(sourceBucket)
                    // .get(keyA).replication;
                console.log(objectReplicationMD);
                assert.equal(err, null, 'Expected success, ' +
                `got error ${JSON.stringify(err)}`);
                done();
            }));
    });
});
