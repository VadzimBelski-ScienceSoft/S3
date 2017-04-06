const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');
const { parseString } = require('xml2js');

const { cleanup, DummyRequestLogger, makeAuthInfo } =
    require('../unit/helpers');
const DummyRequest = require('../unit/DummyRequest');
const constants = require('../../constants');
const { config } = require('../../lib/Config');
const metadata = require('../../lib/metadata/in_memory/metadata').metadata;

const { bucketPut } = require('../../lib/api/bucketPut');
const initiateMultipartUpload =
    require('../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../lib/api/objectPutPart');
const completeMultipartUpload =
    require('../../lib/api/completeMultipartUpload');

const s3 = new AWS.S3();
const log = new DummyRequestLogger();

const splitter = constants.splitter;
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const awsLocation = 'aws-test';
const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
const postBody = Buffer.from('I am a body', 'utf8');
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post: '',
    parsedHost: 'localhost',
};
const objectKey = 'testObject';
const initiateRequest = {
    bucketName,
    namespace,
    objectKey,
    headers: { 'host': `${bucketName}.s3.amazonaws.com`,
        'x-amz-meta-scal-location-constraint': `${awsLocation}` },
    url: `/${objectKey}?uploads`,
    parsedHost: 'localhost',
};
const awsETag = 'd41d8cd98f00b204e9800998ecf8427e';
const partParams = {
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    awsETag,
};
const completeBody = '<CompleteMultipartUpload>' +
    '<Part>' +
    '<PartNumber>1</PartNumber>' +
    `<ETag>"${awsETag}"</ETag>` +
    '</Part>' +
    '</CompleteMultipartUpload>';
const completeParams = {
    bucketName,
    namespace,
    objectKey,
    parsedHost: 's3.amazonaws.com',
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    post: completeBody,
};

const awsParams = { Bucket: awsBucket, Key: objectKey };

describe('Multipart Upload API with AWS Backend', () => {
    afterEach(() => {
        cleanup();
    });

    it('should initiate a multipart upload on real AWS', done => {
        bucketPut(authInfo, bucketPutRequest, log, () => {
            initiateMultipartUpload(authInfo, initiateRequest, log,
            (err, result) => {
                assert.strictEqual(err, null, 'Error initiating MPU');
                parseString(result, (err, json) => {
                    assert.strictEqual(json.InitiateMultipartUploadResult
                        .Bucket[0], bucketName);
                    assert.strictEqual(json.InitiateMultipartUploadResult
                        .Key[0], objectKey);
                    assert(json.InitiateMultipartUploadResult.UploadId[0]);
                    const mpuKeys = metadata.keyMaps.get(mpuBucket);
                    assert.strictEqual(mpuKeys.size, 1);
                    assert(mpuKeys.keys().next().value
                        .startsWith(`overview${splitter}${objectKey}`));
                    awsParams.UploadId =
                        json.InitiateMultipartUploadResult.UploadId[0];
                    s3.abortMultipartUpload(awsParams, err => {
                        assert.strictEqual(err, null,
                            `Error aborting MPU ${err}`);
                        done();
                    });
                });
            });
        });
    });

    it('should complete a multipart upload on real AWS', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, err =>
                next(err)
            ),
            next => initiateMultipartUpload(authInfo, initiateRequest,
            log, (err, result) =>
                next(err, result)
            ),
            (result, next) => {
                parseString(result, (err, json) => {
                    const uploadId =
                        json.InitiateMultipartUploadResult.UploadId[0];
                    return next(err, uploadId);
                });
            },
            (uploadId, next) => {
                partParams.url =
                    `/${objectKey}?partNumber=1&uploadId=${uploadId}`;
                partParams.query = { partNumber: '1', uploadId };
                const partRequest = new DummyRequest(partParams, postBody);
                objectPutPart(authInfo, partRequest, undefined, log,
                err =>
                    next(err, uploadId)
                );
            },
            (uploadId, next) => {
                awsParams.UploadId = uploadId;
                s3.listParts(awsParams, (err, data) => {
                    assert.strictEqual(data.Parts.length, 1);
                    return next(err, uploadId);
                });
            },
            (uploadId, next) => {
                completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
                completeParams.query = { uploadId };
                completeMultipartUpload(authInfo, completeParams, log,
                (err, result) =>
                    next(err, result)
                );
            },
            (result, next) => {
                parseString(result, (err, json) => {
                    assert.strictEqual(
                        json.CompleteMultipartUploadResult.Location[0],
                        `http://${bucketName}.s3.amazonaws.com`
                        + `/${objectKey}`);
                    assert.strictEqual(json.CompleteMultipartUploadResult
                        .Bucket[0], bucketName);
                    assert.strictEqual(
                        json.CompleteMultipartUploadResult.Key[0],
                        objectKey);
                    const MD = metadata.keyMaps.get(bucketName).get(objectKey);
                    assert(MD);
                    return next(err);
                });
            },
        ], err => {
            assert.strictEqual(err, null, `Error completing MPU: ${err}`);
            done();
        });
    });
});
