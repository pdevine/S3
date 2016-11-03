import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'object-test-mpu';
const objectKey = 'toAbort&<>"\'';

const throwErr = (str, err) => {
    process.stdout.write(`${str}: ${err}\n`);
    throw err;
};

const objCmp = (responseObj, expectedObj, cb) => {
    assert.deepStrictEqual(responseObj, expectedObj);
    cb();
};

function listMpuObj(uploadId, userId, displayName, maxUploads) {
    const defaultMax = typeof maxUploads === 'undefined' ? 1000 : maxUploads;

    return {
        Bucket: bucket,
        KeyMarker: '',
        UploadIdMarker: '',
        NextKeyMarker: objectKey,
        Prefix: '',
        Delimiter: '',
        NextUploadIdMarker: uploadId,
        MaxUploads: defaultMax,
        IsTruncated: false,
        Uploads: [{
            UploadId: uploadId,
            Key: objectKey,
            StorageClass: 'STANDARD',
            Owner:
            {
                DisplayName: displayName,
                ID: userId,
            },
            Initiator:
            {
                DisplayName: displayName,
                ID: userId,
            },
        }],
        CommonPrefixes: [],
    };
}

describe('aws-node-sdk test suite of listMultipartUploads', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;
        let displayName;
        let userId;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => bucketUtil.getOwner())
            .then(res => {
                /* In this case, the owner of the bucket will also be the MPU
                 * upload owner. We need these values for object comparison.
                 */
                displayName = res.DisplayName;
                userId = res.ID;
            })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket,
                Key: objectKey,
            }))
            .then(res => {
                uploadId = res.UploadId;
            })
            .catch(err => throwErr('Error in beforeEach', err));
        });

        afterEach(() =>
            s3.abortMultipartUploadAsync({
                Bucket: bucket,
                Key: objectKey,
                UploadId: uploadId,
            })
            .then(() => bucketUtil.empty(bucket))
            .then(() => bucketUtil.deleteOne(bucket))
            .catch(err => throwErr('Error in afterEach', err))
        );

        it('should list ongoing multipart uploads', done => {
            s3.listMultipartUploadsAsync({ Bucket: bucket })
            .then(res => {
                /* The dateString of the Date object cannot be tested for
                 * because there may be a difference between the time of upload
                 * creation and assertion testing. Testing that the value is an
                 * instance of Date, then removing the Date object so we can
                 * still compare the object is the compromised solution.
                 */
                assert(res.Uploads[0].Initiated instanceof Date);
                const obj = Object.assign({}, res);
                delete obj.Uploads[0].Initiated;

                objCmp(obj, listMpuObj(uploadId, userId, displayName), done);
            })
            .catch(done);
        });

        it('should list ongoing multipart uploads with params', done => {
            s3.listMultipartUploadsAsync({
                Bucket: bucket,
                Prefix: 'to',
                MaxUploads: 1,
            })
            .then(res => {
                assert(res.Uploads[0].Initiated instanceof Date);
                const obj = Object.assign({}, res);
                delete obj.Uploads[0].Initiated;

                objCmp(obj, listMpuObj(uploadId, userId, displayName, 1), done);
            })
            .catch(done);
        });

        it('should list 0 multipart uploads when MaxUploads is 0', done => {
            s3.listMultipartUploadsAsync({
                Bucket: bucket,
                Prefix: 'to',
                MaxUploads: 0,
            })
            .then(res => {
                /* When MaxUploads is set to 0 there is no Initiated value.
                 * IsTruncated is set to false despite the fact that there is,
                 * in fact, one multipartUpload in the bucket. This is the
                 * behavior of AWS.
                 */
                assert.deepStrictEqual(res, {
                    Bucket: bucket,
                    KeyMarker: '',
                    UploadIdMarker: '',
                    NextKeyMarker: '',
                    Prefix: '',
                    Delimiter: '',
                    NextUploadIdMarker: '',
                    MaxUploads: 0,
                    IsTruncated: false,
                    Uploads: [],
                    CommonPrefixes: [],
                });
                done();
            })
            .catch(done);
        });
    });
});
