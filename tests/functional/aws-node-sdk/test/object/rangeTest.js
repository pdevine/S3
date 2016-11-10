import assert from 'assert';
import async from 'async';
import { exec, execFile } from 'child_process';
import fs from 'fs';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucket-for-range-test';
const key = 'key-for-range-test';
let s3;

// Calculate the total number of bytes for given range points
const byteTotal = (start, end) =>
    (parseInt(end, 10) - parseInt(start, 10) + 1).toString();

// Return range values for various range end points (e.g., '-10', '10-', '-')
const getRangeEnds = (range, size) => {
    const arr = range.split('-');
    if (arr[0] === '' && arr[1] !== '') {
        arr[0] = (parseInt(size, 10) - parseInt(arr[1], 10));
        arr[1] = (parseInt(size, 10) - 1);
    } else {
        arr[0] = arr[0] === '' ? '0' : arr[0];
        arr[1] = arr[1] === '' || parseInt(arr[1], 10) >= size ?
            (parseInt(size, 10) - 1) : arr[1];
    }
    return { start: arr[0].toString(), end: arr[1].toString() };
};

// Compare S3 object of a specified range, with corresponding range hashed file
const checkRanges = (range, size, cb) => {
    s3.getObjectAsync({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${range}`,
    })
    .then(res => {
        fs.readFile(`hashedFile.${size}.${range}`, (err, data) => {
            if (err) {
                return cb(err);
            }
            // Handle cases for testing unique responses using a callback
            // (e.g, when the range header is invalid)
            if (cb.length === 2) {
                return cb(res, data);
            }
            const { start, end } = getRangeEnds(range, size);
            assert.deepStrictEqual(res.AcceptRanges, 'bytes');
            assert.deepStrictEqual(res.ContentLength, byteTotal(start, end));
            assert.deepStrictEqual(res.ContentRange,
                `bytes ${start}-${end}/${size}`);
            assert.deepStrictEqual(res.ContentType, 'application/octet-stream');
            assert.deepStrictEqual(res.Metadata, {});
            assert.deepStrictEqual(res.Body, data);
            return cb();
        });
    })
    .catch(cb);
};

// Complete the MPU, or abort it if there is an error
const completeMPU = (range, size, uploadId, cb) =>
    s3.completeMultipartUploadAsync({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: {
            Parts: [
                {
                    ETag: 'fa5a73131c61ee90e128fe6bef8544e2',
                    PartNumber: 1,
                },
                {
                    ETag: 'e09d29b0c1da7495508d56a4047fec71',
                    PartNumber: 2,
                },
            ],
        },
    })
    .then(() => checkRanges(range, size, cb))
    .catch(err =>
        s3.abortMultipartUploadAsync({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
        })
        .then(() => cb(err))
    );

// Create two 5MB parts to be uploaded as a MPU, or abort if there is an error
const uploadParts = (range, size, uploadId, cb) => {
    const fn = `hashedFile.${size}`;
    async.times(2, (n, next) => {
        execFile('dd', [`if=${fn}`, `of=${fn}.mpuPart${n + 1}`, 'bs=5242880',
            `skip=${n}`, 'count=1'], err => {
                if (err) {
                    return next(err);
                }
                return s3.uploadPartAsync({
                    Bucket: bucket,
                    Key: key,
                    PartNumber: n + 1,
                    UploadId: uploadId,
                    Body: fs.createReadStream(`${fn}.mpuPart${n + 1}`),
                })
                .then(() => next())
                .catch(next);
            });
    }, err => {
        if (err) {
            return s3.abortMultipartUploadAsync({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            })
            .then(() => cb(err));
        }
        return completeMPU(range, size, uploadId, cb);
    });
};

// Create the hashed file comprised of the byte range
const rangeTest = (range, size, cb, uploadId) => {
    const fn = `hashedFile.${size}`;
    const { start, end } = getRangeEnds(range, size);

    return execFile('dd', [`if=${fn}`, `of=${fn}.${range}`, 'bs=1',
        `skip=${start}`, `count=${byteTotal(start, end)}`],
            err => {
                if (err) {
                    return cb(err);
                }
                // If the test is for a MPU, we don't want to put the object in
                // a bucket, so instead upload the parts and complete the MPU
                if (uploadId) {
                    return uploadParts(range, size, uploadId, cb);
                }
                return s3.putObjectAsync({
                    Bucket: bucket,
                    Key: key,
                    Body: fs.createReadStream(fn),
                })
                .then(() => checkRanges(range, size, cb))
                .catch(cb);
            });
};

// Creates the hashed file of size n for range comparison
const createHashedFile = (size, cb) =>
    execFile('gcc', ['-o', 'getRangeExec', 'lib/utility/getRange.c'],
        err => {
            if (err) {
                return cb(err);
            }
            return execFile('./getRangeExec', ['--size', `${size}`,
                `hashedFile.${size}`], cb);
        });

describe('aws-node-sdk range test for large end position', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;
        const fileSize = 2890;

        beforeEach(done => {
            s3.createBucketAsync({ Bucket: bucket })
            .then(() => createHashedFile(fileSize, done))
            .catch(done);
        });

        afterEach(done =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRangeExec hashedFile.*', done))
            .catch(done)
        );

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-', done => rangeTest('2800-', fileSize, done));

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-Number.MAX_SAFE_INTEGER', done =>
            rangeTest(`2800-${Number.MAX_SAFE_INTEGER}`, fileSize, done));
    });
});

describe('aws-node-sdk range test for multipartUpload', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;
        let uploadId;
        const fileSize = 10 * 1024 * 1024;

        beforeEach(done =>
            s3.createBucketAsync({ Bucket: bucket })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket,
                Key: key,
            }))
            .then(res => {
                uploadId = res.UploadId;
            })
            .then(() => createHashedFile(fileSize, done))
            .catch(done)
        );

        afterEach(done => bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRangeExec hashedFile.*', done))
        );

        it('should get a range from the first part of an object put by ' +
            'multipart upload', done =>
                rangeTest('0-9', fileSize, done, uploadId));

        it('should get a range from the second part of an object put by ' +
            'multipart upload', done =>
            rangeTest('5242881-5242890', fileSize, done, uploadId));

        it('should get a range from the first byte of the second part of ' +
            'an object put by multipart upload', done =>
            rangeTest('5242880-5242889', fileSize, done, uploadId));

        it('should get a range that spans both parts of an object put by ' +
            'multipart upload', done =>
            rangeTest('5242875-5242884', fileSize, done, uploadId));

        it('should get a range from the second part of an object put by ' +
            'multipart upload and include the end even if the range requested' +
            ' goes beyond the actual object end by multipart upload', done =>
            rangeTest('10485750-10485790', fileSize, done, uploadId));
    });
});

describe('aws-node-sdk range test of regular object put (non-MPU)', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;
        const fileSize = 200;

        beforeEach(done => {
            s3.createBucketAsync({ Bucket: bucket })
            .then(() => createHashedFile(fileSize, done))
            .catch(done);
        });

        afterEach(done =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRangeExec hashedFile.*', done))
            .catch(done)
        );

        it('should get a range for an object put without MPU', done =>
            rangeTest('10-99', fileSize, done));

        it('should get a range for an object using only an end offset in the ' +
            'request', done => rangeTest('-10', fileSize, done));

        it('should get a range for an object using only a start offset in the' +
            'request', done => rangeTest('190-', fileSize, done));

        it('should get full object if range header is invalid', done =>
            rangeTest('-', fileSize, (res, data) => {
                // Since range header is invalid, full object should be returned
                // and there should be no Content-Range or Accept-Ranges header
                assert.deepStrictEqual(res.ContentLength, '200');
                assert.deepStrictEqual(res.ContentType,
                    'application/octet-stream');
                assert.deepStrictEqual(res.Metadata, {});
                assert.deepStrictEqual(res.Body, data);
                done();
            })
        );
    });
});
