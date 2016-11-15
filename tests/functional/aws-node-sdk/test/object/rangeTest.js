import assert from 'assert';
import async from 'async';
import { exec, execFile } from 'child_process';
import fs from 'fs';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucket-for-range-test';
const key = 'key-for-range-test';
let s3;

// Calculate the total number of bytes for range end values
const getTotal = (begin, end) =>
    Number.parseInt(end, 10) - Number.parseInt(begin, 10) + 1;

// Get the expected end values for various ranges (e.g., '-10', '10-', '-')
const getOuterRange = (range, fileSize) => {
    const arr = range.split('-');
    if (arr[0] === '' && arr[1] !== '') {
        arr[0] = Number.parseInt(fileSize, 10) - Number.parseInt(arr[1], 10);
        arr[1] = Number.parseInt(fileSize, 10) - 1;
    } else {
        arr[0] = arr[0] === '' ? '0' : arr[0]; // For invalid ranges
        arr[1] = arr[1] === '' || Number.parseInt(arr[1], 10) >= fileSize ?
            Number.parseInt(fileSize, 10) - 1 : arr[1];
    }
    return {
        begin: arr[0],
        end: arr[1],
    };
};

// Compare the S3 object from getObject with the corresponding hashed range file
const checkRanges = (range, fileSize, cb) =>
    s3.getObjectAsync({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${range}`,
    })
    .then(res => fs.readFile(`hashedFile.${fileSize}.${range}`, (err, data) => {
        if (err) {
            return cb(err);
        }
        const { begin, end } = getOuterRange(range, fileSize);
        const contentLength = getTotal(begin, end).toString();
        // If the range header is invalid, content range should be `undefined`
        const contentRange = range === '-' ? undefined :
            `bytes ${begin}-${end}/${fileSize}`;

        assert.deepStrictEqual(res.ContentLength, contentLength);
        assert.deepStrictEqual(res.ContentRange, contentRange);
        assert.deepStrictEqual(res.ContentType, 'application/octet-stream');
        assert.deepStrictEqual(res.Metadata, {});
        assert.deepStrictEqual(res.Body, data);
        return cb();
    }))
    .catch(cb);

// Complete the MPU, or abort it if there is an error
const completeMPU = (range, fileSize, uploadId, cb) =>
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
    .then(() => checkRanges(range, fileSize, cb))
    .catch(err => s3.abortMultipartUploadAsync({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
    })
    .then(() => cb(err)));

// Create a hashed file of size n
const createHashedFile = (fileSize, cb) => {
    const name = `hashedFile.${fileSize}`;
    execFile('gcc', ['-o', 'getRangeExec', 'lib/utility/getRange.c'],
        err => {
            if (err) {
                return cb(err);
            }
            return execFile('./getRangeExec', ['--size', fileSize, name], cb);
        });
};

// Use the hashed file to create a new file comprised of a byte range
const createRangedFile = (range, fileSize, cb) => {
    const name = `hashedFile.${fileSize}`;
    const { begin, end } = getOuterRange(range, fileSize);
    const total = getTotal(begin, end);
    return execFile('dd', [`if=${name}`, `of=${name}.${range}`, 'bs=1',
        `skip=${begin}`, `count=${total}`], err => cb(err, name));
};

// Create a file comprised of a byte range from the original hashed file
const putRangeTest = (range, fileSize, cb) =>
    createRangedFile(range, fileSize, (err, name) =>
        // Add the original hashed file to the bucket for objectGet API
        s3.putObjectAsync({
            Bucket: bucket,
            Key: key,
            Body: fs.createReadStream(name),
        })
        .then(() => checkRanges(range, fileSize, cb))
        .catch(cb));

// Upload a hashed file as a MPU, then Create two 5MB parts to be uploaded as a
// MPU, or abort it if there is an error
const mpuRangeTest = (range, fileSize, uploadId, cb) =>
    createRangedFile(range, fileSize, (err, name) =>
        async.times(2, (n, next) => {
            execFile('dd', [`if=${name}`, `of=${name}.mpuPart${n + 1}`,
                'bs=5242880', `skip=${n}`, 'count=1'], err => {
                    if (err) {
                        return next(err);
                    }
                    return s3.uploadPartAsync({
                        Bucket: bucket,
                        Key: key,
                        PartNumber: n + 1, // An MPU part cannot be 0
                        UploadId: uploadId,
                        Body: fs.createReadStream(`${name}.mpuPart${n + 1}`),
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
            return completeMPU(range, fileSize, uploadId, cb);
        }));

describe('aws-node-sdk range test for large end position', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;

        beforeEach(done => {
            s3.createBucketAsync({ Bucket: bucket })
            .then(() => createHashedFile(2890, done))
            .catch(done);
        });

        afterEach(done =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRangeExec hashedFile.*', done))
            .catch(done)
        );

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-', done => putRangeTest('2800-', 2890, done));

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-Number.MAX_SAFE_INTEGER', done =>
            putRangeTest(`2800-${Number.MAX_SAFE_INTEGER}`, 2890, done));
    });
});

describe('aws-node-sdk range test of regular object put (non-MPU)', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;

        beforeEach(done => {
            s3.createBucketAsync({ Bucket: bucket })
            .then(() => createHashedFile(200, done))
            .catch(done);
        });

        afterEach(done =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRangeExec hashedFile.*', done))
            .catch(done)
        );

        it('should get a range for an object put without MPU', done =>
            putRangeTest('10-99', 200, done));

        it('should get a range for an object using only an end offset in the ' +
            'request', done => putRangeTest('-10', 200, done));

        it('should get a range for an object using only a begin offset in the' +
            'request', done => putRangeTest('190-', 200, done));

        it('should get full object if range header is invalid', done =>
            putRangeTest('-', 200, done));
    });
});

describe('aws-node-sdk range test for multipartUpload', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;
        const fileSize = 10 * 1024 * 1024;
        let uploadId;

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
            mpuRangeTest('0-9', fileSize, uploadId, done));

        it('should get a range from the second part of an object put by ' +
            'multipart upload', done =>
            mpuRangeTest('5242880-5242889', fileSize, uploadId, done));

        it('should get a range that spans both parts of an object put by ' +
            'multipart upload', done =>
            mpuRangeTest('5242875-5242884', fileSize, uploadId, done));

        it('should get a range from the second part of an object put by ' +
            'multipart upload and include the end even if the range requested' +
            ' goes beyond the actual object end by multipart upload', done =>
            mpuRangeTest('10485750-10485790', fileSize, uploadId, done));
    });
});
