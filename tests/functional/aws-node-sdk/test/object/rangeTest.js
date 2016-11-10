import assert from 'assert';
import async from 'async';
import { exec, execFile } from 'child_process';
import fs from 'fs';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucket-for-range-test';
let eTag;
let fileSize;
let fn;
let uploadId;
let s3;

const throwErr = (str, err) => {
    process.stdout.write(`${str}: ${err}\n`);
    throw err;
};

// Transform the range values to handle undefined end points or ranges beyond
// the size of the file
const getRangeEnds = range => {
    const arr = range.split('-');
    if (arr[0] === '' && arr[1] !== '') {
        arr[0] = (Number.parseInt(fileSize, 10) - Number.parseInt(arr[1], 10));
        arr[1] = (Number.parseInt(fileSize, 10) - 1).toString();
    } else {
        arr[0] = arr[0] === '' ? '0' : arr[0];
        arr[1] = arr[1] === '' || Number.parseInt(arr[1], 10) >= fileSize ?
            arr[1] = (Number.parseInt(fileSize, 10) - 1).toString() : arr[1];
    }
    return { start: arr[0], end: arr[1] };
};

// Calculate the total number of bytes in a byte range
const getByteTotal = (start, end) =>
    ((Number.parseInt(end, 10) - Number.parseInt(start, 10)) + 1).toString();

// Compare values of the object from S3 that has specified range, with the
// corresponding hashed file
const checkRanges = (range, cb) =>
    s3.getObjectAsync({
        Bucket: bucket,
        Key: fn,
        Range: `bytes=${range}`,
    })
    .then(res => {
        fs.readFile(`${fn}.${range}`, (err, data) => {
            if (err) {
                return cb(err);
            }

            console.log(res);

            const { start, end } = getRangeEnds(range);

            // If range header is invalid, AcceptRanges and ContentType should
            // be invalid.
            assert.deepStrictEqual(res.AcceptRanges, 'bytes');
            assert.notDeepStrictEqual(new Date(res.LastModified).toString(),
                'Invalid Date');
            assert.deepStrictEqual(res.ContentLength, getByteTotal(start, end));
            assert.deepStrictEqual(res.ETag, eTag);
            assert.deepStrictEqual(res.ContentRange,
                `bytes ${start}-${end}/${fileSize}`);
            assert.deepStrictEqual(res.ContentType, 'application/octet-stream');
            assert.deepStrictEqual(res.Metadata, {});
            assert.deepStrictEqual(res.Body, data);
            return cb();
        });
    })
    .catch(cb);

// Complete the MPU, then call `checkRanges()` for assertion testing
const completeMPU = (range, cb) =>
    s3.completeMultipartUploadAsync({
        Bucket: bucket,
        Key: fn,
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
    .then(() => checkRanges(range, cb))
    .catch(cb);


// Create the two parts to be uploaded as 1 of 2 multipart uploads, 5MB each.
const uploadParts = (range, cb) =>
    async.times(2, (n, next) => {
        execFile('dd', [`if=${fn}`, `of=${fn}.mpuPart${n + 1}`, 'bs=5242880',
            `skip=${n}`, 'count=1'],
                err => {
                    if (err) {
                        return next(err);
                    }
                    return s3.uploadPartAsync({
                        Bucket: bucket,
                        Key: fn,
                        PartNumber: n + 1,
                        UploadId: uploadId,
                        Body: fs.createReadStream(`${fn}.mpuPart${n + 1}`),
                    })
                    .then(() => next())
                    .catch(next);
                });
    }, err => {
        if (err) {
            return cb(err);
        }
        return completeMPU(range, cb);
    });

// Creates the hashed file that is comprised of the byte range
const rangeTest = (range, cb, isMPU) => {
    const { start, end } = getRangeEnds(range);

    return execFile('dd', [`if=${fn}`, `of=${fn}.${range}`, 'bs=1',
        `skip=${start}`, `count=${getByteTotal(start, end)}`],
            err => {
                if (err) {
                    return cb(err);
                }
                // If the test is for a MPU, we don't want to put the object in
                // a bucket, so instead upload the parts and complete the MPU
                if (isMPU) {
                    return uploadParts(range, cb);
                }
                return s3.putObjectAsync({
                    Bucket: bucket,
                    Key: fn,
                    Body: fs.createReadStream(fn),
                })
                .then(() => checkRanges(range, cb))
                .catch(cb);
            });
};

const createHashedFile = cb =>
    execFile('gcc', ['-o', 'getRangeExec', 'lib/utility/getRange.c'],
        err => {
            if (err) {
                return cb(err);
            }
            return execFile('./getRangeExec', ['--size', fileSize, fn], cb);
        });

// describe('aws-node-sdk range test for large end position', () => {
//     withV4(sigCfg => {
//         const bucketUtil = new BucketUtility('default', sigCfg);
//         s3 = bucketUtil.s3;
//
//         beforeEach(done => {
//             fileSize = 2890;
//             fn = `hashedFile.${fileSize}`;
//             eTag = '"dacc3aa9881a9b138039218029e5c2b3"';
//
//             s3.createBucketAsync({ Bucket: bucket })
//             .then(() => createHashedFile(done))
//             .catch(err => throwErr('Error in beforeEach', err));
//         });
//
//         afterEach(done =>
//             bucketUtil.empty(bucket)
//             .then(() => bucketUtil.deleteOne(bucket))
//             .then(() => exec('rm getRangeExec hashedFile.*', done))
//             .catch(err => throwErr('Error in afterEach', err))
//         );
//
//         it('should get the final 90 bytes of a 2890 byte object for a byte ' +
//             'range of 2800-', done => rangeTest('2800-', done));
//
//         it('should get the final 90 bytes of a 2890 byte object for a byte ' +
//             'range of 2800-Number.MAX_SAFE_INTEGER',
//             done => rangeTest(`2800-${Number.MAX_SAFE_INTEGER}`, done));
//     });
// });
//
// describe('aws-node-sdk range test for multipartUpload', () => {
//     withV4(sigCfg => {
//         const bucketUtil = new BucketUtility('default', sigCfg);
//         s3 = bucketUtil.s3;
//
//         beforeEach(done => {
//             fileSize = 10 * 1024 * 1024;
//             fn = `hashedFile.${fileSize}`;
//             eTag = '"7eb736897d426098850e617502ea953e-2"';
//
//             s3.createBucketAsync({ Bucket: bucket })
//             .then(() => s3.createMultipartUploadAsync({
//                 Bucket: bucket,
//                 Key: fn,
//             }))
//             .then(res => {
//                 uploadId = res.UploadId;
//             })
//             .then(() => createHashedFile(done))
//             .catch(err => throwErr('Error in beforeEach', err));
//         });
//
//         afterEach(done => bucketUtil.empty(bucket)
//             .then(() => bucketUtil.deleteOne(bucket))
//             .then(() => exec('rm getRangeExec hashedFile.*', done))
//         );
//
//         it('should get a range from the first part of an object put by ' +
//             'multipart upload', done => rangeTest('0-9', done, true));
//
//         it('should get a range from the second part of an object put by ' +
//             'multipart upload', done =>
//             rangeTest('5242880-5242889', done, true));
//
//         it('should get a range that spans both parts of an object put by ' +
//             'multipart upload', done =>
//             rangeTest('5242875-5242884', done, true));
//
//         it('should get a range from the second part of an object put by ' +
//             'multipart upload and include the end even if the range requested' +
//             ' goes beyond the actual object end by multipart upload', done =>
//             rangeTest('10485750-10485790', done, true));
//     });
// });

describe.only('aws-node-sdk range test of regular object put (non-MPU)', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;

        beforeEach(done => {
            fileSize = 200;
            fn = `hashedFile.${fileSize}`;
            eTag = '"16c63c8bd6edf5c9ebe3557848e6c518"';

            s3.createBucketAsync({ Bucket: bucket })
            .then(() => createHashedFile(done))
            .catch(err => throwErr('Error in beforeEach', err));
        });

        afterEach(done =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRangeExec hashedFile.*', done))
            .catch(err => throwErr('Error in afterEach', err))
        );

        it('should get a range for an object put without MPU', done =>
            rangeTest('10-99', done));

        it('should get a range for an object using only an end offset in the ' +
            'request', done => rangeTest('-10', done));

        it('should get a range for an object using only a start offset in the' +
            'request', done => rangeTest('190-', done));

        it('should get full object if range header is invalid', done =>
            // Since range header is invalid full object should be returned
            // and there should be no Content-Range header
            rangeTest('-', done));
    });
});
