import assert from 'assert';
import async from 'async';
import { exec, execFile } from 'child_process';
import fs from 'fs';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucket-for-range-test';
const key = 'key-for-range-test';
let s3;

// Get the expected end values for various ranges (e.g., '-10', '10-', '-')
function getOuterRange(range, bytes) {
    const arr = range.split('-');
    if (arr[0] === '' && arr[1] !== '') {
        arr[0] = Number.parseInt(bytes, 10) - Number.parseInt(arr[1], 10);
        arr[1] = Number.parseInt(bytes, 10) - 1;
    } else {
        arr[0] = arr[0] === '' ? 0 : Number.parseInt(arr[0], 10);
        arr[1] = arr[1] === '' || Number.parseInt(arr[1], 10) >= bytes ?
            Number.parseInt(bytes, 10) - 1 : arr[1];
    }
    return {
        begin: arr[0],
        end: arr[1],
    };
}

// Get the ranged object from a bucket. Write the response body to a file, then
// use getRangeExec to check that all the bytes are in the correct location.
function checkRanges(range, bytes, cb) {
    return s3.getObjectAsync({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${range}`,
    })
    .then(res => {
        const { begin, end } = getOuterRange(range, bytes);
        const total = (end - begin) + 1;
        // If the range header is '-' (i.e., it is invalid), content range
        // should be undefined
        const contentRange = range === '-' ? undefined :
            `bytes ${begin}-${end}/${bytes}`;

        assert.deepStrictEqual(res.ContentLength, total.toString());
        assert.deepStrictEqual(res.ContentRange, contentRange);
        assert.deepStrictEqual(res.ContentType, 'application/octet-stream');
        assert.deepStrictEqual(res.Metadata, {});

        // Write a file using the buffer so getRangeExec can then check bytes
        fs.writeFile(`hashedFile.${bytes}.${range}`, res.Body, err => {
            if (err) {
                return cb(err);
            }
            // If the getRangeExec program fails, then the range is incorrect
            return execFile('./getRangeExec', ['--check', '--size', total,
                '--offset', begin, `hashedFile.${bytes}.${range}`],
                err => cb(err));
        });
    })
    .catch(cb);
}

// Create 5MB parts and upload them as parts of a MPU
function uploadParts(bytes, uploadId, cb) {
    const eTags = [];
    const name = `hashedFile.${bytes}`;
    async.times(2, (n, next) => {
        execFile('dd', [`if=${name}`, `of=${name}.mpuPart${n + 1}`,
            'bs=5242880', `skip=${n}`, 'count=1'], err => {
                if (err) {
                    return next(err);
                }
                return s3.uploadPartAsync({
                    Bucket: bucket,
                    Key: key,
                    PartNumber: n + 1, // A MPU part cannot be 0
                    UploadId: uploadId,
                    Body: fs.createReadStream(`${name}.mpuPart${n + 1}`),
                })
                .then(res => {
                    eTags[n] = res.ETag;
                })
                .then(() => next())
                .catch(next);
            });
    }, err => cb(err, eTags));
}

// Create a hashed file of size bytes
function createHashedFile(bytes, cb) {
    const name = `hashedFile.${bytes}`;
    return execFile('./getRangeExec', ['--size', bytes, name], cb);
}

describe('aws-node-sdk range tests', () => {
    before(done => execFile('gcc', ['-o', 'getRangeExec',
        'lib/utility/getRange.c'], err => done(err)
    ));

    after(done => exec('rm getRangeExec', done));

    describe('aws-node-sdk range test for object put by MPU', () => {
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
                .then(() => createHashedFile(fileSize,
                    () => uploadParts(fileSize, uploadId,
                    (err, eTags) => {
                        if (err) {
                            return done(err);
                        }
                        return s3.completeMultipartUploadAsync({
                            Bucket: bucket,
                            Key: key,
                            UploadId: uploadId,
                            MultipartUpload: {
                                Parts: [
                                    {
                                        ETag: eTags[0],
                                        PartNumber: 1,
                                    },
                                    {
                                        ETag: eTags[1],
                                        PartNumber: 2,
                                    },
                                ],
                            },
                        })
                        .then(() => done());
                    })
                ))
                .catch(done)
            );

            afterEach(done => bucketUtil.empty(bucket)
                .then(() => s3.abortMultipartUploadAsync({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                }))
                .catch(err => {
                    if (err.code !== 'NoSuchUpload') {
                        done(err);
                    }
                })
                .then(() => bucketUtil.deleteOne(bucket))
                .then(() => exec(`rm hashedFile.${fileSize}*`, done))
            );

            it('should get a range from the first part of an object', done =>
                checkRanges('0-9', fileSize, done));

            it('should get a range from the second part of an object', done =>
                checkRanges('5242880-5242889', fileSize, done));

            it('should get a range that spans both parts of an object', done =>
                checkRanges('5242875-5242884', fileSize, done));

            it('should get a range from the second part of an object and ' +
                'include the end if the range requested goes beyond the ' +
                'actual object end', done =>
                checkRanges('10485750-10485790', fileSize, done));
        });
    });

    describe('aws-node-sdk range test of regular object put (non-MPU)', () => {
        withV4(sigCfg => {
            const bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const fileSize = 2000;

            beforeEach(done =>
                s3.createBucketAsync({ Bucket: bucket })
                .then(() => createHashedFile(fileSize,
                () => s3.putObjectAsync({
                    Bucket: bucket,
                    Key: key,
                    Body: fs.createReadStream(`hashedFile.${fileSize}`),
                })
                .then(() => done())
                ))
                .catch(done)
            );

            afterEach(done =>
                bucketUtil.empty(bucket)
                .then(() => bucketUtil.deleteOne(bucket))
                .then(() => exec(`rm hashedFile.${fileSize}*`, done))
                .catch(done));

            const putRangeTests = [
                '-', // Test for invalid range
                '-1',
                '-10',
                '-512',
                '-2000',
                '0-',
                '1-',
                '190-',
                '512-',
                '0-7',
                '0-9',
                '8-15',
                '10-99',
                '0-511',
                '0-512',
                '0-513',
                '0-1023',
                '0-1024',
                '0-1025',
                '0-2000',
                '1-2000',
                '1000-1999',
                '1023-1999',
                '1024-1999',
                '1025-1999',
                '1976-1999',
                '1999-2001',
            ];

            putRangeTests.forEach(range => {
                it(`should get a range of ${range} bytes using a ${fileSize} ` +
                    'byte sized object', done => {
                    checkRanges(range, fileSize, done);
                });
            });
        });
    });

    describe('aws-node-sdk range test for large end position', () => {
        withV4(sigCfg => {
            const bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const fileSize = 2900;

            beforeEach(done =>
                s3.createBucketAsync({ Bucket: bucket })
                .then(() => createHashedFile(fileSize,
                () => s3.putObjectAsync({
                    Bucket: bucket,
                    Key: key,
                    Body: fs.createReadStream(`hashedFile.${fileSize}`),
                })
                .then(() => done())
                ))
                .catch(done)
            );

            afterEach(done =>
                bucketUtil.empty(bucket)
                .then(() => bucketUtil.deleteOne(bucket))
                .then(() => exec(`rm hashedFile.${fileSize}*`, done))
                .catch(done));

            it('should get the final 90 bytes of a 2890 byte object for a ' +
                'byte range of 2800-', done =>
                checkRanges('2800-', fileSize, done));

            it('should get the final 90 bytes of a 2890 byte object for a ' +
                'byte range of 2800-Number.MAX_SAFE_INTEGER', done =>
                checkRanges(`2800-${Number.MAX_SAFE_INTEGER}`, fileSize, done));
        });
    });
});
