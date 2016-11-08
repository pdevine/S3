import { exec, execFile } from 'child_process';
import async from 'async';
import fs from 'fs';
import assert from 'assert';
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

const getRangePt = range => {
    const endPts = range.split('-');

    if (endPts[0] === '') {
        endPts[0] = '0';
    } else if (endPts[1] === '' || endPts[1] >= fileSize) {
        endPts[1] = (fileSize - 1).toString();
    }

    return endPts;
};

const byteTotal = rangePt => {
    const start = Number.parseInt(rangePt[0], 10);
    const end = Number.parseInt(rangePt[1], 10);
    return ((end - start) + 1).toString();
};

const checkRanges = (range, cb) => {
    const rangePt = getRangePt(range);

    return s3.getObjectAsync({
        Bucket: bucket,
        Key: fn,
        Range: `bytes=${range}`,
    })
    .then(res => {
        fs.readFile(`${fn}.${range}`, (err, data) => {
            if (err) {
                throw err;
            }

            assert.notDeepStrictEqual(new Date(res.LastModified).toString(),
                'Invalid Date');
            const obj = Object.assign({}, res);
            delete obj.LastModified;

            assert.deepStrictEqual(obj, {
                AcceptRanges: 'bytes',
                ContentLength: byteTotal(rangePt),
                ETag: eTag,
                ContentRange: `bytes ${rangePt[0]}-${rangePt[1]}/${fileSize}`,
                ContentType: 'application/octet-stream',
                Metadata: {},
                Body: data,
            });

            cb();
        });
    })
    .catch(cb);
};

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
        const out = n === 0 ? `${fn}.0-5242880` : `${fn}.5242880-10485759`;

        execFile('dd', [`if=${fn}`, `of=${out}`, 'bs=5242880', `skip=${n}`,
            'count=1'],
                err => {
                    if (err) {
                        return next(err);
                    }
                    return s3.uploadPartAsync({
                        Bucket: bucket,
                        Key: fn,
                        PartNumber: n + 1,
                        UploadId: uploadId,
                        Body: fs.createReadStream(out),
                    }).then(() => next())
                    .catch(next);
                });
    }, err => {
        if (err) {
            return cb(err);
        }
        return completeMPU(range, cb);
    });


// Creates the hashed file that is comprised only of the byte range. Then it
// puts the contents of the file in a bucket. Only use with small byte ranges
// since the blocksize is set at 1. Otherwise, performance will be slow.
const rangeTest = (range, isMpu, cb) => {
    const rangePt = getRangePt(range);
    const count = byteTotal(rangePt);

    return execFile('dd', [`if=${fn}`, `of=${fn}.${range}`, 'bs=1',
        `skip=${rangePt[0]}`, `count=${count}`],
            err => {
                if (err) {
                    return cb(err);
                }
                if (isMpu) {
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
    execFile('gcc', ['-o', 'getRange', 'lib/utility/getRange.c'],
        err => {
            if (err) {
                return cb(err);
            }
            return execFile('./getRange', ['--size', fileSize,
                `hash.${fileSize}`], cb);
        });

describe('for large end position', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;

        beforeEach(done => {
            fileSize = 2890;
            eTag = '"dacc3aa9881a9b138039218029e5c2b3"';
            fn = `hash.${fileSize}`;

            s3.createBucketAsync({ Bucket: bucket })
            .then(() => createHashedFile(done))
            .catch(err => throwErr('Error in beforeEach', err));
        });

        afterEach(done =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRange hash.*', done))
            .catch(err => throwErr('Error in afterEach', err))
        );

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-', done => rangeTest('2800-', false, done));

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-Number.MAX_SAFE_INTEGER',
            done => rangeTest(`2800-${Number.MAX_SAFE_INTEGER}`, false, done));
    });
});

describe('for multipartUpload', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        s3 = bucketUtil.s3;

        beforeEach(done => {
            fileSize = 10 * 1024 * 1024;
            eTag = '"7eb736897d426098850e617502ea953e-2"';
            fn = `hash.${fileSize}`;

            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket,
                Key: fn,
            }))
            .then(res => {
                uploadId = res.UploadId;
            })
            .then(() => createHashedFile(done))
            .catch(err => throwErr('Error in beforeEach', err));
        });

        afterEach(done => bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => exec('rm getRange hash.*', done)
        ));

        it('should get a range from the first part of an object put by ' +
            'multipart upload', done => rangeTest('0-9', true, done));

        it('should get a range from the second part of an object put by ' +
            'multipart upload', done =>
            rangeTest('5242881-5242890', true, done));

        it('should get a range that spans both parts of an object put by ' +
            'multipart upload', done =>
            rangeTest('5242875-5242884', true, done));

        it('should get a range from the second part of an object put by ' +
            'multipart upload and include the end even if the range requested' +
            ' goes beyond the actual object end by multipart upload', done =>
            rangeTest('10485750-10485790', true, done));
    });
});
