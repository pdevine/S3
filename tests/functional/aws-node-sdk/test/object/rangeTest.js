import { exec, execFile } from 'child_process';
import fs from 'fs';
import assert from 'assert';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucket-for-range-test';

let eTag;
let range;
let fileSize;
let s3;

const throwErr = (str, err) => {
    process.stdout.write(`${str}: ${err}\n`);
    throw err;
};

const getRangePts = range => {
    const endPts = range.split('-');

    if (endPts[0] === '') {
        endPts[0] = '0';
    } else if (endPts[1] === '' || endPts[1] >= fileSize) {
        endPts[1] = (fileSize - 1).toString();
    }

    return endPts;
};

const byteTotal = rangePts => {
    const startPt = Number.parseInt(rangePts[0], 10);
    const endPt = Number.parseInt(rangePts[1], 10);
    return ((endPt - startPt) + 1).toString();
};

const checkRanges = (key, range, cb) => {
    const rangePts = getRangePts(range);

    s3.getObjectAsync({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${range}`,
    })
    .then(res => {
        fs.readFile(`./${key}.${range}`, (err, data) => {
            if (err) {
                throw err;
            }

            assert.notDeepStrictEqual(new Date(res.LastModified).toString(),
                'Invalid Date');
            const obj = Object.assign({}, res);
            delete obj.LastModified;

            assert.deepStrictEqual(obj, {
                AcceptRanges: 'bytes',
                ContentLength: byteTotal(rangePts),
                ETag: eTag,
                ContentRange: `bytes ${rangePts[0]}-${rangePts[1]}/${fileSize}`,
                ContentType: 'application/octet-stream',
                Metadata: {},
                Body: data,
            });

            cb();
        });
    })
    .catch(cb);
};

const putFile = (file, key, range, cb) => {
    s3.putObjectAsync({
        Bucket: bucket,
        Key: key,
        Body: fs.createReadStream(file),
    })
    .then(() => checkRanges(key, range, cb))
    .catch(cb);
};

const testfileRange = (range, fn, cb) => {
    const rangePts = getRangePts(range);

    exec(`dd if=./${fn} of=./${fn}.${range} bs=1 skip=${rangePts[0]} ` +
        `count=${byteTotal(rangePts)}`,
        err => {
            if (err) {
                return cb(err);
            }
            return putFile(`./${fn}`, `${fn}`, range, cb);
        });
};

const createHashedFile = cb => {
    execFile('./getRange', ['--size', `${fileSize}`, `./hash.${fileSize}`],
            err => {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
};

const rangeTest = cb => {
    execFile('gcc', ['-o', './getRange', 'lib/utility/getRange.c'],
        err => {
            if (err) {
                throwErr('Error creating getRange executable', err);
            }
            return createHashedFile(cb);
        });
};

describe.only('aws-node-sdk range test for large end position', () => {
    withV4(sigCfg => {
        let bucketUtil;
        fileSize = 2890;
        eTag = '"dacc3aa9881a9b138039218029e5c2b3"';

        beforeEach(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            s3.createBucketAsync({ Bucket: bucket })
            .then(() => rangeTest(done))
            .catch(err => throwErr('Error in beforeEach', err));
        });

        afterEach(done =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket))
            .then(() => {
                exec('rm ./getRange ./hash.*',
                    err => {
                        if (err) {
                            done(err);
                        }
                        done();
                    });
            })
            .catch(err => throwErr('Error in afterEach', err))
        );

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-',
            done => {
                testfileRange('2800-', `./hash.${fileSize}`, done);
            }
        );

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-Number.MAX_SAFE_INTEGER',
            done => {
                testfileRange(`2800-${Number.MAX_SAFE_INTEGER}`,
                    `./hash.${fileSize}`, done);
            }
        );
    });
});
