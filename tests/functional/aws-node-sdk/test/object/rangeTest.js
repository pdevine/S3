import { exec, execFile } from 'child_process';
import fs from 'fs';
import assert from 'assert';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucket-for-range-test';
let range;
let fileSize;
let s3;

const throwErr = (str, err) => {
    process.stdout.write(`${str}: ${err}\n`);
    throw err;
};

const getRangePts = cb => {
    const endPts = range.split('-');

    if (endPts.length !== 2) {
        cb(new Error('Invalid range given'));
    }

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

const checkRanges = (key, cb) => {
    const rangePts = getRangePts(cb);

    s3.getObjectAsync({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${range}`,
    })
    .then(res => {
        const contentRange = `bytes ${rangePts[0]}-${rangePts[1]}/${fileSize}`;

        fs.readFile(`./${key}.${range}`, (err, data) => {
            if (err) {
                throw err;
            }
            assert.deepStrictEqual(res.AcceptRanges, 'bytes');
            assert.deepStrictEqual(res.Body, data);
            assert.deepStrictEqual(res.ContentLength, byteTotal(rangePts));
            assert.deepStrictEqual(res.ContentRange, contentRange);
            cb();
        });
    })
    .catch(cb);
};

const putFile = (file, key, cb) => {
    s3.putObjectAsync({
        Bucket: bucket,
        Key: key,
        Body: fs.createReadStream(file),
    })
    .then(() => checkRanges(key, cb))
    .catch(cb);
};

const createRangeFile = (fn, cb) => {
    const rangePts = getRangePts(cb);

    exec(`dd if=./${fn} of=./${fn}.${range} bs=1 skip=${rangePts[0]} ` +
        `count=${byteTotal(rangePts)}`,
        err => {
            if (err) {
                return cb(err);
            }
            return putFile(`./${fn}`, `${fn}`, cb);
        });
};

const createHashedFile = cb => {
    execFile('./getRange', ['--size', `${fileSize}`, `./hash.${fileSize}`],
            err => {
                if (err) {
                    return cb(err);
                }
                return createRangeFile(`./hash.${fileSize}`, cb);
            });
};

const createExec = cb => {
    execFile('gcc', ['-o', './getRange', 'lib/utility/getRange.c'],
        err => {
            if (err) {
                throwErr('Error creating getRange executable', err);
            }
            return createHashedFile(cb);
        });
};

describe.only('aws-node-sdk range test', () => {
    withV4(sigCfg => {
        let bucketUtil;

        before(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            s3.createBucketAsync({ Bucket: bucket })
            .then(() => done())
            .catch(err => throwErr('Error in before', err));
        });

        after(done =>
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
            .catch(err => throwErr('Error in after', err))
        );

        it('put a 10MB hashedFile in a bucket, compare byte ranges', done => {
            fileSize = 10 * 1024 * 1024;
            range = '0-9';
            createExec(done);
        });

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-',
            done => {
                fileSize = 2890;
                range = '2800-';
                createExec(done);
            }
        );

        it('should get the final 90 bytes of a 2890 byte object for a byte ' +
            'range of 2800-Number.MAX_SAFE_INTEGER',
            done => {
                fileSize = 2890;
                range = `2800-${Number.MAX_SAFE_INTEGER}`;
                createExec(done);
            }
        );
    });
});
