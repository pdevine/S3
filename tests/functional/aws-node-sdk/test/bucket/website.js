import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'testbucketwebsitebucket';

class _makeWebsiteConfig {
    constructor(indexDocument, errorDocument, redirectAllReqHost,
        redirectAllReqProtocol) {
        if (indexDocument) {
            this.IndexDocument = {};
            this.IndexDocument.Suffix = indexDocument;
        }
        if (errorDocument) {
            this.ErrorDocument = {};
            this.ErrorDocument.Key = errorDocument;
        }
        if (redirectAllReqHost) {
            this.RedirectAllRequestTo = {};
            this.RedirectAllRequestTo.HostName = redirectAllReqHost;
            if (redirectAllReqProtocol) {
                this.RedirectAllRequestTo.Protocol = redirectAllReqProtocol;
            }
        }
    }
    addRoutingRule(redirectParams, conditionParams) {
        const newRule = {};
        if (!this.RoutingRules) {
            this.RoutingRules = [];
        }
        if (redirectParams) {
            newRule.Redirect = {};
            Object.keys(redirectParams).forEach(key => {
                newRule.Redirect[`${key}`] = redirectParams.key;
            });
        }
        if (conditionParams) {
            newRule.Condition = {};
            Object.keys(conditionParams).forEach(key => {
                newRule.Condition[`${key}`] = conditionParams.key;
            });
        }
        this.RoutingRules.push(newRule);
    }
    // another property to add is MD5...
}

describe('PUT bucket website', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(done => {
            process.stdout.write('about to create bucket\n');
            s3.createBucket({ Bucket: bucketName }, err => {
                if (err) {
                    process.stdout.write('error in beforeEach', err);
                    done(err);
                }
                done();
            });
        });

        afterEach(() => {
            process.stdout.write('about to empty bucket\n');
            return bucketUtil.empty(bucketName).then(() => {
                process.stdout.write('about to delete bucket\n');
                return bucketUtil.deleteOne(bucketName);
            }).catch(err => {
                if (err) {
                    process.stdout.write('error in afterEach', err);
                    throw err;
                }
            });
        });

        it('should put a bucket website successfully', done => {
            const config = new _makeWebsiteConfig('index.html');
            s3.putBucketWebsite({ Bucket: bucketName,
                WebsiteConfiguration: config }, (err, res) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                done();
            });
        });

        it('should return InvalidArgument if IndexDocument or ' +
        'RedirectAllRequestsTo is not provided', done => {
            const config = new _makeWebsiteConfig();
            s3.putBucketWebsite({ Bucket: bucketName,
                WebsiteConfiguration: config }, err => {
                assert(err, 'Expected err but found one');
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });
    });
});
