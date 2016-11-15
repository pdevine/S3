import { errors } from 'arsenal';
import async from 'async';

import services from '../services';

/**
 * Bucket Put Website - Create bucket website configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPutWebsite(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutWebsite' });

    /* define constants */
    const bucketName = request.bucketName;

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutWebsite',
        log,
    };
    /* check if user has permissions first to add website conf
        we may need to put this in a waterfall, like bucketPutACL*/
    return async.waterfall([
        function waterfall1(next) {
            services.metadataValidateAuthorization(metadataValParams,
                err => {
                    if (err) {
                        log.trace('request authorization failed', {
                            error: err,
                            method: 'services.metadataValidateAuthorization',
                        });
                        return next(err);
                    }
                    return next(/*params for next function here;
                        may need to pass above in args for callback
                        for metadataValidateAuth too */);
                });
        },
        function waterfall2(next) {
            /* parse the website configuration and return appropriate errors */

            /*  add the website configuration object to the object's MD
            EX: adding acl's to bucket metadata
            return acl.addACL(bucket, addACLParams, log, next); */
        },
    ], err => {
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutWebsite' });
            return callback(err);
        }
        // can't I just return callback(err) here and take it out from previous?
        return callback(err, 'bucket website config set');
    });
}