import { parseString } from 'xml2js';

import { errors } from 'arsenal';
// import constants from '../../constants';
// import escapeForXML from '../utilities/escapeForXML';

function _xmlContainsRequiredElem(obj, requiredElem) {
    console.log('===obj', obj);
    if (!Array.isArray(obj)
    || obj.length !== 1
    || !Array.isArray(obj[0][requiredElem])
    || obj[0][requiredElem].length !== 1) {
        return false;
    }
    return true;
}

/**
* @param {obj} jsonResult - website configuration xml parsed into JSON
* @param {string} xml - xml from putBucketWebsite request body
* @param {logger} log - logger object
* @param {callback} cb - callback
* @return {undefined} and calls callback
*/
function _validateWebsiteConfigXml(jsonResult, xml, log, cb) {
    if (!jsonResult || !jsonResult.WebsiteConfiguration) {
        log.warn('invalid website configuration xml', { xml });
        return cb(errors.MalformedXML);
    }

    const resultConfig = jsonResult.WebsiteConfiguration;
    if (!(resultConfig.IndexDocument || resultConfig.RedirectAllRequestsTo)) {
        log.warn('Value for IndexDocument Suffix must be provided if ' +
        'RedirectAllRequestsTo is empty', { xml });
        return cb(errors.InvalidArgument);
    }

    if (resultConfig.RedirectAllRequestsTo) {
        const parent = resultConfig.RedirectAllRequestsTo;
        if (!_xmlContainsRequiredElem(parent, 'HostName')) {
            log.warn('RedirectAllRequestsTo not well-formed', { xml });
            return cb(errors.MalformedXML);
        } else if (typeof parent[0].HostName[0] !== 'string'
        || parent[0].HostName[0] === '') {
            log.warn('HostName required in RedirectAllRequestsTo', { xml });
            return cb(errors.InvalidRequest);
        }
        // maybe here I should save some info into config?
    }

    if (resultConfig.IndexDocument) {
        const parent = resultConfig.IndexDocument;
        if (!_xmlContainsRequiredElem(parent, 'Suffix')) {
            log.warn('IndexDocument not well-formed', { xml });
            return cb(errors.MalformedXML);
        } else if (typeof parent[0].Suffix[0] !== 'string'
        || parent[0].Suffix[0] === ''
        || parent[0].Suffix[0].indexOf('/') !== -1) {
            log.warn('IndexDocument Suffix not well-formed', { xml });
            return cb(errors.InvalidArgument);
        }
        // maybe here I should save some info into config?
    }

    return cb(null, jsonResult);

    // note for testing: AWS does not specify must not be empty
//        if (resConfig.ErrorDocument &&
//            (!resConfig.ErrorDocument.Key
//            || typeof resConfig.ErrorDocument.Key !== 'string'
//            || resConfig.ErrorDocument.Key === "")) {
// log.warn('invalid website configuration xml', { xml: result });
// return next(errors.MalformedXML);
//            }

//        if (resConfig.RoutingRules &&
//            ())
//        if (resConfig.)
}

export function parseWebsiteConfigXml(xml, log, next) {
    parseString(xml, (err, result) => {
        if (err) {
            log.warn('invalid xml', { xmlObj: xml });
            return next(errors.MalformedXML);
        }
        console.log('===== xml', xml);
        console.log('===== parseString res', JSON.stringify(result, null, 4));

        /* is this async? should i put in some sort of async function?
            my guess is almost all JS functions as async, so I better make
            a callback and return error from func*/
        _validateWebsiteConfigXml(result, xml, log, (err, config) => {
            if (err) {
                // some kind of warning?
                // log.warn('', { xmlObj: xml });
                return next(err);
            }
            // for now for testing, just set to config
            log.trace('website configuration', { config });
            return next(null, config);
        });
        return undefined;
    });
}
