import { parseString } from 'xml2js';

import { errors } from 'arsenal';
// import constants from '../../constants';
// import escapeForXML from '../utilities/escapeForXML';

/** Check if parsed xml element contains a required child element
* @param {obj} obj - represents xml element to check for child element
* @param {string} requiredElem - name of required child element
* @param {boolean} isList - indicates if parent can have multiple required elem
* @return {boolean} true / false - if parsed xml element contains required child
*/
function _xmlContainsRequiredElem(obj, requiredElem, isList) {
    console.log('===obj', JSON.stringify(obj, null, 4));
//    console.log('===obj.length', obj.length)
//    console.log(`===Array.isArray(obj[0][${requiredElem}])?`, Array.isArray(obj[0][requiredElem]))

    if (!Array.isArray(obj)
    || obj.length !== 1
    || !Array.isArray(obj[0][requiredElem])) {
        return false;
    }
    if (!isList) {
        if (obj[0][requiredElem].length !== 1) {
            return false;
        }
    } else if (obj[0][requiredElem].length === 0) {
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
    let errMsg;
    if (!jsonResult || !jsonResult.WebsiteConfiguration) {
        errMsg = 'invalid website configuration xml';
        log.warn(errMsg, { xml });
        return cb(errors.MalformedXML.customizeDescription(errMsg));
    }

    const resultConfig = jsonResult.WebsiteConfiguration;
    if (!(resultConfig.IndexDocument || resultConfig.RedirectAllRequestsTo)) {
        errMsg = 'Value for IndexDocument Suffix must be provided if ' +
        'RedirectAllRequestsTo is empty';
        log.warn(errMsg, { xml });
        return cb(errors.InvalidArgument.customizeDescription(errMsg));
    }

    if (resultConfig.RedirectAllRequestsTo) {
        const parent = resultConfig.RedirectAllRequestsTo;
        if (!_xmlContainsRequiredElem(parent, 'HostName')) {
            errMsg = 'RedirectAllRequestsTo not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        } else if (typeof parent[0].HostName[0] !== 'string'
        || parent[0].HostName[0] === '') {
            errMsg = 'Valid HostName required in RedirectAllRequestsTo';
            log.warn(errMsg, { xml });
            return cb(errors.InvalidRequest.customizeDescription(errMsg));
        }
        // maybe here I should save some info into config?
    }

    if (resultConfig.IndexDocument) {
        const parent = resultConfig.IndexDocument;
        if (!_xmlContainsRequiredElem(parent, 'Suffix')) {
            errMsg = 'IndexDocument is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        } else if (typeof parent[0].Suffix[0] !== 'string'
        || parent[0].Suffix[0] === ''
        || parent[0].Suffix[0].indexOf('/') !== -1) {
            errMsg = 'IndexDocument Suffix is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.InvalidArgument.customizeDescription(errMsg));
        }
        // maybe here I should save some info into config?
    }

    if (resultConfig.ErrorDocument) {
        const parent = resultConfig.ErrorDocument;
        if (!_xmlContainsRequiredElem(parent, 'Key')) {
            errMsg = 'ErrorDocument is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        } else if (typeof parent[0].Key[0] !== 'string'
        || parent[0].Key[0] === '') {
            errMsg = 'ErrorDocument Key is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.InvalidArgument.customizeDescription(errMsg));
        }
        // maybe here I should save some info into config?
    }

    if (resultConfig.RoutingRules) {
        const parent = resultConfig.RoutingRules;
        if (!_xmlContainsRequiredElem(parent, 'RoutingRule', true)) {
            errMsg = 'RoutingRules is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        }
        for (let i = 0; i < parent[0].RoutingRule.length; i++) {
            const rule = parent[0].RoutingRule[i];
            if (!(Array.isArray(rule.Redirect) && rule.Redirect.length === 1)) {
                errMsg = 'RoutingRule requires Redirect, which is ' +
                'missing or not well-formed';
                log.warn(errMsg, { xml });
                return cb(errors.MalformedXML.customizeDescription(errMsg));
            }
            const redirect = rule.Redirect;
            // looks like AWS doesn't actually check this one below:
            if (!(_xmlContainsRequiredElem(redirect, 'Protocol')
            || _xmlContainsRequiredElem(redirect, 'HostName')
            || _xmlContainsRequiredElem(redirect, 'ReplaceKeyPrefixWith')
            || _xmlContainsRequiredElem(redirect, 'ReplaceKeyWith')
            || _xmlContainsRequiredElem(redirect, 'HttpRedirectCode'))) {
                errMsg = 'Redirect must contain at least one of ' +
                'following: Protocol, HostName, ReplaceKeyPrefixWith, ' +
                'ReplaceKeyWith, or HttpRedirectCode element';
                log.warn(errMsg, { xml });
                return cb(errors.MalformedXML.customizeDescription(errMsg));
            }
            if (redirect[0].Protocol && !(redirect[0].Protocol[0] === 'http'
            || redirect[0].Protocol[0] === 'https')) {
                errMsg = 'Invalid protocol, protocol can be http or https. ' +
                'If not defined the protocol will be selected automatically.';
                log.warn(errMsg, { xml });
                return cb(errors.InvalidRequest.customizeDescription(errMsg));
            }
            // consider validating other potential children are strings
            if (_xmlContainsRequiredElem(redirect, 'ReplaceKeyPrefixWith')
            && _xmlContainsRequiredElem(redirect, 'ReplaceKeyWith')) {
                errMsg = 'Redirect must not contain both ReplaceKeyWith ' +
                'and ReplaceKeyPrefixWith';
                log.warn(errMsg, { xml });
                return cb(errors.MalformedXML.customizeDescription(errMsg));
            }
        }
    }

    return cb(null, jsonResult);
}

export function parseWebsiteConfigXml(xml, log, next) {
    parseString(xml, (err, result) => {
        if (err) {
            log.warn('invalid xml', { xmlObj: xml });
            return next(errors.MalformedXML);
        }
        console.log('===== xml', xml);
        console.log('===== parseString res', JSON.stringify(result, null, 4));

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
