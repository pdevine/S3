import { parseString } from 'xml2js';

import { errors } from 'arsenal';
// import constants from '../../constants';
// import escapeForXML from '../utilities/escapeForXML';

/** Check if parsed xml element contains a specified child element
* @param {obj} obj - represents xml element to check for child element
* @param {string} requiredElem - name of child element
* @param {boolean} isList - indicates if parent is list of children elements
* @return {boolean} true / false - if parsed xml element contains child
*/
function _xmlContainsElem(obj, requiredElem, isList) {
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

/** Validate XML; on success pass object representing website configuration
* to callback, otherwise pass error
* @param {obj} jsonResult - website configuration xml parsed into JSON
* @param {string} xml - xml from putBucketWebsite request body
* @param {logger} log - logger object
* @param {callback} cb - callback
* @return {undefined} and calls callback
*/
function _validateWebsiteConfigXml(jsonResult, xml, log, cb) {
    const websiteConfiguration = {};
    let errMsg;

    function _validateStringNotEmpty(obj, key, errType, customErrMsg) {
        errMsg = customErrMsg || `${key} is not well-formed`;
        if (typeof obj[`${key}`][0] !== 'string' || obj[`${key}`][0] === '') {
            errMsg = `${key} is not well-formed`;
            log.warn(errMsg, { xml });
            return cb(errors[errType].customizeDescription(errMsg));
        }
        return undefined;
    }

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
        if (!_xmlContainsElem(parent, 'HostName')) {
            errMsg = 'RedirectAllRequestsTo not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        }
        _validateStringNotEmpty(parent[0], 'HostName', 'InvalidRequest',
        'Valid HostName required in RedirectAllRequestsTo');
//        else if (typeof parent[0].HostName[0] !== 'string'
//        || parent[0].HostName[0] === '') {
//            errMsg = 'Valid HostName required in RedirectAllRequestsTo';
//            log.warn(errMsg, { xml });
//            return cb(errors.InvalidRequest.customizeDescription(errMsg));
//        }
        websiteConfiguration.redirectAllRequestsTo =
            { hostName: parent[0].HostName[0] };
        if (_xmlContainsElem(parent, 'Protocol')) {
            if (parent[0].Protocol[0] !== 'http' &&
            parent[0].Protocol[0] !== 'https') {
                errMsg = 'Invalid protocol, protocol can be http or https. ' +
                'If not defined the protocol will be selected automatically.';
                log.warn(errMsg, { xml });
                return cb(errors.InvalidRequest.customizeDescription(errMsg));
            }
            websiteConfiguration.redirectAllRequestsTo.protocol =
                parent[0].Protocol[0];
        }
    }

    if (resultConfig.IndexDocument) {
        const parent = resultConfig.IndexDocument;
        if (!_xmlContainsElem(parent, 'Suffix')) {
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
        websiteConfiguration.indexDocument = parent[0].Suffix[0];
    }

    if (resultConfig.ErrorDocument) {
        const parent = resultConfig.ErrorDocument;
        if (!_xmlContainsElem(parent, 'Key')) {
            errMsg = 'ErrorDocument is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        } else if (typeof parent[0].Key[0] !== 'string'
        || parent[0].Key[0] === '') {
            errMsg = 'ErrorDocument Key is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.InvalidArgument.customizeDescription(errMsg));
        }
        websiteConfiguration.errorDocument = parent[0].Key[0];
    }

    if (resultConfig.RoutingRules) {
        websiteConfiguration.routingRules = [];
        const parent = resultConfig.RoutingRules;
        if (!_xmlContainsElem(parent, 'RoutingRule', true)) {
            errMsg = 'RoutingRules is not well-formed';
            log.warn(errMsg, { xml });
            return cb(errors.MalformedXML.customizeDescription(errMsg));
        }
        for (let i = 0; i < parent[0].RoutingRule.length; i++) {
            const rule = parent[0].RoutingRule[i];
            const ruleObj = { redirect: {} };
            if (!(Array.isArray(rule.Redirect) && rule.Redirect.length === 1)) {
                errMsg = 'RoutingRule requires Redirect, which is ' +
                'missing or not well-formed';
                log.warn(errMsg, { xml });
                return cb(errors.MalformedXML.customizeDescription(errMsg));
            }
            // looks like AWS doesn't actually check this one below:
            if (!(_xmlContainsElem(rule.Redirect, 'Protocol')
            || _xmlContainsElem(rule.Redirect, 'HostName')
            || _xmlContainsElem(rule.Redirect, 'ReplaceKeyPrefixWith')
            || _xmlContainsElem(rule.Redirect, 'ReplaceKeyWith')
            || _xmlContainsElem(rule.Redirect, 'HttpRedirectCode'))) {
                errMsg = 'Redirect must contain at least one of ' +
                'following: Protocol, HostName, ReplaceKeyPrefixWith, ' +
                'ReplaceKeyWith, or HttpRedirectCode element';
                log.warn(errMsg, { xml });
                return cb(errors.MalformedXML.customizeDescription(errMsg));
            }
            if (rule.Redirect[0].Protocol) {
                if (!rule.Redirect[0].Protocol[0] === 'http' &&
                !rule.Redirect[0].Protocol[0] === 'https') {
                    errMsg = 'Invalid protocol, protocol can be http or ' +
                    'https. If not defined protocol will be auto-selected.';
                    log.warn(errMsg, { xml });
                    return cb(errors.InvalidRequest
                        .customizeDescription(errMsg));
                }
                ruleObj.redirect.protocol = rule.Redirect[0].Protocol[0];
            }
//            async.each(['HostName', 'ReplaceKeyPrefixWith',
//            'ReplaceKeyWith', 'HttpRedirectCode'], key => {
//                if (rule.Redirect[0].key) {
//                    if (typeof rule.Redirect.key[0] !== 'string' ||
//                    rule.Redirect.key[0] === "") {
//                        errMsg = `${key} is not well-formed`;
//                        log.warn(errMsg, { xml });
//                        return cb(errors.MalformedXML
//                            .customizeDescription(errMsg));
//                    }
//                    objKey = key.charAt(0).toLowerCase() + key.slice(1);
//                    ruleObj.redirect.objKey= rule.Redirect[0].key[0];
//                }
            // consider validating other potential children are strings
            if (_xmlContainsElem(rule.Redirect, 'ReplaceKeyPrefixWith')
            && _xmlContainsElem(rule.Redirect, 'ReplaceKeyWith')) {
                errMsg = 'Redirect must not contain both ReplaceKeyWith ' +
                'and ReplaceKeyPrefixWith';
                log.warn(errMsg, { xml });
                return cb(errors.MalformedXML.customizeDescription(errMsg));
            }
            if (Array.isArray(rule.Condition) && rule.Condition.length === 1) {
                if (!_xmlContainsElem(rule.Condition, 'KeyPrefixEquals') &&
                !_xmlContainsElem(rule.Condition,
                'HttpErrorCodeReturnedEquals')) {
                    errMsg = 'Condition is not well-formed or empty. ' +
                    'Condition should contain valid KeyPrefixEquals or ' +
                    'HttpErrorCodeReturnEquals element.';
                    log.warn(errMsg, { xml });
                    return cb(errors.InvalidRequest
                        .customizeDescription(errMsg));
                }
            }
        }
    }
    // ok, so the plan is for each element, filter the properties that we would
    // care about into an object
    // and maybe there we can bother checking about empty strings and what not
    // in a forEach cycle and return a MalformedXML error if they are empty or
    // smth
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
