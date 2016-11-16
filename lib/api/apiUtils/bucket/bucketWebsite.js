import { parseString } from 'xml2js';

import { errors } from 'arsenal';
// import constants from '../../constants';
// import escapeForXML from '../utilities/escapeForXML';

export function parseWebsiteConfigXml(xml, log, next) {
    return parseString(xml, (err, result) => {
        if (err) {
            log.warn('invalid xml', { xmlObj: xml });
            return next(errors.MalformedXML);
        }
        console.log(xml);
        console.log(JSON.stringify(result, null, 4));

        /* TODO: Need to test all of the following against AWS
        to verify err and errcode */

        // WebsiteConfiguration must contain IndexDocument, unless redirecting
        // all requests
        const resConfig = result.WebsiteConfiguration
        if (!resConfig ||
            !(resConfig.IndexDocument || resConfig.RedirectAllRequestsTo)) {
                log.warn('invalid acl', { acl: result });
                return next(errors.MalformedACLError);
            }

        // note for testing: AWS does not specify must not be empty (but it
        // could be a problem if user input leaves it empty and we expect other
        // wise, right? if we don't check for that later)
        if (resConfig.RedirectAllRequests &&
            (!resConfig.RedirectAllRequests.HostName
            || typeof resConfig.RedirectAllRequests.HostName !== 'string')) {
                log.warn('invalid acl', { acl: result });
                return next(errors.MalformedACLError);
            }

        if (resConfig.IndexDocument &&
            (!resConfig.IndexDocument.Suffix
            || typeof resConfig.IndexDocument.Suffix !== 'string'
            || resConfig.IndexDocument.Suffix === "")) {
                log.warn('invalid acl', { acl: result });
                return next(errors.MalformedACLError);
            }

        // note for testing: AWS does not specify must not be empty
        if (resConfig.ErrorDocument &&
            (!resConfig.ErrorDocument.Key
            || typeof resConfig.ErrorDocument.Key !== 'string'
            || resConfig.ErrorDocument.Key === "")) {
                log.warn('invalid acl', { acl: result });
                return next(errors.MalformedACLError);
            }

        
        const config = { result['ErrorDocument'], result.;
        log.trace('website configuration', { config });
        return next(null, config);
    });
}
