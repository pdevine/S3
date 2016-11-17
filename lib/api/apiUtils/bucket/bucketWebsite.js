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
        console.log('===== xml', xml);
        console.log('===== parseString res', JSON.stringify(result, null, 4));

        if (!result || !result.WebsiteConfiguration) {
            log.warn('invalid website configuration xml', { xml: result });
            return next(errors.MalformedXML);
        }

        const resConfig = result.WebsiteConfiguration;
        if (!(resConfig.IndexDocument || resConfig.RedirectAllRequestsTo)) {
            log.warn('Value for IndexDocument Suffix must be provided if ' +
            'RedirectAllRequestsTo is empty', { xml: result });
            return next(errors.InvalidArgument);
        }

        // note for testing: AWS does not specify must not be empty (but it
        // could be a problem if user input leaves it empty and we expect other
        // wise, right? if we don't check for that later)
        if (resConfig.RedirectAllRequestsTo &&
            (!resConfig.RedirectAllRequestsTo.HostName
            || typeof resConfig.RedirectAllRequestsTo.HostName !== 'string')) {
            log.warn('invalid website configuration xml', { xml: result });
            return next(errors.MalformedACLError);
        }

//        if (resConfig.IndexDocument &&
//            (!resConfig.IndexDocument.Suffix
//            || typeof resConfig.IndexDocument.Suffix !== 'string'
//            || resConfig.IndexDocument.Suffix === "")) {
//                log.warn('invalid acl', { acl: result });
//                return next(errors.MalformedACLError);
//            }

        // note for testing: AWS does not specify must not be empty
//        if (resConfig.ErrorDocument &&
//            (!resConfig.ErrorDocument.Key
//            || typeof resConfig.ErrorDocument.Key !== 'string'
//            || resConfig.ErrorDocument.Key === "")) {
//                log.warn('invalid acl', { acl: result });
//                return next(errors.MalformedACLError);
//            }

//        if (resConfig.RoutingRules &&
//            ())
//        if (resConfig.)
        const config = resConfig;
        // for now for testing, just set to resConfig
        // { result['ErrorDocument'], result.;
        log.trace('website configuration', { config });
        return next(null, config);
    });
}
