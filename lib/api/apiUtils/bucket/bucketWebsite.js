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

        // consider conflating this and next check into one util function
        // remember error codes will differ though
        if (resConfig.RedirectAllRequestsTo &&
        (!Array.isArray(resConfig.RedirectAllRequestsTo)
        || resConfig.RedirectAllRequestsTo.length !== 1
        || !resConfig.RedirectAllRequestsTo[0].HostName
        || !Array.isArray(resConfig.RedirectAllRequestsTo[0].HostName)
        || resConfig.RedirectAllRequestsTo[0].HostName.length !== 1)) {
            log.warn('RedirectAllRequestsTo not well-formed', { xml: result });
            return next(errors.MalformedXML);
        } else if (resConfig.RedirectAllRequestsTo &&
        (typeof resConfig.RedirectAllRequestsTo[0].HostName[0] !== 'string'
        || resConfig.RedirectAllRequestsTo[0].HostName[0] === '')) {
            log.warn('HostName required in RedirectAllRequestsTo',
            { xml: result });
            return next(errors.InvalidRequest);
        }

        if (resConfig.IndexDocument && (!Array.isArray(resConfig.IndexDocument)
        || resConfig.IndexDocument.length !== 1
        || !resConfig.IndexDocument[0].Suffix
        || !Array.isArray(resConfig.IndexDocument[0].Suffix)
        || resConfig.IndexDocument[0].Suffix.length !== 1)) {
            log.warn('IndexDocument not well-formed', { xml: result });
            return next(errors.MalformedXML);
        } else if (resConfig.IndexDocument &&
        (typeof resConfig.IndexDocument[0].Suffix[0] !== 'string'
        || resConfig.IndexDocument[0].Suffix[0] === ''
        || resConfig.IndexDocument[0].Suffix[0].indexOf('/') !== -1)) {
            log.warn('IndexDocument Suffix not well-formed', { xml: result });
            return next(errors.InvalidArgument);
        }
//        if (resConfig.IndexDocument &&
//            (!resConfig.IndexDocument.Suffix
//            || typeof resConfig.IndexDocument.Suffix !== 'string'
//            || resConfig.IndexDocument.Suffix === "")) {
//                log.warn('invalid website configuration xml', { xml: result });
//                return next(errors.MalformedXML);
//            }

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
        const config = resConfig;
        // for now for testing, just set to resConfig
        // { result['ErrorDocument'], result.;
        log.trace('website configuration', { config });
        return next(null, config);
    });
}
