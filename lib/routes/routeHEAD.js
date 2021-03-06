const api = require('../api/api');
const { errors } = require('arsenal');

const routesUtils = require('./routesUtils');
const statsReport500 = require('../utilities/statsReport500');

function routeHEAD(request, response, log, statsClient) {
    log.debug('routing request', { method: 'routeHEAD' });
    if (request.bucketName === undefined) {
        log.trace('head request without bucketName');
        routesUtils.responseXMLBody(errors.MethodNotAllowed,
            null, response, log);
    } else if (request.objectKey === undefined) {
        // HEAD bucket
        api.callApiMethod('bucketHead', request, response, log,
            (err, corsHeaders) => {
                statsReport500(err, statsClient);
                return routesUtils.responseNoBody(err, corsHeaders, response,
                    200, log);
            });
    } else {
        // HEAD object
        api.callApiMethod('objectHead', request, response, log,
            (err, resHeaders) => {
                statsReport500(err, statsClient);
                return routesUtils.responseContentHeaders(err, {}, resHeaders,
                    response, log);
            });
    }
}

module.exports = routeHEAD;
