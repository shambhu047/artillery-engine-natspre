const async = require('async');
const helpers = require('artillery/core/lib/engine_util');
const _ = require("lodash");


class NatsEngineUtils {
    static executeBeforeRequestFunctions(script, functionNames, params, context, ee, doneCallback) {
        async.eachSeries(
            functionNames,
            (functionName, next) => {
                let fn = helpers.template(functionName, context);

                let processFunc = script.config.processor[fn];
                if (!processFunc) {
                    processFunc = NatsEngineUtils.noopCallbackFunction();
                    console.log(`WARN: specified function ${fn} could not be found`);
                }

                processFunc(params, context, ee, (err) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null);
                });
            },

            doneCallback,
        );
    }

    static executeAfterResponseFunctions(script, functionNames, params, response, context, ee, doneCallback) {
        async.eachSeries(
            functionNames,
            (functionName, next) => {
                let fn = helpers.template(functionName, context);
                let processFunc = script.config.processor[fn];
                if (!processFunc) {
                    processFunc = NatsEngineUtils.noopCallbackFunction();
                    console.log(`WARN: specified function ${fn} could not be found`);
                }

                processFunc(params, response, context, ee, (err) => {
                    if (err) {
                        return next(err);
                    }
                    return next(null);
                });
            },
            doneCallback,
        );
    }

    static noopCallbackFunction() {
        return () => {
            const cb = arguments[arguments.length - 1];
            return cb(null);
        }
    }

    static captureAndMatchFunction(requestParams, response, context, ee, next) {
        const captureDoneCallback = (err, result) => {
            if (result && result.captures) {
                if (result.captures) {
                    Object.keys(result.captures).forEach((k) => {
                        if (result.captures[k] && !result.captures[k]['failed']) {
                            _.set(context.vars, k, result.captures[k]['value'])
                        }
                    })
                }
                // FIXME Handle failed captures
                // TODO matches
            }
            return next()
        }

        return helpers.captureOrMatch(requestParams, response, context, captureDoneCallback);
    }

    static parseSafely(data, headers, statusCode = 200) {
        const parsedData = {
            body: data,
            statusCode: statusCode,
            headers: {
                'content-type': ''
            }
        };

        if (headers) {
            headers.keys().forEach(k => {
                parsedData.headers[k] = headers.get(k)
            })
        }

        if (typeof data === 'string') {
            try {
                parsedData.body = JSON.parse(data);
                parsedData.headers['content-type'] = 'application/json'
            } catch (e) {
            }
        }

        return parsedData;
    }

    static emptyResponse(statusCode = 200) {
        return {
            body: null,
            statusCode: statusCode,
            headers: {},
        }
    }
}


module.exports = NatsEngineUtils
