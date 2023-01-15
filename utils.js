const async = require('async');
const helpers = require('artillery/core/lib/engine_util');


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

        try {
            parsedData.body = JSON.parse(data);
            parsedData.headers['content-type'] = 'application/json'
        } catch (e) {
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
