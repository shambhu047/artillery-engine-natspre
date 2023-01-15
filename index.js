'use strict';

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

const debug = require('debug')('engine:natspre');
const A = require('async');
const _ = require('lodash');
const helpers = require('artillery/core/lib/engine_util');
const nats = require('nats')

const NatsEngineUtils = require('./utils');


class NatsPreEngine {
    constructor(script, ee) {
        debug("Instantiating NatsPreEngine")

        this.script = script;
        this.ee = ee;
        this.helpers = helpers;
        this.config = script.config;

        this.config.processor = this.config.processor || {};
    }

    createScenario(scenarioSpec, ee) {

        const beforeScenarioFunctions = _.map(scenarioSpec.beforeScenario, functionName => ({'function': functionName}));
        const afterScenarioFunctions = _.map(scenarioSpec.afterScenario, functionName => ({'function': functionName}));

        scenarioSpec.flow = beforeScenarioFunctions.concat(scenarioSpec.flow.concat(afterScenarioFunctions));

        const tasks = scenarioSpec.flow.map(rs => this.step(rs, ee, {
            beforeRequest: scenarioSpec.beforeRequest,
            afterResponse: scenarioSpec.afterResponse,
        }));

        return this.compile(tasks, scenarioSpec.flow, ee);
    }

    step(rs, ee, opts) {
        opts = opts || {};

        if (rs.log) {
            return this.createLogHandler(rs, ee, opts)
        } else if (rs.think) {
            return this.createThinkHandler(rs, ee, opts)
        } else if (rs.function) {
            return this.createFunctionHandler(rs, ee, opts)
        } else if (rs.pub) {
            return this.createPubHandler(rs, ee, opts)
        } else if (rs.req) {
            return this.createReqHandler(rs, ee, opts)
        }


        return function (context, callback) {
            return callback(null, context);
        };
    }

    createLogHandler(rs, ee, opts) {
        return (context, callback) => {
            return process.nextTick(function () {
                callback(null, context);
            });
        };
    }

    createThinkHandler(rs, ee, opts) {
        return this.helpers.createThink(rs, _.get(this.config, 'defaults.think', {}));
    }

    createFunctionHandler(rs, ee, opts) {
        return (context, callback) => {
            let func = this.config.processor[rs.function];
            if (!func) {
                return process.nextTick(function () {
                    callback(null, context);
                });
            }

            return func(context, ee, function () {
                return callback(null, context);
            });
        };
    }

    createPubHandler(rs, ee, opts) {
        return (context, callback) => {

            context.funcs.$increment = this.increment;
            context.funcs.$decrement = this.decrement;
            context.funcs.$contextUid = () => context._uid;

            const payload = typeof rs.pub.payload === 'object'
                ? JSON.stringify(rs.pub.payload)
                : String(rs.pub.payload);

            const pubParams = {
                Payload: helpers.template(payload, context),
                Subject: rs.pub.subject
            };

            // construct and keep all params to pass it to hooks
            // so that plugins can use it if needed
            const params = _.assign({
                url: rs.pub.payload,
                pubParams: pubParams,
            }, rs.pub);


            const beforeRequestFunctionNames = _.concat(opts.beforeRequest || [], rs.pub.beforeRequest || []);

            NatsEngineUtils.executeBeforeRequestFunctions(
                this.script,
                beforeRequestFunctionNames,
                params,
                context,
                ee,
                function done(err) {
                    if (err) {
                        debug(err);
                        return callback(err, context);
                    }

                    ee.emit('request');
                    const startedAt = process.hrtime();

                    // after running beforeRequest functions
                    // the context could have changed
                    // we need to rerun template on payload
                    pubParams.Payload = helpers.template(payload, context);

                    try {
                        debug("Publishing to: [" + pubParams.Subject + "] Message: [" + pubParams.Payload + "]")
                        context.nc.publish(pubParams.Subject, context.sc.encode(pubParams.Payload))
                        debug("Published")

                        context.nc.flush().then((value) => {
                            const endedAt = process.hrtime(startedAt);

                            let delta = (endedAt[0] * 1e9) + endedAt[1];
                            const code = 0
                            ee.emit('response', delta, code, context._uid);

                            const response = NatsEngineUtils.emptyResponse();
                            const captureDoneCallback = (err, result) => {
                                if (result && result.captures) {
                                    // TODO handle matches
                                    let haveFailedCaptures = _.some(result.captures, (v, k) => v === '');
                                    if (!haveFailedCaptures) {
                                        _.each(result.captures, function (v, k) {
                                            _.set(context.vars, k, v);
                                        });
                                    }
                                }

                                const afterResponseFunctionNames = _.concat(opts.afterResponse || [], rs.pub.afterResponse || []);
                                NatsEngineUtils.executeAfterResponseFunctions(
                                    this.script,
                                    afterResponseFunctionNames,
                                    params,
                                    response,
                                    context,
                                    ee,
                                    (err) => {
                                        if (err) {
                                            debug(err);
                                            return callback(err, context);
                                        }

                                        return callback(null, context);
                                    }
                                )
                            }

                            helpers.captureOrMatch(params, response, context, captureDoneCallback);
                        }).catch((err) => {
                            debug("Failed to flush: " + err)
                            ee.emit('error', err);
                            return callback(err, context);
                        })
                    } catch (err) {
                        debug("Failed to publish: " + err);
                        ee.emit('error', err);
                        return callback(err, context);
                    }
                }
            )
        }
    }

    createReqHandler(rs, ee, opts) {
        console.log("Request handler")

        return (context, callback) => {

            context.funcs.$increment = this.increment;
            context.funcs.$decrement = this.decrement;
            context.funcs.$contextUid = () => context._uid;

            const payload = typeof rs.req.payload === 'object'
                ? JSON.stringify(rs.req.payload)
                : String(rs.req.payload);

            const pubParams = {
                Payload: helpers.template(payload, context),
                Subject: rs.req.subject,
                Timeout: rs.req.timeout || 5000,
                Headers: rs.req.headers || {}
            };

            // construct and keep all params to pass it to hooks
            // so that plugins can use it if needed
            const params = _.assign({
                url: rs.req.payload,
                pubParams: pubParams,
            }, rs.req);


            const beforeRequestFunctionNames = _.concat(opts.beforeRequest || [], rs.req.beforeRequest || []);

            NatsEngineUtils.executeBeforeRequestFunctions(
                this.script,
                beforeRequestFunctionNames,
                params,
                context,
                ee,
                function done(err) {
                    if (err) {
                        debug(err);
                        return callback(err, context);
                    }

                    ee.emit('request');
                    const startedAt = process.hrtime();

                    console.log("Requesting to: " + pubParams.Subject)
                    console.log("Message: " + pubParams.Payload)

                    const natsHeaders = nats.headers()
                    Object.keys(pubParams.Headers).forEach(k => {
                        natsHeaders.append(k, pubParams.Headers[k])
                    })

                    const requestOptions = {
                        timeout: pubParams.Timeout,
                        headers: natsHeaders
                    }

                    try {
                        console.log("Requesting to: [" + pubParams.Subject + "] Message: [" + pubParams.Payload + "]")
                        context.nc.request(pubParams.Subject, context.sc.encode(pubParams.Payload), requestOptions).then((msg) => {
                            const endedAt = process.hrtime(startedAt);

                            const decodedResponse = context.sc.decode(msg.data)
                            const response = NatsEngineUtils.parseSafely(decodedResponse, msg.headers)

                            let delta = (endedAt[0] * 1e9) + endedAt[1];
                            const code = 0
                            ee.emit('response', delta, code, context._uid);

                            console.log("Response: " + JSON.stringify(response))
                            const captureDoneCallback = (err, result) => {
                                if (result && result.captures) {
                                    // TODO handle matches
                                    let haveFailedCaptures = _.some(result.captures, (v, k) => v === '');
                                    if (!haveFailedCaptures) {
                                        _.each(result.captures, function (v, k) {
                                            _.set(context.vars, k, v);
                                        });
                                    }
                                }

                                const afterResponseFunctionNames = _.concat(opts.afterResponse || [], rs.req.afterResponse || []);
                                NatsEngineUtils.executeAfterResponseFunctions(
                                    this.script,
                                    afterResponseFunctionNames,
                                    params,
                                    response,
                                    context,
                                    ee,
                                    (err) => {
                                        if (err) {
                                            debug(err);
                                            return callback(err, context);
                                        }

                                        return callback(null, context);
                                    }
                                )
                            }

                            helpers.captureOrMatch(params, response, context, captureDoneCallback);
                        }).catch((err) => {
                            ee.emit('error', err);
                            return callback(err, context);
                        })
                    } catch (err) {
                        debug("Failed to request: " + err);
                        ee.emit('error', err);
                        return callback(err, context);
                    }
                }
            )
        }
    }

    compile(tasks, scenarioSpec, ee) {
        return (initialContext, callback) => {
            const init = (next) => {
                debug("Initializing nats connection")

                let natsOptions = {
                    server: this.script.config.natspre.server || 'demo.nats.io:4222'
                };

                if (this.script.config.natspre.subject) {
                    natsOptions.subject = this.script.config.natspre.subject;
                }

                nats.connect({servers: natsOptions.server}).then((nc) => {
                    debug("Successfully established nats connection")

                    initialContext.sc = nats.StringCodec();
                    initialContext.nc = nc
                    ee.emit('started');

                    return next(null, initialContext);
                }).catch((err) => {
                    console.log("Error")
                    ee.emit('error', err);
                })
            };

            let steps = [init].concat(tasks);

            A.waterfall(
                steps,
                function done(err, context) {
                    if (err) {
                        debug(err);
                    }
                    return callback(err, context);
                });
        };
    }

    increment(value) {
        return Number.isInteger(value) ? value + 1 : NaN;
    }

    decrement(value) {
        return Number.isInteger(value) ? value - 1 : NaN;
    }
}

module.exports = NatsPreEngine;
