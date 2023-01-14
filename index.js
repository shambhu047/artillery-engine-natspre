'use strict';

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

const debug = require('debug')('engine:natspre');
const A = require('async');
const _ = require('lodash');
const helpers = require('artillery/core/lib/engine_util');

const nats = require('nats')

const utils = require('./utils');


function NatsEngine(script, ee) {
    this.script = script;
    this.ee = ee;
    this.helpers = helpers;
    this.config = script.config;

    this.config.processor = this.config.processor || {};
    console.log("Hello")

    return this;
}

NatsEngine.prototype.createScenario = function createScenario(scenarioSpec, ee) {

    const beforeScenarioFns = _.map(
        scenarioSpec.beforeScenario,
        function (hookFunctionName) {
            return {'function': hookFunctionName};
        });

    const afterScenarioFns = _.map(
        scenarioSpec.afterScenario,
        function (hookFunctionName) {
            return {'function': hookFunctionName};
        });

    const newFlow = beforeScenarioFns.concat(
        scenarioSpec.flow.concat(afterScenarioFns));

    scenarioSpec.flow = newFlow;

    const tasks = scenarioSpec.flow.map(rs => this.step(rs, ee, {
        beforeRequest: scenarioSpec.beforeRequest,
        afterResponse: scenarioSpec.afterResponse,
    }));

    return this.compile(tasks, scenarioSpec.flow, ee);
};

NatsEngine.prototype.step = function step(rs, ee, opts) {
    opts = opts || {};
    let self = this;

    if (rs.log) {
        return function log(context, callback) {
            return process.nextTick(function () {
                callback(null, context);
            });
        };
    }

    if (rs.think) {
        return this.helpers.createThink(rs, _.get(self.config, 'defaults.think', {}));
    }

    if (rs.function) {
        return function (context, callback) {
            let func = self.config.processor[rs.function];
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

    if (rs.pub) {

        return function pub(context, callback) {

            context.funcs.$increment = self.$increment;
            context.funcs.$decrement = self.$decrement;
            context.funcs.$contextUid = function () {
                return context._uid;
            };

            const payload = typeof rs.pub.payload === 'object'
                ? JSON.stringify(rs.pub.payload)
                : String(rs.pub.payload);

            const pubParams = {
                Payload: helpers.template(payload, context),
                Subject: rs.pub.subject
            };

            // build object to pass to hooks
            // we do not pass only pub params but also additional information
            // we need to make the engine work with other plugins
            const params = _.assign({
                url: rs.pub.payload,
                pubParams: pubParams,
            }, rs.pub);


            const beforeRequestFunctionNames = _.concat(opts.beforeRequest || [], rs.pub.beforeRequest || []);

            utils.processBeforeRequestFunctions(
                self.script,
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
                        console.log("Publishing to: " + pubParams.Subject)
                        console.log("Message: " + pubParams.Payload)
                        context.nc.publish(pubParams.Subject, context.sc.encode(pubParams.Payload))
                        console.log("Published: " + pubParams.Payload)

                        context.nc.flush().then((value) => {
                            console.log("Flushed: " + value)
                            const endedAt = process.hrtime(startedAt);
                            let delta = (endedAt[0] * 1e9) + endedAt[1];
                            const code = 0
                            ee.emit('response', delta, code, context._uid);

                            const response = {
                                body: null,
                                statusCode: 200,
                                headers: {},
                            };

                            helpers.captureOrMatch(
                                params,
                                response,
                                context,
                                function captured(err, result) {
                                    if (result && result.captures) {
                                        // TODO handle matches
                                        let haveFailedCaptures = _.some(result.captures, function (v, k) {
                                            return v === '';
                                        });

                                        if (!haveFailedCaptures) {
                                            _.each(result.captures, function (v, k) {
                                                _.set(context.vars, k, v);
                                            });
                                        }
                                    }

                                    const afterResponseFunctionNames = _.concat(opts.afterResponse || [], rs.pub.afterResponse || []);

                                    utils.processAfterResponseFunctions(
                                        self.script,
                                        afterResponseFunctionNames,
                                        params,
                                        response,
                                        context,
                                        ee,
                                        function done(err) {
                                            if (err) {
                                                debug(err);
                                                return callback(err, context);
                                            }

                                            return callback(null, context);
                                        }
                                    );
                                }
                            );
                        }).catch((err) => {
                            console.log("Flush Error: " + err)
                            debug(err);
                            ee.emit('error', err);
                            return callback(err, context);
                        })
                    } catch (err) {
                        console.log("Publish Error: " + err)
                        debug(err);
                        ee.emit('error', err);
                        return callback(err, context);
                    }
                }
            )
        };
    }

    if (rs.req) {

        return function req(context, callback) {

            context.funcs.$increment = self.$increment;
            context.funcs.$decrement = self.$decrement;
            context.funcs.$contextUid = function () {
                return context._uid;
            };

            const payload = typeof rs.req.payload === 'object'
                ? JSON.stringify(rs.req.payload)
                : String(rs.req.payload);

            const pubParams = {
                Payload: helpers.template(payload, context),
                Subject: rs.req.subject,
                Timeout: rs.req.timeout || 5000,
                Headers: rs.req.headers || {}
            };

            // build object to pass to hooks
            // we do not pass only pub params but also additional information
            // we need to make the engine work with other plugins
            const params = _.assign({
                url: rs.req.payload,
                pubParams: pubParams,
            }, rs.req);


            const beforeRequestFunctionNames = _.concat(opts.beforeRequest || [], rs.req.beforeRequest || []);

            utils.processBeforeRequestFunctions(
                self.script,
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

                        context.nc.request(pubParams.Subject, context.sc.encode(pubParams.Payload), requestOptions).then((msg) => {

                            const decodedResponse = context.sc.decode(msg.data)
                            const parsedResponse = utils.tryToParse(decodedResponse)
                            const endedAt = process.hrtime(startedAt);
                            let delta = (endedAt[0] * 1e9) + endedAt[1];
                            const code = 0
                            ee.emit('response', delta, code, context._uid);

                            const response = {
                                body: parsedResponse.body,
                                statusCode: 200,
                                headers: {
                                    contentType: parsedResponse.contentType
                                },
                            };

                            console.log("Response: " + JSON.stringify(response))

                            helpers.captureOrMatch(
                                params,
                                response,
                                context,
                                function captured(err, result) {
                                    if (result && result.captures) {
                                        // TODO handle matches
                                        let haveFailedCaptures = _.some(result.captures, function (v, k) {
                                            return v === '';
                                        });

                                        if (!haveFailedCaptures) {
                                            _.each(result.captures, function (v, k) {
                                                _.set(context.vars, k, v);
                                            });
                                        }
                                    }

                                    const afterResponseFunctionNames = _.concat(opts.afterResponse || [], rs.req.afterResponse || []);

                                    utils.processAfterResponseFunctions(
                                        self.script,
                                        afterResponseFunctionNames,
                                        params,
                                        response,
                                        context,
                                        ee,
                                        function done(err) {
                                            if (err) {
                                                debug(err);
                                                return callback(err, context);
                                            }

                                            return callback(null, context);
                                        }
                                    );
                                }
                            );

                        }).catch((err) => {
                            console.log("Flush Error: " + err)
                            debug(err);
                            ee.emit('error', err);
                            return callback(err, context);
                        })
                    } catch (err) {
                        console.log("Request Error: " + err)
                        debug(err);
                        ee.emit('error', err);
                        return callback(err, context);
                    }
                }
            )
        };
    }


    return function (context, callback) {
        return callback(null, context);
    };
};

NatsEngine.prototype.compile = function compile(tasks, scenarioSpec, ee) {
    const self = this;
    return function scenario(initialContext, callback) {
        const init = function init(next) {
            console.log("Connecting")

            let opts = {
                server: self.script.config.natspre.server || 'demo.nats.io:4222'
            };

            if (self.script.config.natspre.subject) {
                opts.subject = self.script.config.natspre.subject;
            }

            nats.connect({servers: opts.server}).then((nc) => {
                console.log("Connected")

                initialContext.nc = nc
                initialContext.sc = nats.StringCodec();

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
};

NatsEngine.prototype.$increment = function $increment(value) {
    return Number.isInteger(value) ? value += 1 : NaN;
};

NatsEngine.prototype.$decrement = function $decrement(value) {
    return Number.isInteger(value) ? value -= 1 : NaN;
};

console.log("123")
module.exports = NatsEngine;
