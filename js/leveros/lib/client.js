
import * as common from 'leveros-common';
import grpc from 'grpc';
import grpcpool from './grpcpool';
import lodash from 'lodash';

class Endpoint {
    constructor(env, service, resource, client) {
        this._leverURL = new common.LeverURL(env, service, resource);
        this._client = client;
    }

    invoke(method, ...argsAndCallback) {
        const leverURL = new common.LeverURL(
            this._leverURL.environment,
            this._leverURL.service,
            this._leverURL.resource,
            method);
        const invokeArgs = lodash.concat([leverURL], argsAndCallback);
        this._client._invoke.apply(this._client, invokeArgs);
    }

    invokeChan(method, ...argsAndCallback) {
        const leverURL = new common.LeverURL(
            this._leverURL.environment,
            this._leverURL.service,
            this._leverURL.resource,
            method);
        const invokeChanArgs = lodash.concat([leverURL], argsAndCallback);
        this._client._invokeChan.apply(this._client, invokeChanArgs);
    }
}

class Client {
    constructor() {
        this._connections = new grpcpool.GRPCPool();
        this.forceHost = "";
    }

    service(env, service) {
        return new Endpoint(env, service, "", this);
    }

    resource(env, service, resource) {
        return new Endpoint(env, service, resource, this);
    }

    invokeURL(leverURLStr, ...argsAndCallback) {
        const leverURL = common.parseLeverURL(leverURLStr);
        const invokeArgs = lodash.concat([leverURL], argsAndCallback);
        this._invoke.apply(this, invokeArgs);
    }

    invokeChanURL(leverURLStr, ...argsAndCallback) {
        const leverURL = common.parseLeverURL(leverURLStr);
        const invokeChanArgs = lodash.concat([leverURL], argsAndCallback);
        this._invokeChan.apply(this, invokeChanArgs);
    }

    _invoke(leverURL, ...argsAndCallback) {
        if (argsAndCallback.length === 0 ||
                !lodash.isFunction(argsAndCallback[argsAndCallback.length-1])) {
            throw new Error("Callback not provided");
        }
        const args = argsAndCallback.slice(0, argsAndCallback.length-1);
        const callback = argsAndCallback[argsAndCallback.length-1];

        if (isChanMethod(leverURL.method)) {
            callback(new Error("Use invokeChan for streaming methods"));
            return;
        }
        if (!leverURL.environment && !common.ownEnvironment) {
            callback(
                new Error("Environment not specified and cannot be deduced"));
            return;
        }
        if (!leverURL.environment) {
            leverURL.environment = common.ownEnvironment;
        }

        let dialTo;
        if (this.forceHost) {
            dialTo = this.forceHost;
        } else {
            if ((isInternalEnvironment(leverURL.environment) ||
                leverURL.environment === common.ownEnvironment) &&
                global.leverInternalRPCGateway) {
                dialTo = global.leverInternalRPCGateway;
            } else {
                dialTo = leverURL.environment;
            }
        }
        this._connections.dial(dialTo, (error, connection) => {
            if (error) {
                callback(error);
                return;
            }

            const rpc = common.jsToRpc(args);
            sendLeverRPC(
                connection, leverURL, rpc, (sendError, reply) => {
                    if (sendError) {
                        callback(sendError);
                        return;
                    }
                    callback.apply(null, common.replyToJs(reply));
                });
        });
    }

    _invokeChan(leverURL, ...argsAndCallback) {
        if (argsAndCallback.length === 0 ||
                !lodash.isFunction(argsAndCallback[argsAndCallback.length-1])) {
            throw new Error("Callback not provided");
        }
        const args = argsAndCallback.slice(0, argsAndCallback.length-1);
        const callback = argsAndCallback[argsAndCallback.length-1];

        if (!isChanMethod(leverURL.method)) {
            callback(new Error("Use invoke for non-streaming methods"));
            return;
        }
        if (!leverURL.environment && !common.ownEnvironment) {
            callback(
                new Error("Environment not specified and cannot be deduced"));
            return;
        }
        if (!leverURL.environment) {
            leverURL.environment = common.ownEnvironment;
        }

        let dialTo;
        if (this.forceHost) {
            dialTo = this.forceHost;
        } else {
            if ((isInternalEnvironment(leverURL.environment) ||
                leverURL.environment === common.ownEnvironment) &&
                global.leverInternalRPCGateway) {
                dialTo = global.leverInternalRPCGateway;
            } else {
                dialTo = leverURL.environment;
            }
        }
        this._connections.dial(dialTo, (error, connection) => {
            if (error) {
                callback(error);
                return;
            }

            const call = sendStreamingLeverRPC(connection, leverURL);
            call.write({rpc: common.jsToRpc(args)});
            callback(null, new common.Stream(call));
        });
    }
}

function isChanMethod(name) {
    return (
        lodash.endsWith(name, "Chan") ||
        lodash.endsWith(name, "_chan") ||
        name === "chan");
}

function isInternalEnvironment(env) {
    if (!common.internalEnvSuffix) {
        return false;
    }
    return lodash.endsWith(env, common.internalEnvSuffix);
}

function sendLeverRPC(connection, leverURL, rpc, callback) {
    const metadata = new grpc.Metadata();
    metadata.set('lever-url', leverURL.toString());
    /* eslint new-cap: "off" */
    connection.HandleRPC(rpc, callback, metadata);
}

function sendStreamingLeverRPC(connection, leverURL) {
    const metadata = new grpc.Metadata();
    metadata.set('lever-url', leverURL.toString());
    /* eslint new-cap: "off" */
    return connection.HandleStreamingRPC(metadata);
}

export default new Client();
