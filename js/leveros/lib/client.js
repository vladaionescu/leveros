
import common from 'leveros-common';
import grpcpool from './grpcpool';

export class Endpoint {
    constructor(env, service, resource, client) {
        this._env = env;
        this._service = service;
        this._resource = resource;
        this._client = client;
    }

    invoke(method, ...argsAndCallback) {
        const invokeArgs = lodash.concat(
            [this._env, this._service, this._resource, method],
            argsAndCallback);
        this._client._invoke.apply(this._client, invokeArgs);
    }

    invokeChan(method, ...argsAndCallback) {
        const invokeChanArgs = lodash.concat(
            [this._env, this._service, this._resource, method],
            argsAndCallback);
        this._client._invokeChan.apply(this._client, invokeChanArgs);
    }
}

export class Client {
    constructor() {
        this._connections = new grpcpool.GRPCPool();
        this._forceHost = "";
    }

    endpointFromURL(leverURL) {
        // TODO
    }

    service(env, service) {
        return new Endpoint(env, service, "", this);
    }

    resource(env, service, resource) {
        return new Endpoint(env, service, resource, this);
    }

    _invoke(env, service, resource, method, ...argsAndCallback) {
        if (argsAndCallback.length === 0 ||
                !lodash.isFunction(argsAndCallback[argsAndCallback.length-1])) {
            throw new Error("Callback not provided");
        }
        const args = argsAndCallback.slice(0, argsAndCallback.length-1);
        const callback = argsAndCallback[argsAndCallback.length-1];

        if isChanMethod(method) {
            callback(new Error("Use invokeChan for streaming methods"));
            return;
        }
        if (!env && !common.ownEnvironment) {
            callback(
                new Error("Environment not specified and cannot be deduced"));
            return;
        }
        if (!env) {
            env = common.ownEnvironment;
        }

        const dialTo = this._client._forceHost ? this._client._forceHost : env;
        this._connections.dial(dialTo, (error, connection) => {
            if (error) {
                callback(error);
                return;
            }

            const rpc = argsconv.jsToRpc(method, args);
            sendLeverRPC(
                connection, env, service, resource, rpc, (error, reply) => {
                    if (error) {
                        callback(error);
                        return;
                    }
                    callback.apply(null, argsconv.replyToJs(reply));
                });
        });
    }
}

function sendLeverRPC(connection, env, service, resource, rpc, callback) {
    const path = `/${service}/${resource}/HandleRPC`;
    connection.invoke(path, rpc, callback);
}

function sendStreamingLeverRPC(
    connection, env, service, resource, rpc, callback) {
    const path = `/${service}/${resource}/HandleStreamingRPC`;
    // TODO ...
}
