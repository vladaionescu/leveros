
import 'source-map-support/register';

import grpc from 'grpc';
import path from 'path';
import lodash from 'lodash';
import * as common from 'leveros-common';

class Handler {
    constructor(importPath) {
        /* eslint global-require: "off" */
        this._custHandler = require(path.join(process.cwd(), importPath));
        this._resources = {};  // string -> true
    }

    handleRpc(call, callback) {
        setInternalRPCGateway(call.metadata);
        let leverURL;
        try {
            leverURL = common.parseLeverURL(call.metadata.get('lever-url')[0]);
        } catch (ex) {
            callback(ex);
            return;
        }
        const rpc = call.request;
        if (lodash.startsWith(leverURL.method, '_') ||
            !this._custHandler.hasOwnProperty(leverURL.method) ||
            !lodash.isFunction(this._custHandler[leverURL.method])) {
            callback(new Error("Invalid method"));
            return;
        }
        const method = this._custHandler[leverURL.method];
        const onCallback = (error, result) => {
            callback(null, common.jsToReply(error, result));
        };
        const args = [];
        if (leverURL.resource !== "") {
            args.push(leverURL.resource);
        }
        Array.prototype.push.apply(args, common.rpcToJs(rpc));
        args.push(onCallback);
        const exit = this._maybeHandleResourceLifecycle(
            leverURL, args, onCallback);
        if (exit) {
            return;
        }
        method.apply(this._custHandler, args);
    }

    handleStreamingRpc(call) {
        setInternalRPCGateway(call.metadata);
        let leverURL;
        try {
            leverURL = common.parseLeverURL(call.metadata.get('lever-url')[0]);
        } catch (ex) {
            call.write({error: ex.message});
            call.end();
            return;
        }
        const onError = () => {
            call.end();
        };
        call.on('error', onError);
        call.once('data', (streamMsg) => {
            call.removeListener('error', onError);
            if (streamMsg.message_oneof !== 'rpc') {
                call.write({error: "First message was not rpc"});
                call.end();
            }
            const rpc = streamMsg.rpc;
            if (lodash.startsWith(leverURL.method, '_') ||
                !this._custHandler.hasOwnProperty(leverURL.method) ||
                !lodash.isFunction(this._custHandler[leverURL.method])) {
                call.write({error: "Invalid method"});
                call.end();
                return;
            }
            const method = this._custHandler[leverURL.method];

            // First message sent back must be empty.
            call.write({});

            const args = [new common.Stream(call)];
            if (leverURL.resource !== "") {
                args.push(leverURL.resource);
            }
            Array.prototype.push.apply(args, common.rpcToJs(rpc));
            method.apply(this._custHandler, args);
        });
    }

    _maybeHandleResourceLifecycle(leverURL, args, callback) {
        if (leverURL.resource !== "") {
            return false;
        }
        if (leverURL.method === "NewResource") {
            const resource = args[0];
            if (this._resources.hasOwnProperty(resource)) {
                callback(new Error("Resource already exists"));
                return true;
            }
            this._resources[resource] = true;
        } else if (leverURL.method === "CloseResource") {
            const resource = args[0];
            if (!this._resources.hasOwnProperty(resource)) {
                callback(new Error("Resource does not exist"));
                return true;
            }
            delete this._resources[resource];
        }
        return false;
    }
}

function setInternalRPCGateway(metadata) {
    const headers = metadata.get('x-lever-internal-rpc-gateway');
    if (!headers || headers.length === 0 || !headers[0]) {
        return;
    }
    global.leverInternalRPCGateway = headers[0];
}

function main() {
    if (process.argv.length < 3) {
        throw new Error("No entry point specified");
    }
    const handler = new Handler(process.argv[2]);
    const server = new grpc.Server();
    server.addProtoService(common.leverRPCProto.core.LeverRPC.service, handler);
    server.bind("0.0.0.0:3837", grpc.ServerCredentials.createInsecure());
    server.start();
}

main();
