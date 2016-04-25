
import 'source-map-support/register';

import grpc from 'grpc';
import path from 'path';
import lodash from 'lodash';
import common from 'leveros-common';

class Handler {
    constructor(importPath) {
        /* eslint global-require: "off" */
        this._custHandler = require(path.join(process.cwd(), importPath));
    }

    handleRpc(call, callback) {
        // TODO RPC gateway
        //call.metadata.get('x-lever-internal-rpc-gateway');
        const rpc = call.request;
        if (lodash.startsWith(rpc.method, '_') ||
            !this._custHandler.hasOwnProperty(rpc.method) ||
            !lodash.isFunction(this._custHandler[rpc.method])) {
            callback(new Error("Invalid method"));
            return;
        }
        const method = this._custHandler[rpc.method];
        const onCallback = (error, result) => {
            callback(null, common.jsToReply(error, result));
        };
        const args = common.argsToJs(rpc);
        method.apply(this._custHandler, lodash.concat(args, [onCallback]));
    }

    handleStreamingRpc(call, callback) {
        // TODO ...
    }
}

function main() {
    if (process.argv.length < 3) {
        throw new Error("No entry point specified");
    }
    const handler = new Handler(process.argv[2]);
    const server = new grpc.Server();
    server.addProtoService(common.leverRPCProto.core.LeverRPC.service, handler);
    // TODO ...
    const resourceName = '';
    const prefix = `/${common.ownService}/${resourceName}/`;
    server.handlers[`${prefix}HandleRPC`] = (
        server.handlers['/core.LeverRPC/HandleRPC']);
    server.handlers[`${prefix}HandleStreamingRPC`] = (
        server.handlers['/core.LeverRPC/HandleStreamingRPC']);
    server.bind("0.0.0.0:3837", grpc.ServerCredentials.createInsecure());
    server.start();
}

main();
