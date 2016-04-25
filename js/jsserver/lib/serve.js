
import 'source-map-support/register'

import * as grpc from 'grpc';
import * as path from 'path';
import * as util from 'util';
import * as lodash from 'lodash';
import * as argsconv from './argsconv';

const protoDesc = grpc.load('/leveros/leverrpc.proto');

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
            callback(null, argsconv.jsToReply(error, result));
        };
        const args = argsconv.argsToJs(rpc);
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
    server.addProtoService(protoDesc.core.LeverRPC.service, handler);
    // TODO ...
    const serviceName = 'test';
    const resourceName = '';
    const prefix = '/' + serviceName + '/' + resourceName + '/';
    server.handlers[prefix + 'HandleRPC'] = (
        server.handlers['/core.LeverRPC/HandleRPC']);
    server.handlers[prefix + 'HandleStreamingRPC'] = (
        server.handlers['/core.LeverRPC/HandleStreamingRPC']);
    server.bind("0.0.0.0:3837", grpc.ServerCredentials.createInsecure());
    server.start();
}

main();
