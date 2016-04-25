
import lodash from 'lodash';
import path from 'path';

export const ownEnvironment = process.env.LEVEROS_ENVIRONMENT;
export const ownService = process.env.LEVEROS_SERVICE;
export const ownInstanceID = process.env.LEVEROS_INSTANCE_ID;

export const leverRPCProto = grpc.load(
    path.join(__dirname, '..', '..', 'leverrpc.proto'));

export function argsToJs(rpc) {
    if (rpc.args_oneof === 'args') {
        const ret = [];
        lodash.forEach(rpc.args.element, (arg) => {
            ret.push(argToJs(arg));
        });
        return ret;
    }
    if (rpc.args_oneof === 'byte_args') {
        return [rpc.byte_args];
    }
    throw new Error("Invalid args_oneof");
}

export function argToJs(element) {
    let ret;
    switch (element.json_value_oneof) {
    case 'json_string':
    case 'json_number':
    case 'json_bool':
        return element[element.json_value_oneof];
    case 'json_array':
        ret = [];
        lodash.forEach(element.json_array.element, (arg) => {
            ret.push(argToJs(arg));
        });
        return ret;
    case 'json_object':
        ret = {};
        lodash.forEach(element.json_object.property, (entry) => {
            ret[entry.name] = argToJs(entry.value);
        });
        return ret;
    case null:
        return null;
    default:
        throw new Error("Invalid json_value_oneof");
    }
}

export function jsToReply(error, value) {
    const reply = {};
    if (error) {
        if (lodash.isError(error)) {
            if (error.message) {
                reply.error = jsToArg(error.message);
            } else {
                reply.error = jsToArg(error);
            }
        } else if (lodash.isBuffer(error)) {
            reply.byte_error = error;
        } else {
            reply.error = jsToArg(error);
        }
    } else {
        if (lodash.isBuffer(value)) {
            reply.byte_result = value;
        } else {
            reply.result = jsToArg(value);
        }
    }
    return reply;
}

export function jsToArg(value) {
    if (lodash.isDate(value)) {
        return jsToArg(JSON.stringify(value));
    }
    const arg = {};
    if (lodash.isString(value)) {
        arg.json_string = value;
    } else if (lodash.isNumber(value)) {
        arg.json_number = value;
    } else if (lodash.isBool(value)) {
        arg.json_bool = value;
    } else if (lodash.isNull(value) || lodash.isUndefined(value)) {
        // Set nothing.
    } else if (lodash.isArray(value)) {
        arg.json_array = {element: []};
        lodash.forEach(value, (element) => {
            arg.json_array.element.push(jsToArg(element));
        });
    } else if (lodash.isObject(value)) {
        arg.json_object = {property: []};
        lodash.forEach(value, (propertyValue, propertyName) => {
            arg.json_object.property.push({
                name: propertyName,
                value: jsToArg(propertyValue),
            });
        });
    } else {
        throw new Error("Invalid JSON value");
    }
    return arg;
}

export function jsToRpc(method, args) {
    const rpc = {
        method,
    };
    if (args.length === 1 && lodash.isBuffer(args[0])) {
        rpc.byte_args = args[0];
    } else {
        rpc.args = {element: []};
        lodash.forEach(args, (arg) => {
            rpc.args.element.push(jsToArg(arg));
        })
    }
    return rpc;
}

export function replyToJs(reply) {
    switch (reply.result_oneof) {
    case 'result':
        return [null, argToJs(reply.result)];
    case 'byte_result':
        return [null, reply.byte_result];
    case 'error':
        return [new Error(argToJs(reply.error)), null];
    case 'byte_error':
        return [new Error(reply.byte_error), null];
    default:
        throw new Error("Invalid result_oneof");
    }
}
