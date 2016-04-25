
import * as grpc from 'grpc';
import * as util from 'util';
import * as lodash from 'lodash';

const core = grpc.load('/leveros/leverrpc.proto').core;

export function argsToJs(rpc) {
    console.log("rpc: " + util.inspect(rpc));
    if (rpc.args_oneof === 'args') {
        let ret = [];
        lodash.forEach(rpc.args.element, (arg) => {
            ret.push(argToJs(arg));
        });
        return ret;
    } else if (rpc.args_oneof === 'byte_args') {
        return [rpc.byte_args];
    } else {
        throw new Error("Invalid args_oneof");
    }
}

export function argToJs(element) {
    switch (element.json_value_oneof) {
    case 'json_string':
    case 'json_number':
    case 'json_bool':
        return element[element.json_value_oneof]
    case 'json_array':
        let array = [];
        lodash.forEach(element.json_array.element, (arg) => {
            array.push(argToJs(arg));
        });
        return array;
    case 'json_object':
        let obj = {};
        lodash.forEach(element.json_object.property, (entry) => {
            obj[entry.name] = argToJs(entry.value);
        });
        return obj;
    case null:
        return null;
    default:
        throw new Error("Invalid json_value_oneof");
    }
}

export function jsToReply(error, value) {
    let reply = {};
    if (error) {
        if (lodash.isBuffer(error)) {
            reply.byte_error = error;
        } else {
            reply.error = jsToArg(error);
        }
        return reply;
    }

    if (lodash.isBuffer(value)) {
        reply.byte_result = value;
    } else {
        reply.result = jsToArg(value);
    }
    return reply;
}

export function jsToArg(value) {
    let arg = {};
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
        lodash.forEach(value, (value, propertyName) => {
            arg.json_object.property.push({
                name: propertyName,
                value: jsToArg(value),
            });
        });
    } else {
        throw new Error("Invalid JSON value");
    }
    return arg;
}
