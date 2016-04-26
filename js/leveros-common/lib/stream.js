
import { EventEmitter } from 'events';
import lodash from 'lodash';
import {jsToArg, argToJs} from './leveros-common';


export class Stream extends EventEmitter {
    constructor(grpcStream) {
        super();
        this._grpcStream = grpcStream;
        this._grpcStream.on('data', this._onData.bind(this));
        this._grpcStream.on('end', this._onEnd.bind(this));
    }

    write(msg) {
        const streamMsg = {};
        if (lodash.isBuffer(msg)) {
            streamMsg.byte_message = msg;
        } else {
            streamMsg.message = jsToArg(msg);
        }
        this._grpcStream.write(streamMsg);
    }

    end() {
        this._grpcStream.end();
    }

    _onData(streamMsg) {
        switch (streamMsg.message_oneof) {
        case 'message':
            this.emit('data', argToJs(streamMsg.message));
            return;
        case 'byte_message':
            this.emit('data', streamMsg.message);
            return;
        case 'error':
            this.emit('error', new Error(argToJs(streamMsg.error)));
            return;
        case 'byte_error':
            this.emit('error', new Error(streamMsg.byte_error));
            return;
        default:
            throw new Error("Invalid message_oneof");
        }
    }

    _onEnd() {
        this.emit('end');
    }
}
