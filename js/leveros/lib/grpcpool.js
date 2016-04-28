
import * as common from 'leveros-common';
import grpc from 'grpc';
import lodash from 'lodash';
import SortedMap from 'collections/sorted-map';

export class GRPCPool {
    constructor() {
        const expiryMs = 5 * 60 * 1000;  // 5 minutes in ms.
        this._conns = new Cache(
            expiryMs, this._newConn.bind(this), this._destroyConn.bind(this));
    }

    dial(target, callback) {
        this._conns.get(target, callback);
    }

    keepAlive(target) {
        this._conns.keepAlive(target);
    }

    _newConn(target, callback) {
        callback(
            null,
            new common.leverRPCProto.core.LeverRPC(
                target, grpc.credentials.createInsecure()));
    }

    _destroyConn(conn) {
        conn.$channel.close();
    }
}

class CacheEntry {
    constructor() {
        this.element = null;
        this.error = new Error("Not yet constructed");
        this.lastUsed = null;
        this.keepAlive();
    }

    keepAlive() {
        this.lastUsed = Date.now();
    }
}

class Cache {
    constructor(expiryMs, constr, destr) {
        this._expiryMs = expiryMs;
        this._constr = constr;
        this._destr = destr;
        this._data = {};  // key -> CacheEntry
        this._lastUsedMap = new SortedMap();  // lastUsed (ms) -> array of keys
    }

    get(key, callback) {
        if (this._data.hasOwnProperty(key)) {
            this.keepAlive(key);
            const entry = this._data[key];
            setImmediate(callback.bind(null, entry.error, entry.element));
            return;
        }

        const entry = new CacheEntry();
        sortedMapInsert(this._lastUsedMap, entry.lastUsed, key);
        this._data[key] = entry;
        this._maybeScheduleExpire(entry.lastUsed);

        this._constr(key, (error, element) => {
            entry.error = error;
            entry.element = element;

            if (error) {
                // Remove from cache if construction failed.
                delete this._data[key];
                sortedMapRemove(this._lastUsedMap, entry.lastUsed, key);
            }

            callback(error, element);
        });
    }

    _destroyEntry(entry) {
        if (entry.element !== null) {
            try {
                this._destr(entry.element);
            } catch (ex) {
                // Do nothing.
            }
            entry.element = null;
            entry.error = new Error("Was destructed");
        }
    }

    keepAlive(key) {
        if (!this._data.hasOwnProperty(key)) {
            return false;
        }
        const entry = this._data[key];
        sortedMapRemove(this._lastUsedMap, entry.lastUsed, key);
        entry.keepAlive();
        sortedMapInsert(this._lastUsedMap, entry.lastUsed, key);
        this._maybeScheduleExpire(entry.lastUsed);
        return true;
    }

    _maybeScheduleExpire(lastUsed) {
        if (this._lastUsedMap.store.min()[0] !== lastUsed) {
            // Already scheduled.
            return;
        }
        const value = this._lastUsedMap.get(lastUsed, []);
        if (value.length > 1) {
            // Already scheduled.
            return;
        }
        this._doExpire();
    }

    _doExpire() {
        while (this._lastUsedMap.length !== 0) {
            const lastUsed = this._lastUsedMap.store.min()[0];
            const expiryTime = lastUsed + this._expiryMs;
            if (expiryTime <= Date.now()) {
                // Entry expired.
                const value = this._lastUsedMap.get(lastUsed, []);
                lodash.forEach(value, (key) => {
                    const entry = this._data[key];
                    delete this._data[key];
                    this._destroyEntry(entry);
                });
                this._lastUsedMap.delete(lastUsed);
            } else {
                // Not yet time. Schedule next.
                const untilExpiry = expiryTime - Date.now();
                setTimeout(this._doExpire.bind(this), untilExpiry);
                return;
            }
        }
    }
}

function sortedMapInsert(map, key, target) {
    if (!map.has(key)) {
        map.set(key, [target]);
        return;
    }
    const value = map.get(key);
    value.push(target);
    map.set(key, value);
}

function sortedMapRemove(map, key, target) {
    if (!map.has(key)) {
        return;
    }
    const value = map.get(key);
    const foundIndex = lodash.indexOf(value, target);
    if (foundIndex === -1) {
        return;
    }
    value.splice(foundIndex, 1);
    if (value.length === 0) {
        map.delete(key);
    } else {
        map.set(key, value);
    }
}
