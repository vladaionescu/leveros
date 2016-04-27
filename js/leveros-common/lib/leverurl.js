
import url from 'url';
import lodash from 'lodash';

export class LeverURL {
    constructor(env="", service="", resource="", method="") {
        this.environment = env;
        this.service = service;
        this.resource = resource;
        this.method = method;
    }

    toString() {
        if (this.environment === "" || this.environment === null) {
            return `/${this.service}/${this.resource}/${this.method}`;
        }
        return (
            `lever://${this.environment}/${this.service}/` +
            `${this.resource}/${this.method}`);
    }
}

export function parseLeverURL(str) {
    const parsed = url.parse(str);
    if (parsed.protocol !== "lever:" && parsed.protocol !== null) {
        throw new Error(`Invalid Lever URL ${str}`);
    }
    let path = parsed.pathname;
    if (path !== null && path !== "" && path[0] === '/') {
        path = path.slice(1);
    }
    const firstSep = lodash.indexOf(path, '/');
    const lastSep = lodash.lastIndexOf(path, '/');
    const leverURL = new LeverURL(parsed.hostname);
    if (firstSep === -1) {
        throw new Error("Invalid Lever URL");
    }
    leverURL.service = path.slice(0, firstSep);
    if (firstSep !== lastSep) {
        leverURL.resource = path.slice(firstSep+1, lastSep);
    }
    leverURL.method = path.slice(lastSep+1);
    return leverURL;
}
