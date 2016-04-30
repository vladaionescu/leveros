![Lever OS](doc/images/leveros-logo-full-white-bg-v0.2.png "Lever OS")
======================================================================

**Serverless + Microservices = ♥**

*The cloud platform that allows fast-moving teams to build and deploy microservice-oriented backends in the blink of an eye*

Lever OS is in **beta**. Please report bugs via [GitHub issues](https://github.com/leveros/leveros/issues)!

Documentation
-------------

For complete documentation and API reference, please visit [Lever OS on ReadMe.io](https://leveros.readme.io).

* [CLI reference](https://leveros.readme.io/docs/cli)
* [lever.json reference](https://leveros.readme.io/docs/lever-json)
* [HTTP API](https://leveros.readme.io/docs/http-api)
* [Node Client API](https://leveros.readme.io/docs/node-client-api)
* [Node Server API](https://leveros.readme.io/docs/node-server-api)
* [Go API](https://godoc.org/github.com/leveros/leveros)

Getting started
---------------

### Prerequisites

* [Docker](https://docs.docker.com/engine/installation/) and [Docker Compose](https://docs.docker.com/compose/install/). On a Mac you can install [Docker Toolbox](https://docs.docker.com/toolbox/overview/) to get what you need.
* Make
* Linux or Mac (Windows should work too but it was never tested)

If you need to use docker-machine to run docker (eg on a Mac), you also need to install [VirtualBox](https://www.virtualbox.org/wiki/Downloads) and then run these commands to [get started](https://docs.docker.com/machine/get-started/):

```
$ docker-machine create --driver virtualbox default
$ eval `docker-machine env default`
```

You will need to run the second command for every new terminal window.

### Installation

```bash
$ git clone https://github.com/leveros/leveros
$ cd leveros
$ make cli pull-docker-images
$ sudo make install-cli
$ make fastrun
```

The commands above pull the necessary Docker images, install the `lever` CLI and run a Lever OS instance locally.

### Your first Lever service

```bash
$ mkdir hello
$ cd hello
```

###### server.js

```javascript
module.exports.sayHello = function (name, callback) {
    callback(null, "Hello, " + name + "!");
};
```

###### lever.json

```json
{
    "name": "helloService",
    "description": "A hello service.",
    "jsEntry": "server.js"
}
```

Deploy your service locally

```bash
$ lever deploy
```

This takes the whole current directory, archives it and deploys it onto Lever, in an environment that was created by default: `dev.lever`.

###### Invoke via CLI

```bash
$ lever invoke lever://dev.lever/helloService/sayHello '"world"'
"Hello, world!"

# Or even shorter (only for dev.lever)...
$ lever invoke /helloService/sayHello '"world"'
"Hello, world!"
```

Remember to use proper JSON for arguments. This includes the quotes for strings.

###### Invoke via HTTP POST request

```bash
# Without docker-machine
$ curl -H "Content-Type: application/json" -X POST -d '["world"]' \
http://127.0.0.1:8080/helloService/sayHello?forceenv=dev.lever
"Hello, world!"

# With docher-machine
$ curl -H "Content-Type: application/json" -X POST -d '["world"]' \
http://$(docker-machine ip default):8080/helloService/sayHello?forceenv=dev.lever
"Hello, world!"
```

Notice the `forceenv` query param at the end of the command. This is a convenience feature that allows you to access Lever's HTTP API without having `dev.lever` assigned to the listening IP (and also configuring Lever to serve on port `80`).

###### Invoke from browser (via HTTP API)

```javascript
$.ajax({
    'type': 'POST',
    'url': 'http://127.0.0.1:8080/helloService/sayHello?forceenv=dev.lever',
    'contentType': 'application/json',
    'data': JSON.stringify(["world"]),
    'dataType': 'json',
    'success': function (data) {
        console.log(data);  // Hello, world!
    },
});
```

Note that when using docker-machine, you need to replace `127.0.0.1` with the output of the command `docker-machine ip default`.

###### Invoke from Node

```bash
$ npm install leveros
```

```javascript
var leveros = require('leveros');

var client = new leveros.Client();
client.forceHost = process.env.LEVEROS_IP_PORT;
var service = client.service('dev.lever', 'helloService');
service.invoke('sayHello', "world", function (error, reply) {
    console.log(reply);  // Hello, world!
});
```

```bash
# Without docker-machine
$ LEVEROS_IP_PORT="127.0.0.1:8080" node client.js

# With docher-machine
$ LEVEROS_IP_PORT="$(docker-machine ip default):8080" node client.js
```

Setting `LEVEROS_IP_PORT` is necessary so that you can invoke the `dev.lever` environment without adding an entry for it in `/etc/hosts` and setting the listen port to `80`.

### What's next?

To learn more about using Lever OS, check the [full documentation](https://leveros.readme.io/).

Contributing
------------

* Please report bugs as [GitHub issues](https://github.com/leveros/leveros/issues).
* Join us on [Gitter](https://gitter.im/leveros/leveros)!
* Questions via GitHub issues are welcome!
* PRs welcome! But please give a heads-up in GitHub issue before starting work. If there is no GitHub issue for what you want to do, please create one.
* To build from source, check the [contributing](./doc/contributing.md) page.

Security Disclosure
-------------------

Security is very important to us. If you have any issue regarding security, please disclose the information responsibly by sending an email to `security [at] leveros.com` and not by creating a GitHub issue.

Licensing
---------

Lever OS is licensed under the Apache License, Version 2.0. See [LICENSE.md](./LICENSE.md) for the full license text.
