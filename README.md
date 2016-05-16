![Lever OS](doc/images/leveros-logo-full-white-bg-v0.2.png "Lever OS")
======================================================================

**Serverless + Microservices = ♥**

Lever OS is in **beta**. Please report bugs via [GitHub issues](https://github.com/leveros/leveros/issues)!

[![ReadMe.io](https://img.shields.io/badge/ReadMe.io-docs-blue.svg)](https://leveros.readme.io/) [![Build Status](https://travis-ci.org/leveros/leveros.svg?branch=master)](https://travis-ci.org/leveros/leveros) [![Go Report Card](https://goreportcard.com/badge/github.com/leveros/leveros)](https://goreportcard.com/report/github.com/leveros/leveros) [![Join the chat at https://gitter.im/leveros/leveros](https://badges.gitter.im/leveros/leveros.svg)](https://gitter.im/leveros/leveros?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0) [![Analytics](https://ga-beacon.appspot.com/UA-77293003-2/github.com/leveros/leveros?pixel)](https://github.com/igrigorik/ga-beacon)

Lever OS is the open-source cloud platform that allows fast-moving teams to build and deploy microservice-oriented backends in the blink of an eye. It abstracts away complicated infrastructure and leaves developers with very simple, but powerful building blocks that handle scale transparently.

How does it work?
-----------------

With Lever OS, you don't think about servers. You think about services. Lever takes care of distributing your code on multiple servers and bringing up as many instances as necessary, depending on real-time demand. It routes and load-balances traffic transparently so you don't need to configure complicated reverse proxies or service discovery. It's all built-in.

The [services](https://leveros.readme.io/docs/services) are made out of a few exported functions that you develop and then deploy onto Lever. In JavaScript, for example, you export the functions as part of a `.js` file and then point Lever to that file. The functions become the API of your service and you can trigger them using an HTTP API, a Lever client library (Node and Go supported for now), or even the `lever` command-line tool.

Documentation
-------------

For complete documentation and API reference, see [Lever OS on ReadMe.io](https://leveros.readme.io).

* [Concepts](https://leveros.readme.io/docs/basic-concepts)
* [CLI reference](https://leveros.readme.io/docs/cli)
* [lever.json reference](https://leveros.readme.io/docs/lever-json)
* [HTTP API](https://leveros.readme.io/docs/http-api)
* [Node Client API](https://leveros.readme.io/docs/node-client-api)
* [Node Server API](https://leveros.readme.io/docs/node-server-api)
* [Go API](https://godoc.org/github.com/leveros/leveros/api)

Getting started
---------------

### Prerequisites

* [Docker](https://docs.docker.com/engine/installation/) 1.11+ and [Docker Compose](https://docs.docker.com/compose/install/) 1.7+. On a Mac you can install [Docker Toolbox](https://docs.docker.com/toolbox/overview/) to get what you need.
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
$ make HAVE_GO="" cli pull-docker-images
$ sudo make HAVE_GO="" install-cli
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

# With docker-machine
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

# With docker-machine
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
