![Lever OS](../doc/images/leveros-logo-full-white-bg-v0.2.png "Lever OS")
=========================================================================

Go library for Lever OS
=======================

The Go library can be used for both implementing Lever services in Go and invoking Lever methods, as a client.

[![GoDoc](https://godoc.org/github.com/leveros/leveros/api?status.svg)](https://godoc.org/github.com/leveros/leveros/api) [![ReadMe.io](https://img.shields.io/badge/ReadMe.io-docs-blue.svg)](https://leveros.readme.io/) [![Build Status](https://travis-ci.org/leveros/leveros.svg?branch=master)](https://travis-ci.org/leveros/leveros) [![Join the chat at https://gitter.im/leveros/leveros](https://badges.gitter.im/leveros/leveros.svg)](https://gitter.im/leveros/leveros?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0) [![Analytics](https://ga-beacon.appspot.com/UA-77293003-2/github.com/leveros/leveros/api?pixel)](https://github.com/igrigorik/ga-beacon)

Documentation
-------------

See [Godoc](https://godoc.org/github.com/leveros/leveros/api).

Installation
------------

```bash
go get -u github.com/leveros/leveros/api
```

```go
import leverapi "github.com/leveros/leveros/api"
```

Quick example
-------------

### Server

```bash
$ mkdir hello
$ cd hello
```

###### server.go

```go
package main

import (
	"fmt"
	"log"

	leverapi "github.com/leveros/leveros/api"
)

func main() {
	server, err := leverapi.NewServer()
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	err = server.RegisterHandlerObject(new(Handler))
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	server.Serve()
}

// Handler is the service handler. Its exported methods become the service's
// methods.
type Handler struct {
}

// SayHello returns a salutation for the given name.
func (*Handler) SayHello(name string) (result string, err error) {
	return fmt.Sprintf("Hello, %s!", name), nil
}
```

###### lever.json

```json
{
    "name": "helloService",
    "description": "A hello service.",
    "entry": ["./serve"]
}
```

Compile and deploy

```bash
$ GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ./serve server.go
$ lever deploy
```

Note the env vars for compiling for Lever OS. These need to be `GOOS=linux GOARCH=amd64 CGO_ENABLED=0` even when running on Mac or Windows.

If you have problems building, you may need to reinstall Go to include cross-compilation support. On a Mac, you can achieve this with `brew install go --with-cc-common`.

To try it out, invoke it with the CLI:

```bash
$ lever invoke /helloService/SayHello '"world"'

"Hello, world!"
```

### Client

###### client.go

```go
package main

import (
	"log"
	"os"

	leverapi "github.com/leveros/leveros/api"
)

func main() {
    client, err := leverapi.NewClient()
    if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
	client.ForceHost = os.Getenv("LEVEROS_IP_PORT")
    leverService := client.Service("dev.lever", "helloService")
    var reply string
    err = leverService.Invoke(&reply, "SayHello", "world")
    if err != nil {
        log.Printf("Error: %v\n", err)
    }
    log.Printf("%s\n", reply) // Hello, world!
}
```

Run

```bash
# Without docker-machine
$ LEVEROS_IP_PORT="127.0.0.1:8080" go run client.go

# With docher-machine
$ LEVEROS_IP_PORT="$(docker-machine ip default):8080" go run client.go
```

Setting `LEVEROS_IP_PORT` is necessary so that you can invoke the `dev.lever` environment without adding an entry for it in `/etc/hosts` and setting the listen port to `80`.
