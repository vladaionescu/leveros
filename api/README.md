![Lever OS](../doc/images/leveros-logo-full-white-bg-v0.2.png "Lever OS")
=========================================================================

API libraries for Go
====================

Installation
------------

```bash
go get -u github.com/leveros/leveros/api
```

```go
import leverapi "github.com/leveros/leveros/api"
```

Reference
---------

See [Godoc](https://godoc.org/github.com/leveros/leveros/api).

Quick example
-------------

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

type Handler struct {
}

func (*Handler) SayHello(name string) (result string, err error) {
	return fmt.Sprintf("Hello, %s!", name), nil
}
```

###### lever.json

```json
{
    "name": "helloService",
    "description": "A hello service.",
    "entry": "serve"
}
```

Compile and deploy

```bash
$ GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ./serve server.go
$ lever deploy dev.lever
```

Note the env vars for compiling for Lever OS. These need to be
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0` even when running on Mac or Windows.

If you have problems building, you may need to reinstall go to include
cross-compilation support. On a Mac, you can achieve this with
`brew install go --with-cc-common`.

###### client.go

```go
package main

import (
	"log"

	leverapi "github.com/leveros/leveros/api"
)

func main() {
    client, err := leverapi.NewClient()
    if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
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
