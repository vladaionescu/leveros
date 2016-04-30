
Contributing
============

Prerequisites
-------------

* Install make.
* Install golang and set GOROOT and GOPATH correctly.
* If not on linux/amd64, go needs to be able to cross-compile for linux/amd64. (On Mac you can achieve this by installing go with this command: `brew install go --with-cc-common`).
* Install protoc from https://github.com/google/protobuf/releases (eg https://github.com/google/protobuf/releases/download/v3.0.0-beta-2/protoc-3.0.0-beta-2-linux-x86_64.zip). Put protoc in your PATH somewhere.
* `go get -u github.com/golang/protobuf/protoc-gen-go`
* `go get -u github.com/tools/godep`
* In leveros dir: `godep restore ./...`
* Install docker and docker-compose.
* Optional (for multi-node testing), install VirtualBox and docker-machine.

* Check that everything works with `make run` and (in a different terminal) `make systest`.
* If something is broken, please open a GitHub issue.

Contributing
------------

Committed code must pass golint, go vet, gofmt and go test. Running `make test` will check all of these. Also, no more than 80 characters per line, please.

If your editor does not automatically call gofmt, `make fmt` will format all go files for you.

Running the system tests is also recommended. To run them use `make run` and (in a different terminal) `make systest`.

Misc recipes
------------

### To install Go dependencies

* `go get github.com/tools/godep`
* `godep restore ./...`

### To add new dependencies

* `go get <new-dependency>`
* import from code
* `godep save ./...`

### To update dependencies

* `go get -u <dependecy>`
* `godep update ./...`
