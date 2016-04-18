
Install dev env
===============

* Install GNU make.
* Install golang and set GOROOT and GOPATH correctly.
* If not on linux/amd64, go needs to be able to cross-compile for linux/amd64.
    (On Mac you can achieve this by installing go with this command:
    `$ brew install go --with-cc-common`).
* Install protoc from https://github.com/google/protobuf/releases (eg https://github.com/google/protobuf/releases/download/v3.0.0-beta-2/protoc-3.0.0-beta-2-linux-x86_64.zip). Put protoc in your PATH somewhere.
* Install docker and docker-compose.
* Optional (for multi-node testing), install VirtualBox and docker-machine.
* Build everything with `make build`.

* Check that it works with `make run` and (in a different terminal)
    `make test`.
* If something is broken, please open a GitHub issue.

Misc recipes
------------

### To install Go dependencies

* `go get github.com/tools/godep`
* `godep restore`

### To add new dependencies

* `go get <new-dependency>`
* import from code
* `godep save ./...`

### To update dependencies

* `go get -u <dependecy>`
* `godep update ./...`
