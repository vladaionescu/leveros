helloService in Go
==================

To run this example

```bash
$ GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ./serve server.go
$ lever deploy
$ lever invoke /helloService/SayHello '"world"'

"Hello, world!"
```
