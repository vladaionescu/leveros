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
