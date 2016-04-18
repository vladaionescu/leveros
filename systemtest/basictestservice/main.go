package main

import (
	"fmt"
	"io"
	"log"

	"github.com/leveros/leveros/api"
)

func main() {
	server, err := api.NewServer()
	if err != nil {
		log.Fatalf("Error creating server: %v\n", err)
	}
	err = server.RegisterHandlerObject(new(Handler))
	if err != nil {
		log.Fatalf("Error registering handler: %v\n", err)
	}

	log.Printf("Serving...\n")
	server.Serve()
}

// Handler handles RPCs.
type Handler struct {
}

// BasicHello tests a very basic RPC.
func (*Handler) BasicHello(name string) (result string, err error) {
	return fmt.Sprintf("hello %s", name), nil
}

// ReqReplyStreamChan tests a request-reply type of com over a stream.
func (*Handler) ReqReplyStreamChan(stream api.Stream, name string) error {
	for {
		var msg string
		err := stream.Receive(&msg)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error: %v\n", err)
			return err
		}
		err = stream.Send(name + ":" + msg + " received")
		if err != nil {
			log.Printf("Error: %v\n", err)
			return err
		}
	}
	return stream.Close()
}

// BasicError tests errors in RPCs.
func (*Handler) BasicError(name string) (result string, err error) {
	return "", fmt.Errorf("error sent: %s", name)
}
