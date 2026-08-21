package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"xtframework"
)

const (
	messagePing uint32 = 1001
	messagePong uint32 = 1002
)

type ping struct{ Text string }
type pong struct{ Text string }

type exampleService struct {
	xtframework.BaseService
}

func (s *exampleService) HandleMessage(ctx *xtframework.MessageContext, message *xtframework.Message) error {
	request := message.Payload.(*ping)
	log.Printf("service %s:%d received %q from %s", s.Name(), s.ID(), request.Text, ctx.Source())
	if ctx.IsRequest() {
		return ctx.Respond(&xtframework.Message{ID: messagePong, Payload: &pong{Text: "pong: " + request.Text}})
	}
	return nil
}

func main() {
	configPath := flag.String("config", "nodes.yaml", "path to the cluster YAML config")
	nodeID := flag.Int("node", 1, "node ID to run")
	flag.Parse()

	config, err := xtframework.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	factories := xtframework.NewFactoryRegistry()
	for _, name := range []string{"center", "room"} {
		serviceName := name
		if err := factories.Register(serviceName, func(node *xtframework.Node, serviceConfig xtframework.ServiceConfig) (xtframework.Service, error) {
			return &exampleService{BaseService: xtframework.NewBaseService(node, serviceConfig)}, nil
		}); err != nil {
			log.Fatal(err)
		}
	}

	messages := xtframework.NewMessageRegistry()
	if err := messages.Register(messagePing, func() any { return &ping{} }); err != nil {
		log.Fatal(err)
	}
	if err := messages.Register(messagePong, func() any { return &pong{} }); err != nil {
		log.Fatal(err)
	}

	node, err := xtframework.NewNode(config, *nodeID,
		xtframework.WithFactoryRegistry(factories),
		xtframework.WithMessageRegistry(messages),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := node.Start(); err != nil {
		log.Fatal(err)
	}
	log.Printf("node %d is listening at %s", node.ID(), node.ListenAddr())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	if err := node.Stop(); err != nil {
		log.Printf("stop node: %v", err)
	}
}
