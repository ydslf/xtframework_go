package main

import (
	"encoding/json"
	"flag"
	"fmt"
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

func (s *exampleService) HandleMessage(ctx *xtframework.MessageContext, messageID uint32, payload []byte) error {
	if messageID != messagePing {
		return fmt.Errorf("unknown message id %d", messageID)
	}
	var request ping
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	log.Printf("service %s:%d received %q from %s", s.Name(), s.ID(), request.Text, ctx.Source())
	if ctx.IsRequest() {
		response, err := json.Marshal(&pong{Text: "pong: " + request.Text})
		if err != nil {
			return err
		}
		return ctx.Respond(messagePong, response)
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

	node, err := xtframework.NewNode(config, *nodeID,
		xtframework.WithFactoryRegistry(factories),
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
