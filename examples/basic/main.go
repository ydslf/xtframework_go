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
)

type ping struct{ Text string }
type pong struct{ Text string }

type exampleService struct {
	xtframework.BaseService
}

func (s *exampleService) decodePing(ctx *xtframework.MessageContext, messageID uint32, payload []byte) (ping, error) {
	if messageID != messagePing {
		return ping{}, fmt.Errorf("unknown message id %d", messageID)
	}
	var request ping
	if err := json.Unmarshal(payload, &request); err != nil {
		return ping{}, err
	}
	s.Logger().LogDebug("received %q from %s", request.Text, ctx.Source())
	return request, nil
}

func (s *exampleService) HandleRPCDirect(ctx *xtframework.MessageContext, messageID uint32, payload []byte) error {
	_, err := s.decodePing(ctx, messageID, payload)
	return err
}

func (s *exampleService) HandleRPCRequest(ctx *xtframework.MessageContext, messageID uint32, payload []byte) ([]byte, error) {
	request, err := s.decodePing(ctx, messageID, payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(&pong{Text: "pong: " + request.Text})
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
