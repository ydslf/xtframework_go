package xtframework

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

const (
	testRequestID  uint32 = 1
	testResponseID uint32 = 2
)

type testPayload struct{ Text string }

type frameworkTestService struct {
	BaseService
	received chan string
}

func (s *frameworkTestService) HandleMessage(ctx *MessageContext, msg *Message) error {
	payload := msg.Payload.(*testPayload)
	select {
	case s.received <- payload.Text:
	default:
	}
	if ctx.IsRequest() {
		return ctx.Respond(&Message{ID: testResponseID, Payload: &testPayload{Text: "reply:" + payload.Text}})
	}
	return nil
}

type serviceCollector struct {
	mu       sync.Mutex
	services map[string]*frameworkTestService
}

func (c *serviceCollector) factory(node *Node, config ServiceConfig) (Service, error) {
	service := &frameworkTestService{BaseService: NewBaseService(node, config), received: make(chan string, 8)}
	c.mu.Lock()
	c.services[fmt.Sprintf("%d/%s:%d", node.ID, config.Name, config.ID)] = service
	c.mu.Unlock()
	return service, nil
}

func (c *serviceCollector) get(nodeID int, name string, id int) *frameworkTestService {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.services[fmt.Sprintf("%d/%s:%d", nodeID, name, id)]
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func testMessages(t *testing.T) *MessageRegistry {
	t.Helper()
	registry := NewMessageRegistry()
	for _, id := range []uint32{testRequestID, testResponseID} {
		if err := registry.Register(id, func() any { return &testPayload{} }); err != nil {
			t.Fatal(err)
		}
	}
	return registry
}

func waitMessage(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("message = %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

func TestNodeLocalAndRemoteMessaging(t *testing.T) {
	collector := &serviceCollector{services: make(map[string]*frameworkTestService)}
	factories := NewFactoryRegistry()
	for _, name := range []string{"center", "room"} {
		if err := factories.Register(name, collector.factory); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "center", ID: 1}}},
			{ID: 2, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "room", ID: 1}, {Name: "room", ID: 2}}},
		},
	}

	mainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithMessageRegistry(testMessages(t)))
	if err != nil {
		t.Fatal(err)
	}
	roomNode, err := NewNode(cfg, 2, WithFactoryRegistry(factories), WithMessageRegistry(testMessages(t)))
	if err != nil {
		t.Fatal(err)
	}
	if mainNode.LocalService[ServiceKey{Name: "center", ID: 1}].Loop() == roomNode.LocalService[ServiceKey{Name: "room", ID: 1}].Loop() {
		t.Fatal("services unexpectedly share a loop")
	}
	if roomNode.LocalService[ServiceKey{Name: "room", ID: 1}].Loop() == roomNode.LocalService[ServiceKey{Name: "room", ID: 2}].Loop() {
		t.Fatal("services in one node share a loop")
	}

	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })
	if err := roomNode.Start(); err != nil {
		t.Fatal(err)
	}
	roomStopped := false
	t.Cleanup(func() {
		if !roomStopped {
			_ = roomNode.Stop()
		}
	})

	room1 := collector.get(2, "room", 1)
	if err := roomNode.Send2Service("room", 1, &Message{ID: testRequestID, Payload: &testPayload{Text: "local"}}); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, room1.received, "local")

	if err := mainNode.Send2Service("room", 1, &Message{ID: testRequestID, Payload: &testPayload{Text: "remote"}}); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, room1.received, "remote")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := mainNode.CallService(ctx, "room", 1, &Message{ID: testRequestID, Payload: &testPayload{Text: "call"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Payload.(*testPayload).Text; got != "reply:call" {
		t.Fatalf("response = %q", got)
	}
	waitMessage(t, room1.received, "call")

	response, err = roomNode.CallService(ctx, "room", 2, &Message{ID: testRequestID, Payload: &testPayload{Text: "local-call"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Payload.(*testPayload).Text; got != "reply:local-call" {
		t.Fatalf("local response = %q", got)
	}

	if _, err := mainNode.CallService(ctx, "missing", 1, &Message{ID: testRequestID, Payload: &testPayload{Text: "missing"}}); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("missing service error = %v", err)
	}
	if _, err := roomNode.CallService(ctx, "missing", 1, &Message{ID: testRequestID, Payload: &testPayload{Text: "missing-remote"}}); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("remote missing service error = %v", err)
	}

	if err := roomNode.Stop(); err != nil {
		t.Fatal(err)
	}
	roomStopped = true
	if _, found := mainNode.Registry.Lookup(ServiceKey{Name: "room", ID: 1}); found {
		t.Fatal("room service remains registered after node stop")
	}
}

func TestNewNodeRequiresFactory(t *testing.T) {
	cfg := &Config{MainNode: 1, Nodes: []NodeConfig{{ID: 1, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "missing", ID: 1}}}}}
	_, err := NewNode(cfg, 1, WithFactoryRegistry(NewFactoryRegistry()))
	if !errors.Is(err, ErrFactoryNotFound) {
		t.Fatalf("NewNode() error = %v", err)
	}
}

func TestNodeStartRollsBackRegisteredServices(t *testing.T) {
	collector := &serviceCollector{services: make(map[string]*frameworkTestService)}
	factories := NewFactoryRegistry()
	for _, name := range []string{"center", "room"} {
		if err := factories.Register(name, collector.factory); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "center", ID: 1}}},
			{ID: 2, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "room", ID: 1}, {Name: "center", ID: 1}}},
		},
	}
	mainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithMessageRegistry(testMessages(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })

	secondNode, err := NewNode(cfg, 2, WithFactoryRegistry(factories), WithMessageRegistry(testMessages(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := secondNode.Start(); !errors.Is(err, ErrServiceExists) {
		t.Fatalf("Start() error = %v, want ErrServiceExists", err)
	}
	if _, found := mainNode.Registry.Lookup(ServiceKey{Name: "room", ID: 1}); found {
		t.Fatal("service registered before startup failure was not rolled back")
	}
}
