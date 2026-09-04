package xtframework

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testRequestID  uint32 = 1
	testResponseID uint32 = 2
)

type frameworkTestService struct {
	BaseService
	received chan string
}

func (s *frameworkTestService) HandleMessage(ctx *MessageContext, messageID uint32, payload []byte) error {
	if messageID != testRequestID {
		return fmt.Errorf("unexpected message id %d", messageID)
	}
	text := string(payload)
	select {
	case s.received <- text:
	default:
	}
	if ctx.IsRequest() {
		return ctx.Respond(testResponseID, []byte("reply:"+text))
	}
	return nil
}

type serviceCollector struct {
	mu       sync.Mutex
	services map[string]*frameworkTestService
}

type countingRegistry struct {
	*MemoryRegistry
	lookups atomic.Int32
}

func (r *countingRegistry) Lookup(key ServiceKey) (ServiceLocation, bool) {
	r.lookups.Add(1)
	return r.MemoryRegistry.Lookup(key)
}

func (c *serviceCollector) factory(node *Node, config ServiceConfig) (Service, error) {
	service := &frameworkTestService{BaseService: NewBaseService(node, config), received: make(chan string, 8)}
	c.mu.Lock()
	c.services[fmt.Sprintf("%d/%s:%d", node.ID(), config.Name, config.ID)] = service
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

func waitUntil(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
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

	mainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories))
	if err != nil {
		t.Fatal(err)
	}
	roomNode, err := NewNode(cfg, 2, WithFactoryRegistry(factories))
	if err != nil {
		t.Fatal(err)
	}
	center, centerFound := mainNode.LocalService(ServiceKey{Name: "center", ID: 1})
	room1Service, room1Found := roomNode.LocalService(ServiceKey{Name: "room", ID: 1})
	room2Service, room2Found := roomNode.LocalService(ServiceKey{Name: "room", ID: 2})
	if !centerFound || !room1Found || !room2Found {
		t.Fatal("expected local services were not found")
	}
	localSnapshot := roomNode.LocalServices()
	delete(localSnapshot, ServiceKey{Name: "room", ID: 1})
	if _, found := roomNode.LocalService(ServiceKey{Name: "room", ID: 1}); !found {
		t.Fatal("modifying LocalServices snapshot changed the node container")
	}
	if center.Loop() == room1Service.Loop() {
		t.Fatal("services unexpectedly share a loop")
	}
	if room1Service.Loop() == room2Service.Loop() {
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
	if err := roomNode.Send2Service("room", 1, testRequestID, []byte("local")); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, room1.received, "local")

	if err := mainNode.Send2Service("room", 1, testRequestID, []byte("remote")); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, room1.received, "remote")

	responseID, response, err := mainNode.CallService(3*time.Second, "room", 1, testRequestID, []byte("call"))
	if err != nil {
		t.Fatal(err)
	}
	if responseID != testResponseID {
		t.Fatalf("response id = %d, want %d", responseID, testResponseID)
	}
	if got := string(response); got != "reply:call" {
		t.Fatalf("response = %q", got)
	}
	waitMessage(t, room1.received, "call")

	responseID, response, err = roomNode.CallService(3*time.Second, "room", 2, testRequestID, []byte("local-call"))
	if err != nil {
		t.Fatal(err)
	}
	if responseID != testResponseID {
		t.Fatalf("local response id = %d, want %d", responseID, testResponseID)
	}
	if got := string(response); got != "reply:local-call" {
		t.Fatalf("local response = %q", got)
	}

	if _, _, err := mainNode.CallService(3*time.Second, "missing", 1, testRequestID, []byte("missing")); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("missing service error = %v", err)
	}
	if _, _, err := roomNode.CallService(3*time.Second, "missing", 1, testRequestID, []byte("missing-remote")); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("remote missing service error = %v", err)
	}

	if err := roomNode.Stop(); err != nil {
		t.Fatal(err)
	}
	roomStopped = true
	waitUntil(t, func() bool { return mainNode.RPCClientCount() == 0 }, "main node to release the stopped room node connection")
	if _, found := mainNode.RegisteredService(ServiceKey{Name: "room", ID: 1}); found {
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
	mainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories))
	if err != nil {
		t.Fatal(err)
	}
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })

	secondNode, err := NewNode(cfg, 2, WithFactoryRegistry(factories))
	if err != nil {
		t.Fatal(err)
	}
	if err := secondNode.Start(); !errors.Is(err, ErrServiceExists) {
		t.Fatalf("Start() error = %v, want ErrServiceExists", err)
	}
	if _, found := mainNode.RegisteredService(ServiceKey{Name: "room", ID: 1}); found {
		t.Fatal("service registered before startup failure was not rolled back")
	}
}

func TestNodeCachesRemoteServiceLocation(t *testing.T) {
	collector := &serviceCollector{services: make(map[string]*frameworkTestService)}
	factories := NewFactoryRegistry()
	if err := factories.Register("center", collector.factory); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "center", ID: 1}}},
			{ID: 2, ListenAddr: freeAddress(t)},
		},
	}
	registry := &countingRegistry{MemoryRegistry: NewMemoryRegistry()}
	mainNode, err := NewNode(cfg, 1,
		WithFactoryRegistry(factories),
		WithServiceRegistry(registry),
	)
	if err != nil {
		t.Fatal(err)
	}
	senderNode, err := NewNode(cfg, 2,
		WithFactoryRegistry(factories),
		WithRouteCacheTTL(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })
	if err := senderNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = senderNode.Stop() })

	center := collector.get(1, "center", 1)
	for _, text := range []string{"first", "second"} {
		if err := senderNode.Send2Service("center", 1, testRequestID, []byte(text)); err != nil {
			t.Fatal(err)
		}
		waitMessage(t, center.received, text)
	}
	if got := registry.lookups.Load(); got != 1 {
		t.Fatalf("main node lookup count = %d, want 1", got)
	}
}
