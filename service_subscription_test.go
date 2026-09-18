package xtframework

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestServiceSubscriptionIndex(t *testing.T) {
	index := newServiceSubscriptionIndex()
	a := ServiceKey{Name: "watcher", ID: 1}
	b := ServiceKey{Name: "watcher", ID: 2}

	if !index.subscribe(a, "room") {
		t.Fatal("first subscription was not added")
	}
	if index.subscribe(a, "room") {
		t.Fatal("duplicate subscription was added")
	}
	index.subscribe(a, "gate")
	index.subscribe(b, "room")

	if got := index.subscribers("room"); len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("room subscribers = %v, want [%s %s]", got, a, b)
	}
	index.removeSubscriber(a)
	if got := index.subscribers("room"); len(got) != 1 || got[0] != b {
		t.Fatalf("room subscribers after removal = %v, want [%s]", got, b)
	}
	if got := index.subscribers("gate"); len(got) != 0 {
		t.Fatalf("gate subscribers after removal = %v, want empty", got)
	}
}

type subscriptionTestEvent struct {
	kind        string
	serviceName string
	services    []ServiceKey
}

type subscriptionTestWatcher struct {
	BaseService
	events chan subscriptionTestEvent
}

func (s *subscriptionTestWatcher) Start() error {
	return s.Subscribe("worker")
}

func (s *subscriptionTestWatcher) HandleServiceSnapshot(serviceName string, services []ServiceKey) {
	s.events <- subscriptionTestEvent{
		kind:        "snapshot",
		serviceName: serviceName,
		services:    append([]ServiceKey(nil), services...),
	}
}

func (s *subscriptionTestWatcher) HandleServiceOnline(service ServiceKey) {
	s.events <- subscriptionTestEvent{kind: "online", serviceName: service.Name, services: []ServiceKey{service}}
}

func (s *subscriptionTestWatcher) HandleServiceOffline(service ServiceKey) {
	s.events <- subscriptionTestEvent{kind: "offline", serviceName: service.Name, services: []ServiceKey{service}}
}

type subscriptionTestWorker struct{ BaseService }

type subscriptionTestCollector struct {
	mu      sync.Mutex
	watcher *subscriptionTestWatcher
}

func (c *subscriptionTestCollector) factory(node *Node, config ServiceConfig) (Service, error) {
	switch config.Name {
	case "watcher":
		service := &subscriptionTestWatcher{
			BaseService: NewBaseService(node, config),
			events:      make(chan subscriptionTestEvent, 16),
		}
		c.mu.Lock()
		c.watcher = service
		c.mu.Unlock()
		return service, nil
	case "worker":
		return &subscriptionTestWorker{BaseService: NewBaseService(node, config)}, nil
	default:
		return nil, fmt.Errorf("unexpected service %q", config.Name)
	}
}

func (c *subscriptionTestCollector) getWatcher() *subscriptionTestWatcher {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.watcher
}

func waitSubscriptionEvent(t *testing.T, events <-chan subscriptionTestEvent, kind, serviceName string, services []ServiceKey) {
	t.Helper()
	select {
	case event := <-events:
		if event.kind != kind || event.serviceName != serviceName {
			t.Fatalf("discovery event = %+v, want kind=%q service=%q", event, kind, serviceName)
		}
		if len(event.services) != len(services) {
			t.Fatalf("discovery event services = %v, want %v", event.services, services)
		}
		for i := range services {
			if event.services[i] != services[i] {
				t.Fatalf("discovery event services = %v, want %v", event.services, services)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s event for %q", kind, serviceName)
	}
}

func TestServiceSubscriptionLifecycle(t *testing.T) {
	collector := &subscriptionTestCollector{}
	factories := NewFactoryRegistry()
	for _, name := range []string{"watcher", "worker"} {
		if err := factories.Register(name, collector.factory); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: freeAddress(t)},
			{ID: 2, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "watcher", ID: 1}}},
			{ID: 3, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "worker", ID: 1}}},
			{ID: 4, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "worker", ID: 2}}},
		},
	}
	logger := newTestXTLogger(t)
	mainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	watcherNode, err := NewNode(cfg, 2, WithFactoryRegistry(factories), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	workerNode1, err := NewNode(cfg, 3, WithFactoryRegistry(factories), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	workerNode2, err := NewNode(cfg, 4, WithFactoryRegistry(factories), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}

	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })
	if err := watcherNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcherNode.Stop() })
	watcher := collector.getWatcher()
	if watcher == nil {
		t.Fatal("watcher service was not created")
	}
	waitSubscriptionEvent(t, watcher.events, "snapshot", "worker", nil)

	if err := workerNode1.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerNode1.Stop() })
	worker1 := ServiceKey{Name: "worker", ID: 1}
	waitSubscriptionEvent(t, watcher.events, "online", "worker", []ServiceKey{worker1})

	resubscribed := make(chan error, 1)
	watcher.Loop().Post(func() { resubscribed <- watcher.Subscribe("worker") })
	select {
	case err := <-resubscribed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out resubscribing to worker")
	}
	waitSubscriptionEvent(t, watcher.events, "snapshot", "worker", []ServiceKey{worker1})

	if err := workerNode2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerNode2.Stop() })
	worker2 := ServiceKey{Name: "worker", ID: 2}
	waitSubscriptionEvent(t, watcher.events, "online", "worker", []ServiceKey{worker2})

	worker2MainClient, err := workerNode2.mainClient()
	if err != nil {
		t.Fatal(err)
	}
	worker2MainClient.Close()
	waitSubscriptionEvent(t, watcher.events, "offline", "worker", []ServiceKey{worker2})

	if err := workerNode1.Stop(); err != nil {
		t.Fatal(err)
	}
	waitSubscriptionEvent(t, watcher.events, "offline", "worker", []ServiceKey{worker1})

	if err := watcherNode.Stop(); err != nil {
		t.Fatal(err)
	}
	if subscribers := mainNode.subscriptions.subscribers("worker"); len(subscribers) != 0 {
		t.Fatalf("subscriptions after watcher stopped = %v, want empty", subscribers)
	}
}

func TestMainNodeServiceSubscription(t *testing.T) {
	collector := &subscriptionTestCollector{}
	factories := NewFactoryRegistry()
	for _, name := range []string{"watcher", "worker"} {
		if err := factories.Register(name, collector.factory); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "watcher", ID: 1}}},
			{ID: 2, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "worker", ID: 1}}},
		},
	}
	logger := newTestXTLogger(t)
	mainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	workerNode, err := NewNode(cfg, 2, WithFactoryRegistry(factories), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })
	watcher := collector.getWatcher()
	if watcher == nil {
		t.Fatal("watcher service was not created")
	}
	waitSubscriptionEvent(t, watcher.events, "snapshot", "worker", nil)

	if err := workerNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerNode.Stop() })
	waitSubscriptionEvent(t, watcher.events, "online", "worker", []ServiceKey{{Name: "worker", ID: 1}})
}
