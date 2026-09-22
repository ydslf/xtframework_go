package xtframework

import (
	"testing"
	"time"
)

func waitRecoveredSnapshot(t *testing.T, events <-chan subscriptionTestEvent, serviceName string, services []ServiceKey) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-events:
			if event.kind == "offline" {
				// A graceful main-node stop may enqueue an Offline immediately
				// before the connection closes, but delivery is best effort.
				continue
			}
			if event.kind != "snapshot" || event.serviceName != serviceName || len(event.services) != len(services) {
				t.Fatalf("recovery event = %+v, want snapshot for %q with %v", event, serviceName, services)
			}
			for i := range services {
				if event.services[i] != services[i] {
					t.Fatalf("recovery snapshot services = %v, want %v", event.services, services)
				}
			}
			return
		case <-deadline.C:
			t.Fatalf("timed out waiting for recovered snapshot for %q", serviceName)
		}
	}
}

func TestMainNodeReconnectRestoresServicesAndSubscriptions(t *testing.T) {
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
			{ID: 1, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "worker", ID: 1}}},
			{ID: 2, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "watcher", ID: 1}}},
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
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })
	if err := watcherNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcherNode.Stop() })

	watcher := collector.getWatcher()
	worker := ServiceKey{Name: "worker", ID: 1}
	waitSubscriptionEvent(t, watcher.events, "snapshot", "worker", []ServiceKey{worker})

	mainNode.remoteNodesMu.RLock()
	originalRemote := mainNode.remoteNodes[watcherNode.ID()]
	mainNode.remoteNodesMu.RUnlock()
	if originalRemote == nil {
		t.Fatal("watcher node was not registered on the main node")
	}
	originalClient, err := watcherNode.mainClient()
	if err != nil {
		t.Fatal(err)
	}
	originalClient.Close()

	// Subscription replay always produces a fresh authoritative snapshot.
	waitSubscriptionEvent(t, watcher.events, "snapshot", "worker", []ServiceKey{worker})
	watcherKey := ServiceKey{Name: "watcher", ID: 1}
	waitUntil(t, func() bool {
		location, registered := mainNode.RegisteredService(watcherKey)
		if !registered || location.NodeID != watcherNode.ID() {
			return false
		}
		mainNode.remoteNodesMu.RLock()
		currentRemote := mainNode.remoteNodes[watcherNode.ID()]
		mainNode.remoteNodesMu.RUnlock()
		return currentRemote != nil && currentRemote != originalRemote
	}, "watcher node state to be restored on a new main-node connection")

	subscribers := mainNode.subscriptions.subscribers("worker")
	if len(subscribers) != 1 || subscribers[0] != watcherKey {
		t.Fatalf("worker subscribers after reconnect = %v, want [%s]", subscribers, watcherKey)
	}
	var recoveredClient *RPCClient
	waitUntil(t, func() bool {
		client, err := watcherNode.mainClient()
		if err != nil || client == originalClient || !client.Connected() {
			return false
		}
		recoveredClient = client
		return true
	}, "recovered main-node connection to be published")
	if recoveredClient == nil {
		t.Fatal("main-node connection was not replaced with a connected client")
	}
}

func TestMainNodeRestartRestoresServicesAndSubscriptions(t *testing.T) {
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
			{ID: 1, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "worker", ID: 1}}},
			{ID: 2, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "watcher", ID: 1}}},
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
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	if err := watcherNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcherNode.Stop() })

	watcher := collector.getWatcher()
	worker := ServiceKey{Name: "worker", ID: 1}
	waitSubscriptionEvent(t, watcher.events, "snapshot", "worker", []ServiceKey{worker})
	if err := mainNode.Stop(); err != nil {
		t.Fatal(err)
	}

	restartedMainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedMainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restartedMainNode.Stop() })

	waitRecoveredSnapshot(t, watcher.events, "worker", []ServiceKey{worker})
	watcherKey := ServiceKey{Name: "watcher", ID: 1}
	waitUntil(t, func() bool {
		location, registered := restartedMainNode.RegisteredService(watcherKey)
		return registered && location.NodeID == watcherNode.ID()
	}, "watcher service to be restored after the main node restarted")
}
