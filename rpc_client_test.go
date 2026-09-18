package xtframework

import (
	"errors"
	"testing"
	"time"

	"xtframework/internal/rpcpb"
)

func TestRPCClientHeartbeatFailureEvictsConnection(t *testing.T) {
	node := &Node{
		mainNodeID:        1,
		heartbeatInterval: time.Millisecond,
		rpcClients:        make(map[int]*RPCClient),
	}
	client := &RPCClient{
		node:             node,
		nodeID:           2,
		heartbeatPending: true,
	}
	client.connected.Store(true)
	node.rpcClients[client.nodeID] = client

	client.handleHeartbeatResult(errors.New("heartbeat timeout"))
	if client.Connected() {
		t.Fatal("client remains connected after heartbeat failure")
	}
	if got := node.RPCClientCount(); got != 0 {
		t.Fatalf("cached RPC clients = %d, want 0", got)
	}
}

func TestRPCClientHeartbeatRoundTrip(t *testing.T) {
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: freeAddress(t)},
			{ID: 2, ListenAddr: freeAddress(t)},
		},
	}
	logger := newTestXTLogger(t)
	mainNode, err := NewNode(cfg, 1, WithLogger(logger), WithHeartbeat(550*time.Millisecond, 500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	clientNode, err := NewNode(cfg, 2, WithLogger(logger), WithHeartbeat(550*time.Millisecond, 500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })
	if err := clientNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientNode.Stop() })

	time.Sleep(1200 * time.Millisecond)
	if got := clientNode.RPCClientCount(); got != 1 {
		t.Fatalf("cached RPC clients after heartbeat probes = %d, want 1", got)
	}
	client, err := clientNode.mainClient()
	if err != nil {
		t.Fatal(err)
	}
	if !client.Connected() {
		t.Fatal("client disconnected after successful heartbeat probes")
	}
	mainNode.remoteNodesMu.RLock()
	remoteCount := len(mainNode.remoteNodes)
	mainNode.remoteNodesMu.RUnlock()
	if remoteCount != 1 {
		t.Fatalf("remote nodes after heartbeat probes = %d, want 1", remoteCount)
	}
}

func TestRemoteNodeHeartbeatTimeoutRemovesNode(t *testing.T) {
	collector := &serviceCollector{services: make(map[string]*frameworkTestService)}
	factories := NewFactoryRegistry()
	if err := factories.Register("room", collector.factory); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{
			{ID: 1, ListenAddr: freeAddress(t)},
			{ID: 2, ListenAddr: freeAddress(t), Services: []ServiceConfig{{Name: "room", ID: 1}}},
		},
	}
	logger := newTestXTLogger(t)
	heartbeat := WithHeartbeat(100*time.Millisecond, 50*time.Millisecond)
	mainNode, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithLogger(logger), heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	clientNode, err := NewNode(cfg, 2, WithFactoryRegistry(factories), WithLogger(logger), heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if err := mainNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainNode.Stop() })
	if err := clientNode.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientNode.Stop() })

	key := ServiceKey{Name: "room", ID: 1}
	if _, found := mainNode.RegisteredService(key); !found {
		t.Fatal("remote service was not registered")
	}
	originalClient, err := clientNode.mainClient()
	if err != nil {
		t.Fatal(err)
	}
	originalClient.Close()
	waitUntil(t, func() bool {
		mainNode.remoteNodesMu.RLock()
		count := len(mainNode.remoteNodes)
		mainNode.remoteNodesMu.RUnlock()
		_, serviceFound := mainNode.RegisteredService(key)
		return count == 0 && !serviceFound
	}, "main node to remove the original node registration")

	clientWithoutHeartbeat, err := newRPCClient(clientNode, mainNode.ID(), mainNode.ListenAddr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clientWithoutHeartbeat.Close)
	if err := clientWithoutHeartbeat.requestSync(time.Second, opRegisterNode, &rpcpb.RegisterNodeRequest{
		NodeId:   int64(clientNode.ID()),
		NodeAddr: clientNode.ListenAddr(),
	}, &rpcpb.RegisterNodeResponse{}); err != nil {
		t.Fatal(err)
	}
	if err := clientWithoutHeartbeat.requestSync(time.Second, opRegister, &rpcpb.RegisterRequest{
		Location: serviceLocationToProto(ServiceLocation{
			ServiceName: "room",
			ServiceID:   1,
			NodeID:      clientNode.ID(),
			NodeAddr:    clientNode.ListenAddr(),
		}),
	}, &rpcpb.RegisterResponse{}); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, func() bool {
		mainNode.remoteNodesMu.RLock()
		count := len(mainNode.remoteNodes)
		mainNode.remoteNodesMu.RUnlock()
		_, serviceFound := mainNode.RegisteredService(key)
		return count == 0 && !serviceFound
	}, "main node to expire a remote node without heartbeats")
	if _, found := mainNode.RegisteredService(key); found {
		t.Fatal("remote service remains registered after heartbeat timeout")
	}
}
