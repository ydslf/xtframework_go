package xtframework

import (
	"errors"
	"testing"
	"time"
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
}
