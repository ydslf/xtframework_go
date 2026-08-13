package xtframework

import (
	"errors"
	"testing"
)

func TestMemoryRegistry(t *testing.T) {
	registry := NewMemoryRegistry()
	location := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 2, NodeAddr: "127.0.0.1:7002"}
	if err := registry.Register(location); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(location); err != nil {
		t.Fatalf("idempotent registration failed: %v", err)
	}
	other := location
	other.NodeID = 3
	other.NodeAddr = "127.0.0.1:7003"
	if err := registry.Register(other); !errors.Is(err, ErrServiceExists) {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if got, ok := registry.Lookup(location.Key()); !ok || got != location {
		t.Fatalf("Lookup() = %+v, %v", got, ok)
	}
	registry.UnregisterNode(2)
	if _, ok := registry.Lookup(location.Key()); ok {
		t.Fatal("service remains after UnregisterNode")
	}
}
