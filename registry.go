package xtframework

import (
	"fmt"
	"sync"
)

type ServiceKey struct {
	Name string
	ID   int
}

func (k ServiceKey) String() string { return fmt.Sprintf("%s:%d", k.Name, k.ID) }

type ServiceLocation struct {
	ServiceName string `json:"service_name"`
	ServiceID   int    `json:"service_id"`
	NodeID      int    `json:"node_id"`
	NodeAddr    string `json:"node_addr"`
}

func (l ServiceLocation) Key() ServiceKey {
	return ServiceKey{Name: l.ServiceName, ID: l.ServiceID}
}

// ServiceRegistry 用于保存和查询 Service 的位置信息。
//
// 实现必须保证并发安全。Node 可能从多个 RPC session 和应用程序
// goroutine 中并发调用这些方法。
type ServiceRegistry interface {
	Register(ServiceLocation) error
	Unregister(ServiceKey, int) error
	UnregisterNode(int)
	Lookup(ServiceKey) (ServiceLocation, bool)
	List() []ServiceLocation
}

type MemoryRegistry struct {
	mu       sync.RWMutex
	services map[ServiceKey]ServiceLocation
}

func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{services: make(map[ServiceKey]ServiceLocation)}
}

func (r *MemoryRegistry) Register(location ServiceLocation) error {
	key := location.Key()
	if key.Name == "" || key.ID <= 0 || location.NodeID <= 0 || location.NodeAddr == "" {
		return fmt.Errorf("invalid service location: %+v", location)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, exists := r.services[key]; exists {
		if current == location {
			return nil
		}
		return fmt.Errorf("%w: %s belongs to node %d", ErrServiceExists, key, current.NodeID)
	}
	r.services[key] = location
	return nil
}

func (r *MemoryRegistry) Unregister(key ServiceKey, nodeID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.services[key]
	if !exists {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, key)
	}
	if current.NodeID != nodeID {
		return fmt.Errorf("service %s belongs to node %d, not node %d", key, current.NodeID, nodeID)
	}
	delete(r.services, key)
	return nil
}

func (r *MemoryRegistry) UnregisterNode(nodeID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, location := range r.services {
		if location.NodeID == nodeID {
			delete(r.services, key)
		}
	}
}

func (r *MemoryRegistry) Lookup(key ServiceKey) (ServiceLocation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	location, exists := r.services[key]
	return location, exists
}

func (r *MemoryRegistry) List() []ServiceLocation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]ServiceLocation, 0, len(r.services))
	for _, location := range r.services {
		result = append(result, location)
	}
	return result
}
