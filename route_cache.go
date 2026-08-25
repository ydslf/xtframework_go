package xtframework

import (
	"sync"
	"time"
)

type routeCacheEntry struct {
	location  ServiceLocation
	expiresAt time.Time
}

type routeLookupCall struct {
	done     chan struct{}
	location ServiceLocation
	err      error
}

// serviceRouteCache 缓存主节点返回的 Service 路由，并合并同一个 Service
// 的并发查询，避免缓存未命中时同时向主节点发送重复请求。
type serviceRouteCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[ServiceKey]routeCacheEntry
	lookups map[ServiceKey]*routeLookupCall
}

func newServiceRouteCache(ttl time.Duration) *serviceRouteCache {
	return &serviceRouteCache{
		ttl:     ttl,
		entries: make(map[ServiceKey]routeCacheEntry),
		lookups: make(map[ServiceKey]*routeLookupCall),
	}
}

func (c *serviceRouteCache) lookup(key ServiceKey, load func() (ServiceLocation, error)) (ServiceLocation, error) {
	now := time.Now()
	c.mu.Lock()
	if entry, exists := c.entries[key]; exists {
		if now.Before(entry.expiresAt) {
			c.mu.Unlock()
			return entry.location, nil
		}
		delete(c.entries, key)
	}
	if call, exists := c.lookups[key]; exists {
		c.mu.Unlock()
		<-call.done
		return call.location, call.err
	}
	call := &routeLookupCall{done: make(chan struct{})}
	c.lookups[key] = call
	c.mu.Unlock()

	location, err := load()

	c.mu.Lock()
	call.location = location
	call.err = err
	if err == nil && c.ttl > 0 {
		c.entries[key] = routeCacheEntry{
			location:  location,
			expiresAt: time.Now().Add(c.ttl),
		}
	}
	delete(c.lookups, key)
	close(call.done)
	c.mu.Unlock()
	return location, err
}

func (c *serviceRouteCache) invalidate(key ServiceKey) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}
