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
	epoch    uint64
	revision uint64
	stale    bool
}

// serviceRouteCache 缓存主节点返回的 Service 路由，并合并同一个 Service
// 的并发查询，避免缓存未命中时同时向主节点发送重复请求。
type serviceRouteCache struct {
	mu        sync.RWMutex
	ttl       time.Duration
	entries   map[ServiceKey]routeCacheEntry
	lookups   map[ServiceKey]*routeLookupCall
	epoch     uint64
	revisions map[ServiceKey]uint64
}

func newServiceRouteCache(ttl time.Duration) *serviceRouteCache {
	return &serviceRouteCache{
		ttl:       ttl,
		entries:   make(map[ServiceKey]routeCacheEntry),
		lookups:   make(map[ServiceKey]*routeLookupCall),
		revisions: make(map[ServiceKey]uint64),
	}
}

func (c *serviceRouteCache) lookup(key ServiceKey, load func() (ServiceLocation, error)) (ServiceLocation, error) {
	for {
		now := time.Now()
		c.mu.RLock()
		entry, exists := c.entries[key]
		if exists && now.Before(entry.expiresAt) {
			c.mu.RUnlock()
			return entry.location, nil
		}
		c.mu.RUnlock()

		c.mu.Lock()
		if entry, exists = c.entries[key]; exists {
			now = time.Now()
			if now.Before(entry.expiresAt) {
				c.mu.Unlock()
				return entry.location, nil
			}
			delete(c.entries, key)
		}

		epoch := c.epoch
		revision := c.revisions[key]
		if call, exists := c.lookups[key]; exists && call.epoch == epoch && call.revision == revision {
			c.mu.Unlock()
			<-call.done
			if call.stale {
				continue
			}
			return call.location, call.err
		}
		call := &routeLookupCall{
			done:     make(chan struct{}),
			epoch:    epoch,
			revision: revision,
		}
		c.lookups[key] = call
		c.mu.Unlock()

		location, err := load()

		c.mu.Lock()
		call.location = location
		call.err = err
		call.stale = c.epoch != epoch || c.revisions[key] != revision
		if !call.stale && err == nil && c.ttl > 0 {
			c.entries[key] = routeCacheEntry{
				location:  location,
				expiresAt: time.Now().Add(c.ttl),
			}
		}
		if c.lookups[key] == call {
			delete(c.lookups, key)
		}
		close(call.done)
		c.mu.Unlock()
		if call.stale {
			continue
		}
		return location, err
	}
}

func (c *serviceRouteCache) invalidate(key ServiceKey) {
	c.mu.Lock()
	delete(c.entries, key)
	c.revisions[key]++
	c.mu.Unlock()
}

func (c *serviceRouteCache) invalidateAll() {
	c.mu.Lock()
	clear(c.entries)
	c.epoch++
	c.mu.Unlock()
}
