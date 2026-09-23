package xtframework

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestServiceRouteCacheReuseAndInvalidate(t *testing.T) {
	cache := newServiceRouteCache(time.Minute, true)
	key := ServiceKey{Name: "room", ID: 1}
	want := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 2, NodeAddr: "127.0.0.1:7002"}
	var loads atomic.Int32
	load := func() (ServiceLocation, error) {
		loads.Add(1)
		return want, nil
	}

	for range 2 {
		got, err := cache.lookup(key, load)
		if err != nil || got != want {
			t.Fatalf("lookup() = %+v, %v", got, err)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}

	cache.invalidate(key)
	if _, err := cache.lookup(key, load); err != nil {
		t.Fatal(err)
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("loader calls after invalidation = %d, want 2", got)
	}
}

func TestServiceRouteCacheMergesConcurrentLookups(t *testing.T) {
	cache := newServiceRouteCache(time.Minute, true)
	key := ServiceKey{Name: "room", ID: 1}
	want := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 2, NodeAddr: "127.0.0.1:7002"}
	var loads atomic.Int32
	release := make(chan struct{})
	load := func() (ServiceLocation, error) {
		loads.Add(1)
		<-release
		return want, nil
	}

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			got, err := cache.lookup(key, load)
			if err != nil || got != want {
				t.Errorf("lookup() = %+v, %v", got, err)
			}
		}()
	}

	deadline := time.Now().Add(time.Second)
	for loads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	if got := loads.Load(); got != 1 {
		t.Fatalf("concurrent loader calls = %d, want 1", got)
	}
}

func TestServiceRouteCacheInvalidationRejectsInflightLookup(t *testing.T) {
	cache := newServiceRouteCache(time.Hour, true)
	key := ServiceKey{Name: "room", ID: 1}
	oldLocation := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 2, NodeAddr: "127.0.0.1:7002"}
	newLocation := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 3, NodeAddr: "127.0.0.1:7003"}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var loads atomic.Int32
	load := func() (ServiceLocation, error) {
		if loads.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return oldLocation, nil
		}
		return newLocation, nil
	}

	firstResult := make(chan ServiceLocation, 1)
	go func() {
		location, _ := cache.lookup(key, load)
		firstResult <- location
	}()
	<-firstStarted

	cache.invalidate(key)
	second, err := cache.lookup(key, load)
	if err != nil {
		t.Fatal(err)
	}
	if second != newLocation {
		t.Fatalf("lookup after invalidation = %+v, want %+v", second, newLocation)
	}

	close(releaseFirst)
	if first := <-firstResult; first != newLocation {
		t.Fatalf("in-flight lookup after invalidation = %+v, want %+v", first, newLocation)
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
}

func TestServiceRouteCacheInvalidateAll(t *testing.T) {
	cache := newServiceRouteCache(time.Hour, true)
	key := ServiceKey{Name: "room", ID: 1}
	location := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 2, NodeAddr: "127.0.0.1:7002"}
	var loads atomic.Int32
	load := func() (ServiceLocation, error) {
		loads.Add(1)
		return location, nil
	}

	if _, err := cache.lookup(key, load); err != nil {
		t.Fatal(err)
	}
	cache.invalidateAll()
	if _, err := cache.lookup(key, load); err != nil {
		t.Fatal(err)
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("loader calls after invalidateAll = %d, want 2", got)
	}
}

func TestServiceRouteCacheZeroTTLDoesNotExpire(t *testing.T) {
	cache := newServiceRouteCache(0, true)
	key := ServiceKey{Name: "room", ID: 1}
	want := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 2, NodeAddr: "127.0.0.1:7002"}
	var loads atomic.Int32
	load := func() (ServiceLocation, error) {
		loads.Add(1)
		return want, nil
	}

	for range 2 {
		got, err := cache.lookup(key, load)
		if err != nil || got != want {
			t.Fatalf("lookup() = %+v, %v", got, err)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}

	cache.mu.RLock()
	expiresAt := cache.entries[key].expiresAt
	cache.mu.RUnlock()
	if !expiresAt.IsZero() {
		t.Fatalf("zero TTL expiration = %s, want zero time", expiresAt)
	}
}

func TestServiceRouteCacheDisabled(t *testing.T) {
	cache := newServiceRouteCache(time.Hour, false)
	key := ServiceKey{Name: "room", ID: 1}
	want := ServiceLocation{ServiceName: "room", ServiceID: 1, NodeID: 2, NodeAddr: "127.0.0.1:7002"}
	var loads atomic.Int32
	load := func() (ServiceLocation, error) {
		loads.Add(1)
		return want, nil
	}

	for range 2 {
		got, err := cache.lookup(key, load)
		if err != nil || got != want {
			t.Fatalf("lookup() = %+v, %v", got, err)
		}
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
}
