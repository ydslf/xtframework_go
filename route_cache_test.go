package xtframework

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestServiceRouteCacheReuseAndInvalidate(t *testing.T) {
	cache := newServiceRouteCache(time.Minute)
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
	cache := newServiceRouteCache(time.Minute)
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
