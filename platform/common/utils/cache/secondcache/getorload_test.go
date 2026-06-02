/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package secondcache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Same-key concurrent GetOrLoad: loader must run once (singleflight dedup),
// all callers must observe the same value, and the dedup'd followers must
// see fromCache=false consistent with the leader (loader actually ran).
func TestGetOrLoadSingleflightSameKey(t *testing.T) {
	cache := NewTyped[int](100)
	var loaderCount int32
	const N = 100

	var wg sync.WaitGroup
	wg.Add(N)
	barrier := make(chan struct{})

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-barrier
			v, _, err := cache.GetOrLoad("k", func() (int, error) {
				atomic.AddInt32(&loaderCount, 1)
				time.Sleep(20 * time.Millisecond) // hold so others queue at sf
				return 42, nil
			})
			if err != nil {
				t.Errorf("err: %v", err)
			}
			if v != 42 {
				t.Errorf("got %d want 42", v)
			}
		}()
	}
	close(barrier)
	wg.Wait()

	if got := atomic.LoadInt32(&loaderCount); got != 1 {
		t.Errorf("loader ran %d times, want 1", got)
	}
}

// Different-key concurrent GetOrLoad: loaders must run in parallel (not be
// serialized through a global write lock as in the pre-PR-3 implementation).
func TestGetOrLoadDifferentKeysParallel(t *testing.T) {
	cache := NewTyped[int](100)
	const N = 8
	var inFlight, peakInFlight int32

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			_, _, err := cache.GetOrLoad(fmt.Sprintf("k%d", i), func() (int, error) {
				cur := atomic.AddInt32(&inFlight, 1)
				for {
					p := atomic.LoadInt32(&peakInFlight)
					if cur <= p || atomic.CompareAndSwapInt32(&peakInFlight, p, cur) {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				atomic.AddInt32(&inFlight, -1)
				return i, nil
			})
			if err != nil {
				t.Errorf("err: %v", err)
			}
		}()
	}
	wg.Wait()

	if peakInFlight < 2 {
		t.Errorf("peak in-flight loaders = %d, expected concurrent execution", peakInFlight)
	}
}

// Concurrent Add during loader execution must NOT be overwritten by the
// loader's stale value when it returns. This is the bug from PR review
// issue 1: without a final recheck under wlock, the loader's `cache.add`
// would clobber the freshly-Add'd value.
func TestGetOrLoadConcurrentAddNotOverwritten(t *testing.T) {
	cache := NewTyped[string](100)
	loaderStarted := make(chan struct{})
	addDone := make(chan struct{})

	var loadV string
	var loadOk bool
	loadDone := make(chan struct{})
	go func() {
		v, ok, err := cache.GetOrLoad("k", func() (string, error) {
			close(loaderStarted)
			<-addDone // wait for the Add call to land
			return "loader_value", nil
		})
		if err != nil {
			t.Errorf("err: %v", err)
		}
		loadV = v
		loadOk = ok
		close(loadDone)
	}()

	<-loaderStarted
	cache.Add("k", "newer_value")
	close(addDone)
	<-loadDone

	v, ok := cache.Get("k")
	if !ok {
		t.Fatal("expected key to be present")
	}
	if v != "newer_value" {
		t.Errorf("Get returned %q, want %q (loader stale value overwrote concurrent Add)", v, "newer_value")
	}
	// GetOrLoad should reflect the cached value (newer_value), with
	// fromCache=true because the final recheck found the Add.
	if loadV != "newer_value" {
		t.Errorf("GetOrLoad returned val %q, want %q", loadV, "newer_value")
	}
	if !loadOk {
		t.Errorf("GetOrLoad returned ok=false, want true (recheck hit)")
	}
}

// T=interface{} cache with a loader returning (nil, nil) must not panic on
// the type assertion path. This is PR review issue 2.
func TestGetOrLoadInterfaceNilValueNoPanic(t *testing.T) {
	cache := New(100)

	v, ok, err := cache.GetOrLoad("k", func() (interface{}, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if v != nil {
		t.Errorf("got %v want nil", v)
	}
	if ok {
		t.Errorf("got ok=true, want false (loader was invoked)")
	}
}

// ok-semantic check: fast-path RLock hit returns ok=true.
func TestGetOrLoadFastPathOk(t *testing.T) {
	cache := NewTyped[int](100)
	cache.Add("k", 7)

	loaderCalled := false
	v, ok, err := cache.GetOrLoad("k", func() (int, error) {
		loaderCalled = true
		return 99, nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if loaderCalled {
		t.Errorf("loader called on fast-path hit, expected dedup")
	}
	if v != 7 {
		t.Errorf("got %d want 7", v)
	}
	if !ok {
		t.Errorf("got ok=false, want true (fast-path hit)")
	}
}

// ok-semantic check: cold load (no concurrent races) returns ok=false.
func TestGetOrLoadColdLoadOk(t *testing.T) {
	cache := NewTyped[int](100)

	v, ok, err := cache.GetOrLoad("k", func() (int, error) {
		return 11, nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if v != 11 {
		t.Errorf("got %d want 11", v)
	}
	if ok {
		t.Errorf("got ok=true, want false (loader actually ran)")
	}
}

// Loader error must propagate; subsequent identical-key call may retry.
func TestGetOrLoadErrorPropagates(t *testing.T) {
	cache := NewTyped[int](100)
	wantErr := errors.New("loader failed")

	_, _, err := cache.GetOrLoad("k", func() (int, error) {
		return 0, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("got err %v, want %v", err, wantErr)
	}

	// After error, key should still not be cached; next call retries.
	var retried bool
	v, _, err := cache.GetOrLoad("k", func() (int, error) {
		retried = true
		return 5, nil
	})
	if err != nil {
		t.Fatalf("retry err: %v", err)
	}
	if !retried {
		t.Errorf("loader not retried after prior error")
	}
	if v != 5 {
		t.Errorf("got %d want 5", v)
	}
}

// A slow loader for a cold key must not block concurrent Get hits on other
// keys: the loader runs outside the cache lock.
func TestGetOrLoadMissDoesNotBlockHits(t *testing.T) {
	t.Parallel()
	cache := NewTyped[int](25)
	cache.Add("hot-key", 7)

	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	loadDone := make(chan error)
	go func() {
		_, _, err := cache.GetOrLoad("cold-key", func() (int, error) {
			close(loaderStarted)
			<-releaseLoader

			return 9, nil
		})
		loadDone <- err
	}()

	select {
	case <-loaderStarted:
	case <-time.After(time.Second):
		t.Fatal("loader was not called")
	}

	hitDone := make(chan error)
	go func() {
		val, ok := cache.Get("hot-key")
		if !ok {
			hitDone <- errors.New("hot key was not found")

			return
		}
		if val != 7 {
			hitDone <- fmt.Errorf("expected hot key value 7, got %d", val)

			return
		}
		hitDone <- nil
	}()

	select {
	case err := <-hitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cache hit blocked behind a miss loader")
	}

	close(releaseLoader)
	if err := <-loadDone; err != nil {
		t.Fatalf("loader returned error: %v", err)
	}
}
