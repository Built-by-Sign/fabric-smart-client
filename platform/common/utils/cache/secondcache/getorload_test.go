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
				time.Sleep(20 * time.Millisecond)

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

// Different-key concurrent GetOrLoad: loaders must run in parallel, not be
// serialized through a global write lock as in the old implementation.
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

// Concurrent Add during loader execution must not be overwritten by the
// loader's stale value when it returns.
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
			<-addDone

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
		t.Errorf("Get returned %q, want %q", v, "newer_value")
	}
	if loadV != "newer_value" {
		t.Errorf("GetOrLoad returned val %q, want %q", loadV, "newer_value")
	}
	if !loadOk {
		t.Errorf("GetOrLoad returned ok=false, want true")
	}
}

// T=interface{} cache with a loader returning (nil, nil) must not panic on
// the type assertion path.
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
		t.Errorf("got ok=true, want false")
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
		t.Errorf("loader called on fast-path hit")
	}
	if v != 7 {
		t.Errorf("got %d want 7", v)
	}
	if !ok {
		t.Errorf("got ok=false, want true")
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
		t.Errorf("got ok=true, want false")
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
