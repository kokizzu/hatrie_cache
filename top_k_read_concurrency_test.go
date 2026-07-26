package hatriecache

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var topKSingleItemJSONSink string

func TestCommandFastTopKSingleItemJSONMatchesOriginalAllocationCount(t *testing.T) {
	item := topKItem{Key: `"value"`, Value: "value", Count: 2, Error: 1}
	top := topKData{items: []topKItem{item}}
	want, err := commandFastTopKItemsJSON(top)
	if err != nil {
		t.Fatalf("commandFastTopKItemsJSON() error = %v", err)
	}
	got, err := commandFastTopKSingleItemJSON(item)
	if err != nil {
		t.Fatalf("commandFastTopKSingleItemJSON() error = %v", err)
	}
	if got != want {
		t.Fatalf("commandFastTopKSingleItemJSON() = %q, want %q", got, want)
	}

	originalAllocs := testing.AllocsPerRun(1000, func() {
		topKSingleItemJSONSink, _ = commandFastTopKItemsJSON(top)
	})
	ownedAllocs := testing.AllocsPerRun(1000, func() {
		topKSingleItemJSONSink, _ = commandFastTopKSingleItemJSON(item)
	})
	if ownedAllocs > originalAllocs {
		t.Fatalf("single-item owned encoder allocations = %.0f, original = %.0f", ownedAllocs, originalAllocs)
	}
}

func TestGenericTopKReadDoesNotHoldCacheLockDuringJSONEncoding(t *testing.T) {
	for _, itemCount := range []struct {
		name        string
		addExisting bool
	}{
		{name: "OneItem"},
		{name: "MultipleItems", addExisting: true},
	} {
		t.Run(itemCount.name, func(t *testing.T) {
			value := &blockingTopKJSONValue{
				entered: make(chan struct{}, 1),
				release: make(chan struct{}),
			}
			ht := newTestTrie(t)
			if err := ht.UpsertTopK("topk", 2); err != nil {
				t.Fatalf("UpsertTopK() error = %v", err)
			}
			if estimate, err := ht.AddTopKChecked("topk", value, 2); err != nil || !estimate.Tracked {
				t.Fatalf("AddTopKChecked(blocking value) = %#v/%v, want tracked", estimate, err)
			}
			if itemCount.addExisting {
				if estimate, err := ht.AddTopKChecked("topk", "existing", 1); err != nil || !estimate.Tracked {
					t.Fatalf("AddTopKChecked(existing) = %#v/%v, want tracked", estimate, err)
				}
			}

			value.block.Store(true)
			readDone := make(chan CacheCommandResponse, 1)
			go func() {
				readDone <- ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "topk"})
			}()
			select {
			case <-value.entered:
			case <-time.After(time.Second):
				t.Fatal("Top-K JSON marshaler was not entered")
			}

			writeDone := make(chan error, 1)
			go func() {
				_, err := ht.AddTopKChecked("topk", "writer", 1)
				writeDone <- err
			}()
			select {
			case err := <-writeDone:
				if err != nil {
					t.Fatalf("AddTopKChecked(writer) error = %v", err)
				}
			case <-time.After(100 * time.Millisecond):
				close(value.release)
				<-readDone
				<-writeDone
				t.Fatal("Top-K writer blocked behind response JSON encoding")
			}

			close(value.release)
			if response := <-readDone; !response.OK {
				t.Fatalf("ExecuteCommand(GET) = %#v, want ok", response)
			}
		})
	}
}

func BenchmarkGenericTopKReadLockScope(b *testing.B) {
	for _, size := range []int{16, 100} {
		for _, structured := range []bool{false, true} {
			kind := "Strings"
			if structured {
				kind = "Structured"
			}
			for _, order := range []struct {
				name  string
				modes []string
			}{
				{name: "LegacyFirst", modes: []string{"LegacyLocked", "SnapshotThenEncode"}},
				{name: "SnapshotFirst", modes: []string{"SnapshotThenEncode", "LegacyLocked"}},
			} {
				for _, mode := range order.modes {
					b.Run(kind+strconv.Itoa(size)+"/"+order.name+"/"+mode, func(b *testing.B) {
						ht := CreateHatTrie()
						defer ht.Destroy()
						seedTopKGenericGet(b, ht, size, structured)
						b.ReportAllocs()
						b.ResetTimer()
						for idx := 0; idx < b.N; idx++ {
							var response CacheCommandResponse
							var ok bool
							if mode == "LegacyLocked" {
								response, ok = benchmarkLegacyLockedGenericTopKRead(ht, "topk")
							} else {
								response, ok = ht.executeFastGetCommand("topk")
							}
							if !ok || !response.OK {
								b.Fatalf("Top-K GET/%s read = %#v/%v, want ok", mode, response, ok)
							}
							benchmarkCommandResponseSink = response
						}
					})
				}
			}
		}
	}
}

func benchmarkLegacyLockedGenericTopKRead(ht *HatTrie, key string) (CacheCommandResponse, bool) {
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if fallback || err != nil || !hval.IsTopK() {
		ht.mu.RUnlock()
		return CacheCommandResponse{}, false
	}
	top := ht.topKs.array[hval.Index]
	payload, err := commandFastTopKItemsJSON(top)
	ht.recordReadLocked(true, key)
	ht.mu.RUnlock()
	if err != nil {
		return commandError(err.Error()), true
	}
	return CacheCommandResponse{OK: true, Message: "ok", Value: payload}, true
}

type blockingTopKJSONValue struct {
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (value *blockingTopKJSONValue) MarshalJSON() ([]byte, error) {
	if value.block.Load() {
		select {
		case value.entered <- struct{}{}:
		default:
		}
		<-value.release
	}
	return []byte(`{"kind":"blocking"}`), nil
}
