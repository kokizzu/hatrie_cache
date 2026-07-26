package hatriecache

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestMapPeekCommandDoesNotHoldCacheLockDuringJSONEncoding(t *testing.T) {
	value := &blockingMapPeekJSONValue{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	ht := newTestTrie(t)
	if err := ht.PutMapEntriesChecked("map", Map{"field": value}); err != nil {
		t.Fatalf("PutMapEntriesChecked() error = %v", err)
	}

	value.block.Store(true)
	readDone := make(chan CacheCommandResponse, 1)
	go func() {
		readDone <- ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "map", Subkey: "field"})
	}()
	select {
	case <-value.entered:
	case <-time.After(time.Second):
		t.Fatal("map value JSON marshaler was not entered")
	}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- ht.PutMapEntriesChecked("map", Map{"field": "writer"})
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("PutMapEntriesChecked(writer) error = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(value.release)
		<-readDone
		<-writeDone
		t.Fatal("map writer blocked behind PEEKMAP JSON encoding")
	}

	close(value.release)
	if response := <-readDone; !response.OK || response.Value != `{"kind":"blocking"}` {
		t.Fatalf("ExecuteCommand(PEEKMAP) = %#v, want original point-in-time value", response)
	}
	response := ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "map", Subkey: "field"})
	if !response.OK || response.Value != "writer" {
		t.Fatalf("ExecuteCommand(PEEKMAP after write) = %#v, want writer", response)
	}
}

func BenchmarkMapPeekCommandEncoding(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		value interface{}
	}{
		{name: "String", value: "value"},
		{name: "Structured", value: Map{"nested": Slice{"value"}}},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			if err := ht.PutMapEntriesChecked("map", Map{"field": fixture.value}); err != nil {
				b.Fatalf("PutMapEntriesChecked() error = %v", err)
			}
			request := CacheCommandRequest{Command: "PEEKMAP", Key: "map", Subkey: "field"}
			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				response := ht.ExecuteCommand(request)
				if !response.OK {
					b.Fatalf("ExecuteCommand(PEEKMAP) = %#v, want ok", response)
				}
				benchmarkCommandResponseSink = response
			}
		})
	}
}

func BenchmarkMapPeekCommandLockScope(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		value interface{}
	}{
		{name: "String", value: "value"},
		{name: "Structured", value: Map{"nested": Slice{"value"}}},
	} {
		for _, order := range []struct {
			name  string
			modes []string
		}{
			{name: "LegacyFirst", modes: []string{"LegacyLocked", "SnapshotThenEncode"}},
			{name: "SnapshotFirst", modes: []string{"SnapshotThenEncode", "LegacyLocked"}},
		} {
			for _, mode := range order.modes {
				b.Run(fixture.name+"/"+order.name+"/"+mode, func(b *testing.B) {
					ht := CreateHatTrie()
					defer ht.Destroy()
					if err := ht.PutMapEntriesChecked("map", Map{"field": fixture.value}); err != nil {
						b.Fatalf("PutMapEntriesChecked() error = %v", err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for idx := 0; idx < b.N; idx++ {
						var response CacheCommandResponse
						var ok bool
						if mode == "LegacyLocked" {
							response, ok = benchmarkLegacyLockedMapPeekCommand(ht, "map", "field")
						} else {
							response, ok = ht.executeFastPeekMapCommand("map", "field")
						}
						if !ok || !response.OK {
							b.Fatalf("PEEKMAP/%s = %#v/%v, want ok", mode, response, ok)
						}
						benchmarkCommandResponseSink = response
					}
				})
			}
		}
	}
}

func benchmarkLegacyLockedMapPeekCommand(ht *HatTrie, key string, subkey string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval := ht.peekCachedLocked(key)
	if !hval.IsMap() {
		return CacheCommandResponse{}, false
	}
	value, ok := ht.maps.peek(hval.Index, subkey)
	ht.recordReadLocked(ok, key)
	if !ok {
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	if text, ok := value.(string); ok {
		return CacheCommandResponse{OK: true, Message: "ok", Value: text}, true
	}
	payload, err := commandScalarString(value)
	if err != nil {
		return commandError(err.Error()), true
	}
	return CacheCommandResponse{OK: true, Message: "ok", Value: payload}, true
}

type blockingMapPeekJSONValue struct {
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (value *blockingMapPeekJSONValue) MarshalJSON() ([]byte, error) {
	if value.block.Load() {
		select {
		case value.entered <- struct{}{}:
		default:
		}
		<-value.release
	}
	return []byte(`{"kind":"blocking"}`), nil
}
