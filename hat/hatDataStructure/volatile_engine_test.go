package hatDataStructure

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestVolatileEngineCopiesValuesAndExpires(t *testing.T) {
	now := time.Unix(100, 0)
	engine, err := NewVolatileEngine(VolatileEngineOptions{
		MaxEntries:    4,
		MaxBytes:      128,
		MaxKeyBytes:   16,
		MaxValueBytes: 32,
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewVolatileEngine() error = %v", err)
	}

	input := []byte("one")
	if err := engine.Set("key", input, 10*time.Second); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	input[0] = 'X'
	got, ok := engine.Get("key")
	if !ok || string(got) != "one" {
		t.Fatalf("Get() = %q, %v, want one, true", got, ok)
	}
	got[0] = 'X'
	if got, ok := engine.Get("key"); !ok || string(got) != "one" {
		t.Fatalf("Get() after caller mutation = %q, %v, want one, true", got, ok)
	}

	destination := make([]byte, 1, 8)
	destination, ok = engine.GetInto("key", destination)
	if !ok || string(destination) != "one" {
		t.Fatalf("GetInto() = %q, %v, want one, true", destination, ok)
	}
	now = now.Add(11 * time.Second)
	if _, ok := engine.Get("key"); ok {
		t.Fatal("expired Get() succeeded")
	}
	stats := engine.Stats()
	if stats.Entries != 0 || stats.Bytes != 0 || stats.Hits != 3 || stats.Misses != 1 || stats.Expired != 1 {
		t.Fatalf("Stats() = %#v, want one expired entry and three hits", stats)
	}
}

func TestVolatileEngineRejectsAndEvictsByExplicitPolicy(t *testing.T) {
	reject, err := NewVolatileEngine(VolatileEngineOptions{MaxEntries: 2, MaxBytes: 64})
	if err != nil {
		t.Fatalf("NewVolatileEngine(reject) error = %v", err)
	}
	if err := reject.Set("a", []byte("1"), 0); err != nil {
		t.Fatalf("reject Set(a) error = %v", err)
	}
	if err := reject.Set("b", []byte("2"), 0); err != nil {
		t.Fatalf("reject Set(b) error = %v", err)
	}
	if err := reject.Set("c", []byte("3"), 0); !errors.Is(err, ErrVolatileEngineCapacity) {
		t.Fatalf("reject Set(c) error = %v, want ErrVolatileEngineCapacity", err)
	}
	if _, ok := reject.Get("a"); !ok {
		t.Fatal("reject policy evicted a")
	}
	if stats := reject.Stats(); stats.Evictions != 0 || stats.SetRejects != 1 {
		t.Fatalf("reject stats = %#v, want one rejected set and no eviction", stats)
	}

	evict, err := NewVolatileEngine(VolatileEngineOptions{
		MaxEntries:     2,
		MaxBytes:       64,
		EvictionPolicy: VolatileEvictionOldest,
	})
	if err != nil {
		t.Fatalf("NewVolatileEngine(evict) error = %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if err := evict.Set(key, []byte(key), 0); err != nil {
			t.Fatalf("evict Set(%s) error = %v", key, err)
		}
	}
	if _, ok := evict.Get("a"); ok {
		t.Fatal("oldest entry a remains after eviction")
	}
	if _, ok := evict.Get("b"); !ok {
		t.Fatal("entry b missing after eviction")
	}
	if stats := evict.Stats(); stats.Entries != 2 || stats.Evictions != 1 {
		t.Fatalf("evict stats = %#v, want two entries and one eviction", stats)
	}
}

func TestVolatileEngineRejectsInvalidOptionsAndInputs(t *testing.T) {
	for name, options := range map[string]VolatileEngineOptions{
		"negative entries":   {MaxEntries: -1},
		"negative key bytes": {MaxKeyBytes: -1},
		"invalid eviction":   {EvictionPolicy: VolatileEvictionPolicy(99)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewVolatileEngine(options); !errors.Is(err, ErrVolatileEngineOptionsInvalid) {
				t.Fatalf("NewVolatileEngine() error = %v, want ErrVolatileEngineOptionsInvalid", err)
			}
		})
	}

	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxKeyBytes: 2, MaxValueBytes: 2})
	if err != nil {
		t.Fatalf("NewVolatileEngine() error = %v", err)
	}
	tests := []struct {
		name  string
		key   string
		value []byte
		ttl   time.Duration
		want  error
	}{
		{name: "empty key", want: ErrVolatileEngineKeyEmpty},
		{name: "long key", key: "abc", want: ErrVolatileEngineKeyTooLong},
		{name: "large value", key: "ok", value: []byte("123"), want: ErrVolatileEngineValueTooLarge},
		{name: "negative ttl", key: "ok", value: []byte("1"), ttl: -time.Nanosecond, want: ErrVolatileEngineTTLInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := engine.Set(test.key, test.value, test.ttl); !errors.Is(err, test.want) {
				t.Fatalf("Set() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestVolatileEnginePurgeAndClear(t *testing.T) {
	now := time.Unix(200, 0)
	engine, err := NewVolatileEngine(VolatileEngineOptions{
		MaxEntries: 2,
		MaxBytes:   8,
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewVolatileEngine() error = %v", err)
	}
	if err := engine.Set("a", []byte("one"), time.Second); err != nil {
		t.Fatalf("Set(a) error = %v", err)
	}
	if err := engine.Set("b", []byte("two"), 0); err != nil {
		t.Fatalf("Set(b) error = %v", err)
	}
	now = now.Add(2 * time.Second)
	if got := engine.Len(); got != 1 {
		t.Fatalf("Len() = %d, want one live entry", got)
	}
	if stats := engine.Stats(); stats.Expired != 1 || stats.Entries != 1 {
		t.Fatalf("Stats() after lazy purge = %#v, want one expiry", stats)
	}
	if err := engine.Set("b", []byte("x"), 0); err != nil {
		t.Fatalf("replacement Set(b) error = %v", err)
	}
	if !engine.Delete("b") {
		t.Fatal("Delete(b) = false, want true")
	}
	if got := engine.Clear(); got != 0 {
		t.Fatalf("Clear() = %d after delete, want zero", got)
	}
}

func TestVolatileEngineConcurrentAccess(t *testing.T) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxEntries: 128, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewVolatileEngine() error = %v", err)
	}
	const workers = 8
	const operations = 250
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer group.Done()
			key := "key-" + string(rune('a'+worker))
			for i := 0; i < operations; i++ {
				if err := engine.Set(key, []byte("value"), 0); err != nil {
					t.Errorf("Set() error = %v", err)
					return
				}
				buffer, _ := engine.GetInto(key, make([]byte, 0, 8))
				if string(buffer) != "value" {
					t.Errorf("GetInto() = %q, want value", buffer)
					return
				}
			}
		}(worker)
	}
	group.Wait()
	if got := engine.Stats().Entries; got != workers {
		t.Fatalf("Entries = %d, want %d", got, workers)
	}
}
