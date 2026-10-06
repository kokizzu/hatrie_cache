//go:build tu18

package hatCache

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestVolatileEngineRequiresAnExplicitBound(t *testing.T) {
	if _, err := NewVolatileEngine(VolatileEngineOptions{}); !errors.Is(err, ErrVolatileEngineInvalidOptions) {
		t.Fatalf("NewVolatileEngine() error = %v, want invalid options", err)
	}
}

func TestVolatileEngineIsMemoryOnlyAndEnforcesFIFOAndTTL(t *testing.T) {
	now := time.Unix(100, 0)
	engine, err := NewVolatileEngine(VolatileEngineOptions{
		MaxBytes:   256,
		MaxEntries: 2,
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if engine.Backend() != VolatileEngineBackend || engine.Durable() || engine.Path() != "" {
		t.Fatalf("volatile identity = backend=%q durable=%v path=%q", engine.Backend(), engine.Durable(), engine.Path())
	}
	if err := engine.SetBytes("a", []byte("one"), 0); err != nil {
		t.Fatal(err)
	}
	if err := engine.SetBytes("b", []byte("two"), 0); err != nil {
		t.Fatal(err)
	}
	if err := engine.SetBytes("c", []byte("three"), 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := engine.GetBytes("a"); err != nil || ok {
		t.Fatalf("FIFO victim = (%v, %v), want missing", ok, err)
	}
	if got, ok, err := engine.GetBytes("b"); err != nil || !ok || string(got) != "two" {
		t.Fatalf("surviving value = (%q, %v, %v), want two/present", got, ok, err)
	}

	if err := engine.SetBytes("ttl", []byte("soon"), time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, ok, err := engine.GetBytes("ttl"); err != nil || ok {
		t.Fatalf("expired value = (%v, %v), want missing", ok, err)
	}
	stats := engine.Stats()
	if stats.Entries != 1 || stats.Evictions != 2 || stats.Expirations != 1 || stats.ResidentBytes == 0 {
		t.Fatalf("volatile stats = %#v, want bounded resident state", stats)
	}
}

func TestVolatileEngineRejectsAnOversizedEntryWithoutMutation(t *testing.T) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxBytes: 70})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if err := engine.SetBytes("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := engine.SetBytes("large", []byte("0123456789"), 0); !errors.Is(err, ErrVolatileEntryTooLarge) {
		t.Fatalf("oversized SetBytes() error = %v, want entry-too-large", err)
	}
	if got, ok, err := engine.GetBytes("k"); err != nil || !ok || string(got) != "v" {
		t.Fatalf("original value = (%q, %v, %v), want v/present", got, ok, err)
	}
}

func TestVolatileEngineReplacementDoesNotRetainStaleFIFORecords(t *testing.T) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	for index := 0; index < 4096; index++ {
		if err := engine.SetBytes("same", []byte("value"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if len(engine.queue) != 1 || engine.queueHead != 0 {
		t.Fatalf("replacement queue = len %d/head %d, want one live record", len(engine.queue), engine.queueHead)
	}
}

func TestVolatileEngineCloseRejectsFurtherAccess(t *testing.T) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.SetBytes("closed", []byte("value"), 0); !errors.Is(err, ErrVolatileEngineClosed) {
		t.Fatalf("SetBytes after Close() error = %v", err)
	}
	if _, _, err := engine.GetBytes("closed"); !errors.Is(err, ErrVolatileEngineClosed) {
		t.Fatalf("GetBytes after Close() error = %v", err)
	}
	if stats := engine.Stats(); !stats.Closed || stats.Entries != 0 || stats.ResidentBytes != 0 {
		t.Fatalf("closed stats = %#v", stats)
	}
}

func TestVolatileEngineConcurrentAccess(t *testing.T) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxBytes: 1 << 20, MaxEntries: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 128; index++ {
				key := "worker:" + string(rune('a'+worker)) + ":" + string(rune('a'+index%26))
				if err := engine.SetBytes(key, []byte("value"), 0); err != nil {
					t.Errorf("SetBytes(%q): %v", key, err)
					return
				}
				if _, _, err := engine.GetBytes(key); err != nil {
					t.Errorf("GetBytes(%q): %v", key, err)
					return
				}
			}
		}()
	}
	group.Wait()
}
