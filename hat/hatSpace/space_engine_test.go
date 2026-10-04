package hatSpace_test

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"hatrie_cache/hat/hatSpace"
)

func TestSpaceEngineDefaultsToMemtxAndExposesProfile(t *testing.T) {
	engine, err := hatSpace.Open(hatSpace.Options{})
	if err != nil {
		t.Fatalf("Open(default): %v", err)
	}
	defer engine.Close()
	if engine.Kind() != hatSpace.EngineMemtx {
		t.Fatalf("default engine = %q, want memtx", engine.Kind())
	}
	profile := engine.Profile()
	if profile.Kind != hatSpace.EngineMemtx || profile.Durable || profile.SupportsCompaction {
		t.Fatalf("default profile = %#v, want volatile non-compacting memtx", profile)
	}
}

func TestSpaceEngineVinylRoundTripReopenAndCompact(t *testing.T) {
	directory := t.TempDir()
	engine, err := hatSpace.Open(hatSpace.Options{
		Kind:             hatSpace.EngineVinyl,
		Directory:        directory,
		MemoryLimitBytes: 8,
		MaxDiskBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open(vinyl): %v", err)
	}
	if engine.Profile().Kind != hatSpace.EngineVinyl || !engine.Profile().Durable || !engine.Profile().SupportsCompaction {
		t.Fatalf("vinyl profile = %#v, want durable compacting vinyl", engine.Profile())
	}
	for _, entry := range []struct {
		key   string
		value string
	}{
		{key: "a", value: "alpha"},
		{key: "b", value: "bravo"},
		{key: "c", value: "charlie"},
	} {
		if err := engine.Set(entry.key, []byte(entry.value)); err != nil {
			t.Fatalf("Set(%q): %v", entry.key, err)
		}
	}
	path := engine.Path()
	if path == "" {
		t.Fatal("vinyl Path() is empty")
	}
	if got := engine.Stats(); got.Entries != 3 || got.ColdEntries == 0 || got.HotBytes > 8 {
		t.Fatalf("vinyl stats after writes = %#v, want spilled entries and hot-byte bound", got)
	}
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	reopened, err := hatSpace.Open(hatSpace.Options{
		Kind:             hatSpace.EngineVinyl,
		Path:             path,
		MemoryLimitBytes: 8,
		MaxDiskBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("Open(reopen): %v", err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{"a": "alpha", "b": "bravo", "c": "charlie"} {
		got, found, err := reopened.Get(key)
		if err != nil || !found || string(got) != want {
			t.Fatalf("Get(%q) = %q/%t/%v, want %q/true/nil", key, got, found, err, want)
		}
	}
	entries, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"a", "b", "c"}) {
		t.Fatalf("Snapshot keys = %#v, want sorted entries", keys)
	}
	before := reopened.Stats()
	if !reopened.Delete("b") || reopened.Delete("missing") {
		t.Fatal("Delete() results mismatch")
	}
	if err := reopened.Compact(); err != nil {
		t.Fatalf("Compact(): %v", err)
	}
	after := reopened.Stats()
	if after.Entries != 2 || after.DiskBytes >= before.DiskBytes {
		t.Fatalf("stats after delete/compact = %#v, before=%#v", after, before)
	}
}

func TestSpaceEngineIsolatesValuesAndRejectsLimitsAtomically(t *testing.T) {
	engine, err := hatSpace.Open(hatSpace.Options{Kind: hatSpace.EngineMemtx, MemoryLimitBytes: 7})
	if err != nil {
		t.Fatalf("Open(memtx): %v", err)
	}
	defer engine.Close()
	value := []byte("data")
	if err := engine.Set("key", value); err != nil {
		t.Fatalf("Set(valid): %v", err)
	}
	value[0] = 'X'
	got, found, err := engine.Get("key")
	if err != nil || !found || string(got) != "data" {
		t.Fatalf("Get(after caller mutation) = %q/%t/%v, want data/true/nil", got, found, err)
	}
	got[0] = 'Y'
	again, _, err := engine.Get("key")
	if err != nil || string(again) != "data" {
		t.Fatalf("Get(after returned mutation) = %q/%v, want data/nil", again, err)
	}
	if err := engine.Set("other", []byte("x")); !errors.Is(err, hatSpace.ErrSpaceEngineMemoryLimit) {
		t.Fatalf("Set(over limit) = %v, want memory limit", err)
	}
	if got := engine.Stats(); got.Entries != 1 || got.HotBytes != 7 {
		t.Fatalf("state after rejected Set = %#v, want original entry", got)
	}
	if err := engine.Set("", []byte("x")); !errors.Is(err, hatSpace.ErrSpaceEngineKeyRequired) {
		t.Fatalf("Set(empty key) = %v, want key error", err)
	}
}

func TestSpaceEngineRejectsUnknownKindAndInvalidVinylPath(t *testing.T) {
	if _, err := hatSpace.Open(hatSpace.Options{Kind: hatSpace.EngineKind("unknown")}); !errors.Is(err, hatSpace.ErrSpaceEngineKindInvalid) {
		t.Fatalf("unknown kind error = %v, want invalid kind", err)
	}
	if _, err := hatSpace.Open(hatSpace.Options{Kind: hatSpace.EngineVinyl, Path: t.TempDir()}); !errors.Is(err, hatSpace.ErrSpaceEnginePathInvalid) {
		t.Fatalf("directory path error = %v, want invalid path", err)
	}
}

func BenchmarkSpaceEngineSetGet(b *testing.B) {
	for _, kind := range []hatSpace.EngineKind{hatSpace.EngineMemtx, hatSpace.EngineVinyl} {
		b.Run(string(kind), func(b *testing.B) {
			options := hatSpace.Options{Kind: kind, MemoryLimitBytes: 1 << 20, MaxDiskBytes: 64 << 20}
			if kind == hatSpace.EngineVinyl {
				options.Directory = b.TempDir()
			}
			engine, err := hatSpace.Open(options)
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()
			value := []byte("value-0123456789")
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				key := "key-" + string(rune('a'+index%26))
				if err := engine.Set(key, value); err != nil {
					b.Fatal(err)
				}
				if _, _, err := engine.Get(key); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSpaceEngineLookup4096(b *testing.B) {
	const rows = 4096
	value := []byte("payload-0123456789abcdef0123456789abcdef")
	for _, kind := range []hatSpace.EngineKind{hatSpace.EngineMemtx, hatSpace.EngineVinyl} {
		b.Run(string(kind), func(b *testing.B) {
			options := hatSpace.Options{Kind: kind, MemoryLimitBytes: 1 << 30, MaxDiskBytes: 64 << 20}
			if kind == hatSpace.EngineVinyl {
				options.Directory = b.TempDir()
				options.MemoryLimitBytes = 64 << 10
			}
			engine, err := hatSpace.Open(options)
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()
			for row := 0; row < rows; row++ {
				if err := engine.Set("key-"+string(rune(row)), value); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, found, err := engine.Get("key-" + string(rune(iteration%rows))); err != nil || !found {
					b.Fatalf("Get() = found %t err %v, want true/nil", found, err)
				}
			}
			b.StopTimer()
			stats := engine.Stats()
			b.ReportMetric(float64(stats.HotBytes), "hot-bytes")
			b.ReportMetric(float64(stats.DiskBytes), "disk-bytes")
			b.ReportMetric(float64(stats.Entries), "entries")
		})
	}
}
