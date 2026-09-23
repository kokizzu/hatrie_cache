package hatDictionary

import (
	"context"
	"sync"
	"testing"
	"time"
)

type ch046Source struct {
	mu      sync.Mutex
	values  map[string]string
	version string
	loads   int
}

func (source *ch046Source) Load(_ context.Context, keys []string) (map[string]string, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.loads++
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, found := source.values[key]; found {
			values[key] = value
		}
	}
	return values, nil
}

func (source *ch046Source) LoadVersioned(ctx context.Context, keys []string) (map[string]string, string, error) {
	values, err := source.Load(ctx, keys)
	source.mu.Lock()
	defer source.mu.Unlock()
	return values, source.version, err
}

func (source *ch046Source) loadCount() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.loads
}

func TestCH046NegativeCacheSuppressesMissesAndExpires(t *testing.T) {
	now := time.Unix(100, 0)
	source := &ch046Source{values: map[string]string{}}
	dictionary, err := New(source, Options{
		NegativeTTL:        10 * time.Second,
		MaxNegativeEntries: 4,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := dictionary.Lookup(context.Background(), "missing"); err != nil || result.Found {
		t.Fatalf("first missing lookup = %#v, %v", result, err)
	}
	if result, err := dictionary.Lookup(context.Background(), "missing"); err != nil || result.Found {
		t.Fatalf("negative cached lookup = %#v, %v", result, err)
	}
	if source.loadCount() != 1 {
		t.Fatalf("source loads before expiry = %d, want 1", source.loadCount())
	}
	stats := dictionary.Stats()
	if stats.NegativeEntries != 1 || stats.NegativeHits != 1 {
		t.Fatalf("negative stats = %#v, want one entry and one hit", stats)
	}

	now = now.Add(11 * time.Second)
	source.mu.Lock()
	source.values["missing"] = "now-present"
	source.mu.Unlock()
	result, err := dictionary.Lookup(context.Background(), "missing")
	if err != nil || !result.Found || result.Value != "now-present" {
		t.Fatalf("expired negative lookup = %#v, %v", result, err)
	}
	if source.loadCount() != 2 {
		t.Fatalf("source loads after expiry = %d, want 2", source.loadCount())
	}
}

func TestCH046NegativeCacheIsBoundedAndDefaultOff(t *testing.T) {
	source := &ch046Source{values: map[string]string{}}
	dictionary, err := New(source, Options{NegativeTTL: time.Hour, MaxNegativeEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dictionary.Refresh(context.Background(), []string{"a", "b", "c"}); err != nil {
		t.Fatalf("negative refresh = %v", err)
	}
	stats := dictionary.Stats()
	if stats.NegativeEntries != 2 || dictionary.Len() != 2 {
		t.Fatalf("bounded negative state = %#v, len=%d", stats, dictionary.Len())
	}
	loads := source.loadCount()
	if _, err := dictionary.Lookup(context.Background(), "a"); err != nil {
		t.Fatalf("evicted negative lookup = %v", err)
	}
	if source.loadCount() != loads+1 {
		t.Fatalf("evicted negative source loads = %d, want %d", source.loadCount(), loads+1)
	}

	defaultDictionary, err := New(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := defaultDictionary.Lookup(context.Background(), "never"); err != nil {
		t.Fatal(err)
	}
	if _, err := defaultDictionary.Lookup(context.Background(), "never"); err != nil {
		t.Fatal(err)
	}
	if defaultDictionary.Stats().NegativeEntries != 0 || source.loadCount() != loads+3 {
		t.Fatalf("default negative cache state = %#v, source loads=%d", defaultDictionary.Stats(), source.loadCount())
	}
}

func TestCH046EmptyStringRemainsPositiveAndVersionedNegativeIsScoped(t *testing.T) {
	source := &ch046Source{values: map[string]string{"empty": ""}, version: "v1"}
	dictionary, err := New(source, Options{NegativeTTL: time.Hour, MaxNegativeEntries: 4})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dictionary.Lookup(context.Background(), "empty")
	if err != nil || !result.Found || result.Value != "" {
		t.Fatalf("empty value lookup = %#v, %v", result, err)
	}
	if dictionary.Stats().NegativeEntries != 0 {
		t.Fatalf("empty value became negative: %#v", dictionary.Stats())
	}

	versioned, ok := any(source).(VersionedSource)
	if !ok {
		t.Fatal("source does not implement VersionedSource")
	}
	_ = versioned
	if result, err := dictionary.LookupAtVersion(context.Background(), "missing", "v1"); err != nil || result.Found {
		t.Fatalf("versioned negative lookup = %#v, %v", result, err)
	}
	loads := source.loadCount()
	if result, err := dictionary.LookupAtVersion(context.Background(), "missing", "v1"); err != nil || result.Found {
		t.Fatalf("versioned negative hit = %#v, %v", result, err)
	}
	if source.loadCount() != loads {
		t.Fatalf("same-version negative refreshed source: %d != %d", source.loadCount(), loads)
	}
	source.mu.Lock()
	source.version = "v2"
	source.values["missing"] = "v2-value"
	source.mu.Unlock()
	result, err = dictionary.LookupAtVersion(context.Background(), "missing", "v2")
	if err != nil || !result.Found || result.Value != "v2-value" || result.Version != "v2" {
		t.Fatalf("new-version negative invalidation = %#v, %v", result, err)
	}
}
