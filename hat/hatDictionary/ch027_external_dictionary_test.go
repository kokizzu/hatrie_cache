package hatDictionary

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

var errCH027SourceUnavailable = errors.New("source unavailable")

type ch027Source struct {
	mu     sync.Mutex
	values map[string]string
	err    error
	loads  [][]string
}

func (source *ch027Source) Load(_ context.Context, keys []string) (map[string]string, error) {
	source.mu.Lock()
	source.loads = append(source.loads, append([]string(nil), keys...))
	err := source.err
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := source.values[key]; ok {
			values[key] = value
		}
	}
	source.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return values, nil
}

func TestCH027RefreshLimitAndBoundedLRUEviction(t *testing.T) {
	source := &ch027Source{values: map[string]string{
		"a": "one",
		"b": "two",
		"c": "three",
	}}
	dictionary, err := New(source, Options{
		MaxEntries:     2,
		MaxBytes:       100,
		MaxRefreshKeys: 2,
		TTL:            time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dictionary.Refresh(context.Background(), []string{"a", "b", "c"}); !errors.Is(err, ErrRefreshLimit) {
		t.Fatalf("oversized Refresh() error = %v, want ErrRefreshLimit", err)
	}
	if _, err := dictionary.Refresh(context.Background(), []string{"a", "b"}); err != nil {
		t.Fatalf("initial Refresh() error = %v", err)
	}
	if result, err := dictionary.Lookup(context.Background(), "a"); err != nil || !result.Found || result.Value != "one" {
		t.Fatalf("Lookup(a) = %#v, %v", result, err)
	}
	refresh, err := dictionary.Refresh(context.Background(), []string{"c"})
	if err != nil {
		t.Fatalf("evicting Refresh() error = %v", err)
	}
	if refresh.Evicted != 1 || dictionary.Len() != 2 {
		t.Fatalf("eviction result = %#v, len=%d", refresh, dictionary.Len())
	}
	if result, err := dictionary.Lookup(context.Background(), "a"); err != nil || !result.Found || result.Value != "one" {
		t.Fatalf("retained Lookup(a) = %#v, %v", result, err)
	}
	if result, err := dictionary.Lookup(context.Background(), "b"); err != nil || !result.Found || result.Value != "two" {
		t.Fatalf("evicted Lookup(b) = %#v, %v; want a source reload", result, err)
	}
	source.mu.Lock()
	loads := append([][]string(nil), source.loads...)
	source.mu.Unlock()
	if !reflect.DeepEqual(loads, [][]string{{"a", "b"}, {"c"}, {"b"}}) {
		t.Fatalf("source loads = %#v", loads)
	}
}

func TestCH027LookupReturnsStaleValueWithSourceError(t *testing.T) {
	now := time.Unix(100, 0)
	source := &ch027Source{values: map[string]string{"id": "old"}}
	dictionary, err := New(source, Options{
		TTL:          10 * time.Second,
		StaleIfError: true,
		Now:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dictionary.Refresh(context.Background(), []string{"id"}); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	now = now.Add(11 * time.Second)
	source.err = errCH027SourceUnavailable
	result, err := dictionary.Lookup(context.Background(), "id")
	if !errors.Is(err, ErrStale) || !errors.Is(err, errCH027SourceUnavailable) {
		t.Fatalf("stale Lookup() error = %v, want ErrStale and source error", err)
	}
	if !result.Found || !result.Stale || result.Value != "old" {
		t.Fatalf("stale Lookup() result = %#v", result)
	}
}

func TestCH027LookupRejectsInvalidKeysAndMissingValues(t *testing.T) {
	source := &ch027Source{values: map[string]string{}}
	dictionary, err := New(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", " ", "bad\x00key"} {
		if _, err := dictionary.Lookup(context.Background(), key); !errors.Is(err, ErrKeyInvalid) {
			t.Fatalf("Lookup(%q) error = %v, want ErrKeyInvalid", key, err)
		}
	}
	result, err := dictionary.Lookup(context.Background(), "missing")
	if err != nil || result.Found {
		t.Fatalf("missing Lookup() = %#v, %v", result, err)
	}
}
