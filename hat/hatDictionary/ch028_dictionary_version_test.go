package hatDictionary

import (
	"context"
	"errors"
	"sync"
	"testing"
)

var errCH028PrimaryUnavailable = errors.New("primary dictionary unavailable")

type ch028VersionedSource struct {
	mu      sync.Mutex
	values  map[string]string
	version string
	err     error
}

func (source *ch028VersionedSource) Load(_ context.Context, keys []string) (map[string]string, error) {
	values, _, err := source.load(keys)
	return values, err
}

func (source *ch028VersionedSource) LoadVersioned(_ context.Context, keys []string) (map[string]string, string, error) {
	return source.load(keys)
}

func (source *ch028VersionedSource) load(keys []string) (map[string]string, string, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := source.values[key]; ok {
			values[key] = value
		}
	}
	return values, source.version, source.err
}

func TestCH028VersionedLookupRejectsMixedVersions(t *testing.T) {
	source := &ch028VersionedSource{values: map[string]string{"id": "one"}, version: "v1"}
	dictionary, err := New(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if refresh, err := dictionary.RefreshAtVersion(context.Background(), []string{"id"}, "v1"); err != nil || refresh.Version != "v1" {
		t.Fatalf("RefreshAtVersion(v1) = %#v, %v", refresh, err)
	}
	result, err := dictionary.LookupAtVersion(context.Background(), "id", "v1")
	if err != nil || !result.Found || result.Value != "one" || result.Version != "v1" {
		t.Fatalf("LookupAtVersion(v1) = %#v, %v", result, err)
	}

	source.mu.Lock()
	source.values["id"] = "two"
	source.version = "v2"
	source.mu.Unlock()
	refresh, err := dictionary.RefreshAtVersion(context.Background(), []string{"id"}, "v1")
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("RefreshAtVersion(v1) after source change = %#v, %v", refresh, err)
	}
	result, err = dictionary.LookupAtVersion(context.Background(), "id", "v1")
	if err != nil || !result.Found || result.Value != "one" || result.Version != "v1" {
		t.Fatalf("cached LookupAtVersion(v1) = %#v, %v", result, err)
	}
	result, err = dictionary.LookupAtVersion(context.Background(), "id", "v2")
	if err != nil || !result.Found || result.Value != "two" || result.Version != "v2" {
		t.Fatalf("LookupAtVersion(v2) = %#v, %v", result, err)
	}
}

func TestCH028ExplicitFallbackOnMissAndError(t *testing.T) {
	primary := &ch028VersionedSource{values: map[string]string{"primary": "fresh"}, version: "v1"}
	fallback := &ch028VersionedSource{values: map[string]string{"fallback": "backup", "primary": "old"}, version: "v1-fallback"}
	dictionary, err := New(primary, Options{
		Fallback:        fallback,
		FallbackOnMiss:  true,
		FallbackOnError: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := dictionary.Refresh(context.Background(), []string{"primary", "fallback"})
	if err != nil || refresh.Fallback != 1 {
		t.Fatalf("fallback Refresh() = %#v, %v", refresh, err)
	}
	result, err := dictionary.Lookup(context.Background(), "fallback")
	if err != nil || !result.Found || !result.Fallback || result.Value != "backup" || result.Version != "v1-fallback" {
		t.Fatalf("fallback miss Lookup() = %#v, %v", result, err)
	}

	primary.mu.Lock()
	primary.err = errCH028PrimaryUnavailable
	primary.mu.Unlock()
	result, err = dictionary.LookupAtVersion(context.Background(), "primary", "v1-fallback")
	if err != nil || !result.Found || !result.Fallback || result.Value != "old" {
		t.Fatalf("fallback error Lookup() = %#v, %v", result, err)
	}
}

func TestCH028FallbackIsOptInAndRequiresAConfiguredSource(t *testing.T) {
	primary := &ch028VersionedSource{values: map[string]string{}, version: "v1"}
	if _, err := New(primary, Options{FallbackOnMiss: true}); !errors.Is(err, ErrOptionsInvalid) {
		t.Fatalf("missing fallback source error = %v, want ErrOptionsInvalid", err)
	}
	fallback := &ch028VersionedSource{values: map[string]string{"id": "fallback"}, version: "fallback"}
	dictionary, err := New(primary, Options{Fallback: fallback})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dictionary.Lookup(context.Background(), "id")
	if err != nil || result.Found {
		t.Fatalf("disabled fallback Lookup() = %#v, %v", result, err)
	}
}
