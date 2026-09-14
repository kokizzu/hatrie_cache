package hatDictionary

import (
	"context"
	"testing"
)

func BenchmarkCH028DictionaryVersionedLookup(b *testing.B) {
	ctx := context.Background()
	source := VersionedSourceFunc(func(_ context.Context, keys []string) (map[string]string, string, error) {
		values := make(map[string]string, len(keys))
		for _, key := range keys {
			if key == "country" {
				values[key] = "Singapore"
			}
		}
		return values, "v1", nil
	})
	dictionary, err := New(source, Options{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := dictionary.RefreshAtVersion(ctx, []string{"country"}, "v1"); err != nil {
		b.Fatal(err)
	}

	b.Run("CachedUnversionedHit", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := dictionary.Lookup(ctx, "country")
			if err != nil || !result.Found || result.Value != "Singapore" {
				b.Fatalf("unversioned lookup = %#v, %v", result, err)
			}
		}
	})
	b.Run("CachedExpectedVersionHit", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := dictionary.LookupAtVersion(ctx, "country", "v1")
			if err != nil || !result.Found || result.Value != "Singapore" {
				b.Fatalf("versioned lookup = %#v, %v", result, err)
			}
		}
	})
	b.Run("DirectVersionedSource", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			values, version, err := source.LoadVersioned(ctx, []string{"country"})
			if err != nil || version != "v1" || values["country"] != "Singapore" {
				b.Fatalf("source load = %#v, %q, %v", values, version, err)
			}
		}
	})
}
