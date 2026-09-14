package hatDictionary

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCH027DictionaryLookup(b *testing.B) {
	ctx := context.Background()
	source := SourceFunc(func(_ context.Context, keys []string) (map[string]string, error) {
		values := make(map[string]string, len(keys))
		for _, key := range keys {
			if key == "country" {
				values[key] = "Singapore"
			}
		}
		return values, nil
	})
	dictionary, err := New(source, Options{TTL: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := dictionary.Refresh(ctx, []string{"country"}); err != nil {
		b.Fatal(err)
	}

	b.Run("DirectSource", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			values, err := source.Load(ctx, []string{"country"})
			if err != nil || values["country"] != "Singapore" {
				b.Fatalf("source load = %#v, %v", values, err)
			}
		}
	})
	b.Run("DirectSourceParallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				values, err := source.Load(ctx, []string{"country"})
				if err != nil || values["country"] != "Singapore" {
					b.Fatalf("parallel source load = %#v, %v", values, err)
				}
			}
		})
	})
	b.Run("CachedHit", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(dictionary.Bytes()), "retained_bytes")
		for range b.N {
			result, err := dictionary.Lookup(ctx, "country")
			if err != nil || !result.Found || result.Value != "Singapore" {
				b.Fatalf("cached lookup = %#v, %v", result, err)
			}
		}
	})
	b.Run("CachedHitParallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				result, err := dictionary.Lookup(ctx, "country")
				if err != nil || !result.Found || result.Value != "Singapore" {
					b.Fatalf("parallel cached lookup = %#v, %v", result, err)
				}
			}
		})
	})
}
