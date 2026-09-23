package hatDictionary

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCH046DictionaryMissingBaseline(b *testing.B) {
	source := SourceFunc(func(_ context.Context, _ []string) (map[string]string, error) {
		return nil, nil
	})
	dictionary, err := New(source, Options{TTL: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := dictionary.Lookup(ctx, "missing")
		if err != nil || result.Found {
			b.Fatalf("missing baseline lookup = %#v, %v", result, err)
		}
	}
}

func BenchmarkCH046DictionaryMissingNegativeCache(b *testing.B) {
	source := SourceFunc(func(_ context.Context, _ []string) (map[string]string, error) {
		return nil, nil
	})
	dictionary, err := New(source, Options{
		TTL:                time.Hour,
		NegativeTTL:        time.Hour,
		MaxNegativeEntries: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if _, err := dictionary.Lookup(ctx, "missing"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := dictionary.Lookup(ctx, "missing")
		if err != nil || result.Found {
			b.Fatalf("cached missing lookup = %#v, %v", result, err)
		}
	}
}
