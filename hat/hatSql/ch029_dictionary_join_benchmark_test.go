package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatDictionary"
	"hatrie_cache/hat/hatSql"
)

const (
	ch029BenchmarkFactRows      = 1000
	ch029BenchmarkDimensionRows = 10000
)

type ch029FullScanResolver struct {
	facts     []hatSql.Row
	dimension []hatSql.Row
}

func (resolver ch029FullScanResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name == "CACHE" && key == "facts" {
		return resolver.facts, nil
	}
	return nil, nil
}

func (resolver ch029FullScanResolver) ResolveSQLExternalSource(string) ([]hatSql.Row, error) {
	return resolver.dimension, nil
}

func BenchmarkCH029DictionaryBackedJoin(b *testing.B) {
	query := `
FROM CACHE('facts') AS fact
JOIN EXTERNAL('dimension') AS dim ON fact.dimension_id = dim.id
SELECT fact.id, dim.label`
	facts := make([]hatSql.Row, ch029BenchmarkFactRows)
	dimension := make([]hatSql.Row, ch029BenchmarkDimensionRows)
	values := make(map[string]string, ch029BenchmarkDimensionRows)
	keys := make([]string, ch029BenchmarkFactRows)
	for index := range dimension {
		id := fmt.Sprintf("id-%05d", index)
		label := fmt.Sprintf("label-%05d", index)
		dimension[index] = hatSql.Row{"id": id, "label": label}
		if index < len(facts) {
			facts[index] = hatSql.Row{"id": int64(index), "dimension_id": id}
			keys[index] = id
			values[id] = label
		}
	}

	b.Run("FullExternalScan", func(b *testing.B) {
		resolver := ch029FullScanResolver{facts: facts, dimension: dimension}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := hatSql.ExecuteQueryParameters(context.Background(), query, resolver, nil, hatSql.QueryOptions{})
			if err != nil || len(result.Rows) != len(facts) {
				b.Fatalf("full scan result rows=%d err=%v", len(result.Rows), err)
			}
		}
	})

	b.Run("WarmDictionaryLookup", func(b *testing.B) {
		dictionary, err := hatDictionary.New(hatDictionary.SourceFunc(func(_ context.Context, requested []string) (map[string]string, error) {
			loaded := make(map[string]string, len(requested))
			for _, key := range requested {
				loaded[key] = values[key]
			}
			return loaded, nil
		}), hatDictionary.Options{MaxEntries: ch029BenchmarkFactRows, MaxRefreshKeys: ch029BenchmarkFactRows})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := dictionary.Refresh(context.Background(), keys); err != nil {
			b.Fatal(err)
		}
		resolver, err := hatDictionary.NewSQLDictionaryLookupResolver(
			hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
				if name == "CACHE" && key == "facts" {
					return facts, nil
				}
				return nil, nil
			}),
			dictionary,
			hatDictionary.SQLDictionaryLookupOptions{SourceKey: "dimension", KeyField: "id", ValueField: "label"},
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := hatSql.ExecuteQueryParameters(context.Background(), query, resolver, nil, hatSql.QueryOptions{})
			if err != nil || len(result.Rows) != len(facts) {
				b.Fatalf("dictionary result rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
}
