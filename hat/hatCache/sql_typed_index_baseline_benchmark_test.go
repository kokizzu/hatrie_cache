package hatCache

import (
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

// sqlOrderedRowsOnlyResolver exposes the pre-streaming ordered-index contract.
// It keeps the benchmark's materialized control on the same underlying index.
type sqlOrderedRowsOnlyResolver struct{ trie *HatTrie }

func (resolver sqlOrderedRowsOnlyResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver sqlOrderedRowsOnlyResolver) ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLRow, bool, error) {
	return resolver.trie.ResolveSQLOrderedSource(name, key, field, desc, nullsFirst, nullsLast)
}

// sqlCopiedCoveringResolver retains the pre-immutable-source covering path.
// It uses the same configured index as the optimized resolver for comparison.
type sqlCopiedCoveringResolver struct{ trie *HatTrie }

func (resolver sqlCopiedCoveringResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver sqlCopiedCoveringResolver) ResolveSQLCoveringSource(name, key, field string, value interface{}, fields []string) ([]SQLRow, bool, error) {
	if name != "CACHE" || len(fields) == 0 {
		return nil, false, nil
	}
	data, err := resolver.trie.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	resolver.trie.sqlIndexMu.Lock()
	defer resolver.trie.sqlIndexMu.Unlock()
	index := resolver.trie.sqlJSONCoveringIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	for _, column := range fields {
		if _, ok := index.columns[column]; !ok {
			return nil, false, nil
		}
	}
	if err := refreshSQLJSONCoveringIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(index.rows[valueKey]), true, nil
}

func BenchmarkSQLTypedIndexBaseline(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	const rows = 100_000
	var data strings.Builder
	data.Grow(rows * 32)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"state":"ready"}`)
	}
	data.WriteByte(']')
	source := data.String()
	trie.UpsertString("events", source)
	if err := trie.CreateSQLJSONFieldIndex("events", "id"); err != nil {
		b.Fatal(err)
	}
	covering := CreateHatTrie()
	b.Cleanup(covering.Destroy)
	covering.UpsertString("events", source)
	if err := covering.CreateSQLJSONCoveringIndex("events", "id", "id"); err != nil {
		b.Fatal(err)
	}
	equalityQuery := "FROM CACHE('events') AS event WHERE event.id = 99999 SELECT event.id"
	rangeQuery := "FROM CACHE('events') AS event WHERE event.id >= 99900 AND event.id < 99910 SELECT event.id"
	orderQuery := "FROM CACHE('events') AS event ORDER BY event.id DESC LIMIT 10 SELECT event.id"
	if result, err := ExecuteSQLQuery(equalityQuery, trie); err != nil || len(result.Rows) != 1 {
		b.Fatalf("indexed equality warmup = %#v, %v", result, err)
	}
	if result, err := ExecuteSQLQuery(rangeQuery, trie); err != nil || len(result.Rows) != 10 {
		b.Fatalf("indexed range warmup = %#v, %v", result, err)
	}
	if result, err := ExecuteSQLQuery(orderQuery, trie); err != nil || len(result.Rows) != 10 {
		b.Fatalf("indexed order warmup = %#v, %v", result, err)
	}
	if result, err := ExecuteSQLQuery(equalityQuery, covering); err != nil || len(result.Rows) != 1 {
		b.Fatalf("covering equality warmup = %#v, %v", result, err)
	}
	fullScan := sqlRowsOnlyResolver{trie: trie}
	materializedIndex := sqlOrderedRowsOnlyResolver{trie: trie}
	copiedCovering := sqlCopiedCoveringResolver{trie: covering}
	for _, benchmark := range []struct {
		name     string
		query    string
		resolver SQLSourceResolver
	}{
		{name: "equality/indexed", query: equalityQuery, resolver: trie},
		{name: "equality/full_scan", query: equalityQuery, resolver: fullScan},
		{name: "covering/equality_immutable_source", query: equalityQuery, resolver: covering},
		{name: "covering/equality_copied_source", query: equalityQuery, resolver: copiedCovering},
		{name: "range/indexed", query: rangeQuery, resolver: trie},
		{name: "range/full_scan", query: rangeQuery, resolver: fullScan},
		{name: "order_limit/streamed_index", query: orderQuery, resolver: trie},
		{name: "order_limit/materialized_index", query: orderQuery, resolver: materializedIndex},
		{name: "order_limit/full_scan", query: orderQuery, resolver: fullScan},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ExecuteSQLQuery(benchmark.query, benchmark.resolver); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("equality/direct_indexed_resolver", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			resolved, available, err := trie.ResolveSQLIndexedSource("CACHE", "events", "id", float64(rows-1))
			if err != nil || !available || len(resolved) != 1 {
				b.Fatalf("ResolveSQLIndexedSource() = %#v, %v, %v", resolved, available, err)
			}
		}
	})
}
