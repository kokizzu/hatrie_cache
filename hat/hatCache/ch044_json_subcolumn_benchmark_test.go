package hatCache

import (
	"strconv"
	"strings"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

var benchmarkCH044Result SQLQueryResult

type ch044ColumnarOnlyResolver struct{ trie *HatTrie }

func (resolver ch044ColumnarOnlyResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver ch044ColumnarOnlyResolver) ResolveSQLColumnarSource(name, key string, fields []string) (SQLColumnarBatch, bool, error) {
	return resolver.trie.ResolveSQLColumnarSource(name, key, fields)
}

func benchmarkCH044Trie(b *testing.B) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var source strings.Builder
	source.WriteByte('[')
	for row := 0; row < 4096; row++ {
		if row != 0 {
			source.WriteByte(',')
		}
		source.WriteString(`{"doc":{"score":`)
		source.WriteString(strconv.Itoa(row))
		source.WriteString(`,"name":"name-`)
		source.WriteString(strconv.Itoa(row))
		source.WriteString(`"}}`)
	}
	source.WriteByte(']')
	if err := trie.UpsertStringChecked("users", source.String()); err != nil {
		b.Fatalf("UpsertStringChecked() error = %v", err)
	}
	return trie
}

func BenchmarkCH044JSONSubcolumnScanRowBaseline(b *testing.B) {
	trie := benchmarkCH044Trie(b)
	query := "FROM CACHE('users') AS user WHERE JSON_VALUE(user.doc, '$.score') >= 2048 SELECT JSON_VALUE(user.doc, '$.score') AS score, JSON_VALUE(user.doc, '$.name') AS name"
	resolver := sqlRowsOnlyResolver{trie: trie}
	if _, err := ExecuteSQLQuery(query, resolver); err != nil {
		b.Fatalf("baseline warm-up ExecuteSQLQuery() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatalf("baseline ExecuteSQLQuery() error = %v", err)
		}
		benchmarkCH044Result = result
	}
}

func BenchmarkCH044JSONSubcolumnScanDisabled(b *testing.B) {
	trie := benchmarkCH044Trie(b)
	query := "FROM CACHE('users') AS user WHERE JSON_VALUE(user.doc, '$.score') >= 2048 SELECT JSON_VALUE(user.doc, '$.score') AS score, JSON_VALUE(user.doc, '$.name') AS name"
	if _, err := ExecuteSQLQuery(query, trie); err != nil {
		b.Fatalf("disabled warm-up ExecuteSQLQuery() error = %v", err)
	}
	if stats := trie.SQLJSONSubcolumnAutoMaterializerStats(); stats.Entries != 0 || stats.RetainedBytes != 0 {
		b.Fatalf("disabled materializer stats = %+v, want empty", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatalf("disabled ExecuteSQLQuery() error = %v", err)
		}
		benchmarkCH044Result = result
	}
}

func BenchmarkCH044JSONSubcolumnScanExistingColumnar(b *testing.B) {
	trie := benchmarkCH044Trie(b)
	query := "FROM CACHE('users') AS user WHERE JSON_VALUE(user.doc, '$.score') >= 2048 SELECT JSON_VALUE(user.doc, '$.score') AS score, JSON_VALUE(user.doc, '$.name') AS name"
	resolver := ch044ColumnarOnlyResolver{trie: trie}
	if _, err := ExecuteSQLQuery(query, resolver); err != nil {
		b.Fatalf("existing columnar warm-up ExecuteSQLQuery() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatalf("existing columnar ExecuteSQLQuery() error = %v", err)
		}
		benchmarkCH044Result = result
	}
}

func BenchmarkCH044JSONSubcolumnScanMaterialized(b *testing.B) {
	trie := benchmarkCH044Trie(b)
	if err := trie.ConfigureSQLJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 1,
		MaxEntries:      8,
		MaxRows:         8192,
		MaxBytes:        1 << 20,
	}); err != nil {
		b.Fatalf("ConfigureSQLJSONSubcolumnAutoMaterializer() error = %v", err)
	}
	query := "FROM CACHE('users') AS user WHERE JSON_VALUE(user.doc, '$.score') >= 2048 SELECT JSON_VALUE(user.doc, '$.score') AS score, JSON_VALUE(user.doc, '$.name') AS name"
	if _, err := ExecuteSQLQuery(query, trie); err != nil {
		b.Fatalf("materialized warm-up ExecuteSQLQuery() error = %v", err)
	}
	stats := trie.SQLJSONSubcolumnAutoMaterializerStats()
	if stats.Promotions != 2 || stats.Entries != 2 {
		b.Fatalf("materializer stats = %+v, want two promoted paths", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(stats.RetainedBytes), "retained_bytes")
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatalf("materialized ExecuteSQLQuery() error = %v", err)
		}
		benchmarkCH044Result = result
	}
}
