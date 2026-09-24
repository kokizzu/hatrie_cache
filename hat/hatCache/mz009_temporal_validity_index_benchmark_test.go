package hatCache

import (
	"encoding/json"
	"testing"
	"time"
)

const sqlJSONValidityBenchmarkQuery = "FROM CACHE('events') AS event WHERE VALID_AT(TIMESTAMP '2026-01-07T22:40:30Z', event.valid_from, event.valid_to) SELECT event.id"

var sqlJSONValidityBenchmarkSink int

func benchmarkSQLJSONValidityData() string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]map[string]interface{}, 10000)
	for index := range rows {
		from := base.Add(time.Duration(index*2) * time.Minute)
		to := from.Add(time.Minute)
		rows[index] = map[string]interface{}{
			"id":         index,
			"valid_from": from.Format(time.RFC3339Nano),
			"valid_to":   to.Format(time.RFC3339Nano),
		}
	}
	data, err := json.Marshal(rows)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func benchmarkSQLJSONValidityTrie(data string) *HatTrie {
	trie := CreateHatTrie()
	trie.UpsertString("events", data)
	return trie
}

func BenchmarkSQLJSONValidityScan(b *testing.B) {
	data := benchmarkSQLJSONValidityData()
	trie := benchmarkSQLJSONValidityTrie(data)
	defer trie.Destroy()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(sqlJSONValidityBenchmarkQuery, trie)
		if err != nil {
			b.Fatal(err)
		}
		sqlJSONValidityBenchmarkSink = len(result.Rows)
	}
}

func BenchmarkSQLJSONValidityIndexWarm(b *testing.B) {
	data := benchmarkSQLJSONValidityData()
	trie := benchmarkSQLJSONValidityTrie(data)
	defer trie.Destroy()
	if err := trie.CreateSQLJSONValidityIndex("events", "valid_from", "valid_to"); err != nil {
		b.Fatal(err)
	}
	if _, err := ExecuteSQLQuery(sqlJSONValidityBenchmarkQuery, trie); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(sqlJSONValidityBenchmarkQuery, trie)
		if err != nil {
			b.Fatal(err)
		}
		sqlJSONValidityBenchmarkSink = len(result.Rows)
	}
}

func BenchmarkSQLJSONValidityIndexColdBuild(b *testing.B) {
	data := benchmarkSQLJSONValidityData()
	b.ReportAllocs()
	for range b.N {
		trie := benchmarkSQLJSONValidityTrie(data)
		if err := trie.CreateSQLJSONValidityIndex("events", "valid_from", "valid_to"); err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
		result, err := ExecuteSQLQuery(sqlJSONValidityBenchmarkQuery, trie)
		trie.Destroy()
		if err != nil {
			b.Fatal(err)
		}
		sqlJSONValidityBenchmarkSink = len(result.Rows)
	}
}
