package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func benchmarkSQLColumnarJobs(b *testing.B) (*HatTrie, string) {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	rows := make([]string, 4096)
	for index := range rows {
		state := "queued"
		if index%4 == 0 {
			state = "running"
		}
		rows[index] = fmt.Sprintf(`{"id":%d,"state":%q,"name":"job-%d","payload":"unselected analytics payload repeated to make row materialization measurable"}`, index, state, index)
	}
	trie.UpsertString("jobs", "["+strings.Join(rows, ",")+"]")
	return trie, "FROM CACHE('jobs') AS job WHERE job.state = 'queued' SELECT job.id, job.name"
}

func BenchmarkSQLColumnarScan(b *testing.B) {
	trie, query := benchmarkSQLColumnarJobs(b)
	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("materialized", func(b *testing.B) {
		resolver := sqlRowsOnlyResolver{trie: trie}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, resolver); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSQLColumnarNumericFilter(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 1024 SELECT job.id"
	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("materialized", func(b *testing.B) {
		resolver := sqlRowsOnlyResolver{trie: trie}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, resolver); err != nil {
				b.Fatal(err)
			}
		}
	})
}
