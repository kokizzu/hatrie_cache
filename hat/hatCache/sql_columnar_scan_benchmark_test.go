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

func BenchmarkSQLColumnarNumericFilterLimited(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 1024 SELECT job.id LIMIT 32"
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

func BenchmarkSQLColumnarNumericAggregate(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 1024 SELECT COUNT(*) AS count, SUM(job.id) AS total, AVG(job.id) AS average, MIN(job.id) AS minimum, MAX(job.id) AS maximum"
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

func BenchmarkSQLJSONColumnarBatch(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	data, err := trie.GetBytesChecked("jobs")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch, err := sqlJSONColumnarBatch("jobs", data, []string{"id", "state"})
		if err != nil {
			b.Fatal(err)
		}
		if batch.Rows != 4096 {
			b.Fatalf("batch.Rows = %d, want 4096", batch.Rows)
		}
	}
}

type benchmarkColumnarResolver struct{ batch SQLColumnarBatch }

func (resolver benchmarkColumnarResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return nil, nil
}

func (resolver benchmarkColumnarResolver) ResolveSQLColumnarSource(string, string, []string) (SQLColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func BenchmarkSQLColumnarDictionaryFilter(b *testing.B) {
	states := make([]interface{}, 4096)
	ids := make([]interface{}, len(states))
	for index := range states {
		state := "queued"
		if index%4 == 0 {
			state = "running"
		}
		states[index] = state
		ids[index] = float64(index)
	}
	plain := SQLColumnarBatch{Columns: map[string][]interface{}{"state": states, "id": ids}, Rows: len(states)}
	dictionary := SQLColumnarBatch{Columns: map[string][]interface{}{"state": append([]interface{}(nil), states...), "id": ids}, Rows: len(states)}
	dictionary.EncodeRepeatedStrings()
	query := "FROM CACHE('jobs') AS job WHERE job.state = 'queued' SELECT job.id LIMIT 32"
	for name, resolver := range map[string]benchmarkColumnarResolver{"plain": {batch: plain}, "dictionary": {batch: dictionary}} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ExecuteSQLQuery(query, resolver); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
