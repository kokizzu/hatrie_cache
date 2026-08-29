package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func benchmarkSQLColumnarRawBytes(b *testing.B) (*HatTrie, []string) {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	rows := make([]string, 256)
	for index := range rows {
		state := "queued"
		if index%4 == 0 {
			state = "running"
		}
		rows[index] = fmt.Sprintf(`{"id":%d,"state":%q,"name":"job-%d","payload":"unselected analytics payload repeated to make source-copy allocation measurable"}`, index, state, index)
	}
	fields := []string{"state", "id", "name"}
	trie.UpsertBytes("jobs", []byte("["+strings.Join(rows, ",")+"]"))
	if _, handled, err := trie.sqlColumnarRawBytesBatch("jobs", fields); err != nil || !handled {
		b.Fatalf("in-memory raw fixture handled=%v err=%v", handled, err)
	}
	return trie, fields
}

func BenchmarkSQLColumnarRawBytesSource(b *testing.B) {
	trie, fields := benchmarkSQLColumnarRawBytes(b)
	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
				b.Fatalf("ResolveSQLColumnarSource() available=%v err=%v", available, err)
			}
		}
	})
	b.Run("checked_clone", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			data, err := trie.GetBytesChecked("jobs")
			if err != nil {
				b.Fatal(err)
			}
			if _, err := sqlJSONColumnarBatch("jobs", data, fields); err != nil {
				b.Fatal(err)
			}
		}
	})
}
