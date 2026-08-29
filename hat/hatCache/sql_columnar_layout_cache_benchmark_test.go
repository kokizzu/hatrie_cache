package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

type sqlColumnarLayoutCacheBypassResolver struct {
	trie *HatTrie
}

func (resolver sqlColumnarLayoutCacheBypassResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver sqlColumnarLayoutCacheBypassResolver) ResolveSQLColumnarSource(name, key string, fields []string) (SQLColumnarBatch, bool, error) {
	if name != "CACHE" {
		return SQLColumnarBatch{}, false, nil
	}
	data, err := resolver.trie.GetBytesChecked(key)
	if err != nil {
		return SQLColumnarBatch{}, false, err
	}
	batch, err := sqlJSONColumnarBatch(key, data, fields)
	return batch, true, err
}

func BenchmarkSQLColumnarLayoutCache(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.WriteByte('[')
	for row := 0; row < 1024; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		state := "queued"
		if row%4 == 0 {
			state = "running"
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"state":"`)
		data.WriteString(state)
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 512 SELECT job.id, job.state"

	b.Run("decode_every_time", func(b *testing.B) {
		resolver := sqlColumnarLayoutCacheBypassResolver{trie: trie}
		b.ResetTimer()
		for b.Loop() {
			if _, err := ExecuteSQLQuery(query, resolver); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("observed_layout", func(b *testing.B) {
		for scan := 0; scan < sqlColumnarLayoutCacheMinReads; scan++ {
			if _, err := ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()
		for b.Loop() {
			if _, err := ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
		}
	})
}
