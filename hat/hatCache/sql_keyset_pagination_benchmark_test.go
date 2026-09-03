package hatCache

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkHatTrieSQLKeysetPaginationDeepPage(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("events", benchmarkKeysetJSONRows(100000))
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		b.Fatal(err)
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	offsetCursor := ""
	keysetCursor := ""
	for page := 0; page < 900; page++ {
		result, err := ExecuteSQLQueryPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 100, offsetCursor)
		if err != nil {
			b.Fatal(err)
		}
		offsetCursor = result.NextCursor
		result, err = ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 100, keysetCursor)
		if err != nil {
			b.Fatal(err)
		}
		keysetCursor = result.NextCursor
	}
	b.Run("Offset", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := ExecuteSQLQueryPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 100, offsetCursor); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Keyset", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 100, keysetCursor); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkKeysetJSONRows(count int) string {
	data := make([]byte, 0, count*28+1)
	data = append(data, '[')
	for index := 0; index < count; index++ {
		if index > 0 {
			data = append(data, ',')
		}
		data = append(data, `{"id":`...)
		data = strconv.AppendInt(data, int64(index), 10)
		data = append(data, `,"score":`...)
		data = strconv.AppendInt(data, int64(index), 10)
		data = append(data, '}')
	}
	return string(append(data, ']'))
}
