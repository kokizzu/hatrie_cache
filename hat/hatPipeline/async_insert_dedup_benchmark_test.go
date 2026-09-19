package hatPipeline

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func BenchmarkAsyncInsertDeduplicatorMemoryAccept(b *testing.B) {
	deduplicator, err := NewAsyncInsertDeduplicator(AsyncInsertDeduplicatorOptions{Capacity: 1024, TTL: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]string, 1024)
	for index := range ids {
		ids[index] = "insert-" + strconv.Itoa(index)
	}
	payload := []byte("payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := deduplicator.Accept(context.Background(), "orders", ids[index&1023], payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAsyncInsertDedupFileAppend(b *testing.B) {
	store, err := NewAsyncInsertDedupFileStore(AsyncInsertDedupFileStoreOptions{Path: filepath.Join(b.TempDir(), "ledger"), MaxBytes: 128 << 20})
	if err != nil {
		b.Fatal(err)
	}
	record := AsyncInsertDedupRecord{Source: "orders", ID: "insert", ExpiresAt: time.Now().Add(time.Hour)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record.ID = "insert-" + strconv.Itoa(index)
		if err := store.Append(context.Background(), record); err != nil {
			b.Fatal(err)
		}
	}
}
