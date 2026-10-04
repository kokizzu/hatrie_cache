package hatQueryHistory_test

import (
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatQueryHistory"
)

var mg37BenchmarkRecord = hatQueryHistory.QueryRecord{
	QueryID: "query-1",
	State:   hatQueryHistory.StateSucceeded,
}

func BenchmarkMG37DurableAppend(b *testing.B) {
	history, err := hatQueryHistory.Open(hatQueryHistory.Options{
		Path:         filepath.Join(b.TempDir(), "history"),
		MaxEntries:   1024,
		MaxFileBytes: 16 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer history.Close()
	b.ReportAllocs()
	for b.Loop() {
		if err := history.Append(mg37BenchmarkRecord); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMG37DurableAppendSync(b *testing.B) {
	history, err := hatQueryHistory.Open(hatQueryHistory.Options{
		Path:         filepath.Join(b.TempDir(), "history"),
		MaxEntries:   1024,
		MaxFileBytes: 16 << 20,
		Sync:         true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer history.Close()
	b.ReportAllocs()
	for b.Loop() {
		if err := history.Append(mg37BenchmarkRecord); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMG37DurableSnapshot(b *testing.B) {
	history, err := hatQueryHistory.Open(hatQueryHistory.Options{
		Path:         filepath.Join(b.TempDir(), "history"),
		MaxEntries:   1024,
		MaxFileBytes: 16 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer history.Close()
	for index := 0; index < 1024; index++ {
		if err := history.Append(mg37BenchmarkRecord); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if len(history.Snapshot()) != 1024 {
			b.Fatal("unexpected snapshot length")
		}
	}
}
