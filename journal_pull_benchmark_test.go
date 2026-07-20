package hatriecache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkJournalCatchUpDeltaVsFullSnapshot(b *testing.B) {
	b.Run("Delta100Batched", func(b *testing.B) { benchmarkJournalCatchUpDelta100(b, true) })
	b.Run("Delta100FsyncEach", func(b *testing.B) { benchmarkJournalCatchUpDelta100(b, false) })
	b.Run("FullSnapshot10k", benchmarkJournalCatchUpFullSnapshot10k)
}

func BenchmarkJournalPullApplyBatch10K(b *testing.B) {
	b.Run("SerialApply", func(b *testing.B) {
		benchmarkJournalPullApplyBatch10K(b, false)
	})
	b.Run("SingleLockApply", func(b *testing.B) {
		benchmarkJournalPullApplyBatch10K(b, true)
	})
}

func benchmarkJournalPullApplyBatch10K(b *testing.B, scalarBatch bool) {
	const recordsPerBatch = 10_000
	records := benchmarkJournalScalarRecords(recordsPerBatch)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = journal.Close() })

	var walBytes int64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		before, err := journal.file.Stat()
		if err != nil {
			b.Fatal(err)
		}
		applied, response := journal.executeJournalRecordsBatchWithScalarBatch(trie, records, scalarBatch)
		if !response.OK || applied != recordsPerBatch {
			b.Fatalf("executeJournalRecordsBatch() = %d/%#v", applied, response)
		}
		after, err := journal.file.Stat()
		if err != nil {
			b.Fatal(err)
		}
		walBytes += after.Size() - before.Size()
	}
	b.StopTimer()
	b.ReportMetric(recordsPerBatch, "records/op")
	b.ReportMetric(float64(walBytes)/float64(b.N), "wal_B/op")
}

func BenchmarkJournalScalarApply10K(b *testing.B) {
	const recordsPerBatch = 10_000
	records := benchmarkJournalScalarRecords(recordsPerBatch)
	b.Run("Serial", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for index, record := range records {
				if response := trie.ExecuteCommand(record.Request); !response.OK {
					b.Fatalf("ExecuteCommand(record %d) = %#v", index, response)
				}
			}
		}
		b.ReportMetric(recordsPerBatch, "records/op")
	})
	b.Run("SingleLockBatch", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			applied, response, used := trie.executeJournalScalarBatch(records)
			if !used || !response.OK || applied != recordsPerBatch {
				b.Fatalf("executeJournalScalarBatch() = %d/%#v/%t", applied, response, used)
			}
		}
		b.ReportMetric(recordsPerBatch, "records/op")
	})
}

func benchmarkJournalScalarRecords(count int) []CommandJournalRecord {
	records := make([]CommandJournalRecord, count)
	for index := range records {
		records[index] = CommandJournalRecord{
			Sequence: uint64(index + 1),
			Request: CacheCommandRequest{
				Command: "SETINT",
				Key:     fmt.Sprintf("pull-apply:%05d", index),
				Value:   "42",
			},
		}
	}
	return records
}

func benchmarkJournalCatchUpDelta100(b *testing.B, batched bool) {
	tail := CommandJournalTail{
		LastSequence: 100,
		Entries:      make([]CommandJournalRecord, 100),
	}
	for idx := range tail.Entries {
		tail.Entries[idx] = CommandJournalRecord{
			Sequence: uint64(idx + 1),
			Request: CacheCommandRequest{
				Command: "SETSTR",
				Key:     fmt.Sprintf("changed:%06d", idx),
				Value:   "updated-value",
			},
		}
	}
	payload, err := json.Marshal(tail)
	if err != nil {
		b.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer source.Close()
	dir := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		trie := CreateHatTrie()
		journal, err := OpenCommandJournal(filepath.Join(dir, fmt.Sprintf("delta-%d.journal", idx)))
		if err != nil {
			b.Fatal(err)
		}
		applied := 0
		if batched {
			result, err := PullCommandJournal(context.Background(), trie, journal, CommandJournalPullOptions{
				Source:       source.URL,
				UntilCurrent: true,
				MaxBatches:   1,
				Client:       source.Client(),
			})
			if err != nil {
				b.Fatal(err)
			}
			applied = result.Applied
		} else {
			endpoint, err := commandJournalEndpoint(source.URL, 0, DefaultCommandJournalTailLimit)
			if err != nil {
				b.Fatal(err)
			}
			fetched, _, err := fetchCommandJournalTail(context.Background(), source.Client(), endpoint)
			if err != nil {
				b.Fatal(err)
			}
			for _, record := range fetched.Entries {
				if response := journal.ExecuteCommand(trie, record.Request); !response.OK {
					b.Fatalf("ExecuteCommand() = %#v", response)
				}
				applied++
			}
		}
		if applied != 100 {
			b.Fatalf("applied = %d, want 100", applied)
		}
		if err := journal.Close(); err != nil {
			b.Fatal(err)
		}
		trie.Destroy()
	}
	b.ReportMetric(100, "delta_entries/op")
	b.ReportMetric(float64(len(payload)), "wire_B/op")
}

func benchmarkJournalCatchUpFullSnapshot10k(b *testing.B) {
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	for idx := 0; idx < 10000; idx++ {
		sourceTrie.UpsertString(fmt.Sprintf("key:%06d", idx), "snapshot-value")
	}
	sourceJournal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "source.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer sourceJournal.Close()
	source := httptest.NewServer(NewMonitoringHandler(sourceTrie, MonitoringOptions{Journal: sourceJournal}).Handler())
	defer source.Close()
	dir := b.TempDir()
	var transferredBytes int64
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		trie := CreateHatTrie()
		trie.UpsertString("stale", "remove")
		journal, err := OpenCommandJournal(filepath.Join(dir, fmt.Sprintf("full-%d.journal", idx)))
		if err != nil {
			b.Fatal(err)
		}
		snapshotPath := filepath.Join(dir, fmt.Sprintf("full-%d.hc", idx))
		if _, err := PullCommandJournalSnapshot(context.Background(), source.URL, "", source.Client(), snapshotPath); err != nil {
			b.Fatal(err)
		}
		info, err := os.Stat(snapshotPath)
		if err != nil {
			b.Fatal(err)
		}
		transferredBytes += info.Size()
		if _, err := journal.ReplaceWithSnapshot(trie, snapshotPath); err != nil {
			b.Fatal(err)
		}
		if trie.Exists("stale") || trie.GetString("key:009999") != "snapshot-value" {
			b.Fatal("full snapshot did not replace exact state")
		}
		if err := journal.Close(); err != nil {
			b.Fatal(err)
		}
		trie.Destroy()
	}
	b.ReportMetric(10000, "snapshot_entries/op")
	b.ReportMetric(float64(transferredBytes)/float64(b.N), "wire_B/op")
}
