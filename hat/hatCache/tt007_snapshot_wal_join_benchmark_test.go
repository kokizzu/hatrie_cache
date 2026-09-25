package hatCache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func BenchmarkTT007JoinCommandJournalSnapshot(b *testing.B) {
	benchmarkTT007Join(b, true)
}

func BenchmarkTT007ManualSnapshotThenPull(b *testing.B) {
	benchmarkTT007Join(b, false)
}

func benchmarkTT007Join(b *testing.B, composed bool) {
	b.Helper()
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceJournal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "source.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer sourceJournal.Close()
	if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "base", Value: "before"}); !response.OK {
		b.Fatal(response.Message)
	}
	sourceHandler := NewMonitoringHandler(sourceTrie, MonitoringOptions{Journal: sourceJournal}).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHandler.ServeHTTP(w, r)
		if r.URL.Path == "/api/journal/snapshot" {
			if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "delta", Value: "wal"}); !response.OK {
				b.Errorf("post-snapshot SETSTR failed: %s", response.Message)
			}
		}
	}))
	defer server.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		targetTrie := CreateHatTrie()
		targetJournal, err := OpenCommandJournal(filepath.Join(b.TempDir(), fmt.Sprintf("target-%d.journal", index)))
		if err != nil {
			b.Fatal(err)
		}
		snapshotPath := filepath.Join(b.TempDir(), fmt.Sprintf("snapshot-%d.hc", index))
		b.StartTimer()
		if composed {
			if _, err := JoinCommandJournalSnapshot(context.Background(), targetTrie, targetJournal, CommandJournalJoinOptions{
				Source:       server.URL,
				Client:       server.Client(),
				SnapshotPath: snapshotPath,
				Limit:        16,
				MaxBatches:   4,
			}); err != nil {
				b.Fatal(err)
			}
		} else {
			metadata, err := PullCommandJournalSnapshot(context.Background(), server.URL, "", server.Client(), snapshotPath)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := targetJournal.ReplaceWithSnapshot(targetTrie, snapshotPath); err != nil {
				b.Fatal(err)
			}
			result, err := PullCommandJournal(context.Background(), targetTrie, targetJournal, CommandJournalPullOptions{
				Source:        server.URL,
				AfterSequence: metadata.JournalSequence,
				Limit:         16,
				UntilCurrent:  true,
				MaxBatches:    4,
				Client:        server.Client(),
			})
			if err != nil || result.HasMore {
				b.Fatalf("manual join result = %#v, error = %v", result, err)
			}
		}
		b.StopTimer()
		_ = targetJournal.Close()
		targetTrie.Destroy()
		b.StartTimer()
	}
}
