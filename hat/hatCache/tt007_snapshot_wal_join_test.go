package hatCache

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinCommandJournalSnapshotAppliesSnapshotAndWAL(t *testing.T) {
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer sourceJournal.Close()
	if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "base", Value: "before"}); !response.OK {
		t.Fatal(response.Message)
	}

	sourceHandler := NewMonitoringHandler(sourceTrie, MonitoringOptions{Journal: sourceJournal}).Handler()
	var advanced atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHandler.ServeHTTP(w, r)
		if r.URL.Path == "/api/journal/snapshot" && advanced.CompareAndSwap(false, true) {
			if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "base", Value: "after"}); !response.OK {
				t.Errorf("post-snapshot SETSTR(base) failed: %s", response.Message)
			}
			if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "delta", Value: "wal"}); !response.OK {
				t.Errorf("post-snapshot SETSTR(delta) failed: %s", response.Message)
			}
		}
	}))
	defer server.Close()

	targetTrie := CreateHatTrie()
	defer targetTrie.Destroy()
	targetTrie.UpsertString("stale", "must disappear")
	targetJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer targetJournal.Close()
	dirty := NewLevelDBDirtyTracker()
	var persistCalls atomic.Int64
	result, err := JoinCommandJournalSnapshot(context.Background(), targetTrie, targetJournal, CommandJournalJoinOptions{
		Source:       server.URL,
		Client:       server.Client(),
		Limit:        1,
		MaxBatches:   4,
		DirtyTracker: dirty,
		Persist:      func() error { persistCalls.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Snapshot.JournalSequence != 1 || result.Pull.AppliedThrough != 3 || result.Pull.Applied != 2 || result.Pull.HasMore {
		t.Fatalf("join result = %#v, want snapshot 1 and complete WAL through 3", result)
	}
	if got := targetTrie.GetString("base"); got != "after" {
		t.Fatalf("base = %q, want after", got)
	}
	if got := targetTrie.GetString("delta"); got != "wal" {
		t.Fatalf("delta = %q, want wal", got)
	}
	if got := targetTrie.GetString("stale"); got != "" {
		t.Fatalf("stale = %q, want removed by snapshot replacement", got)
	}
	if got := targetJournal.Sequence(); got != 3 {
		t.Fatalf("target journal sequence = %d, want 3", got)
	}
	if got := persistCalls.Load(); got != 1 {
		t.Fatalf("persist calls = %d, want 1 after complete join", got)
	}
}

func TestJoinCommandJournalSnapshotRejectsIncompleteWALBeforePersist(t *testing.T) {
	sourceTrie := CreateHatTrie()
	defer sourceTrie.Destroy()
	sourceJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer sourceJournal.Close()
	if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "base", Value: "before"}); !response.OK {
		t.Fatal(response.Message)
	}
	sourceHandler := NewMonitoringHandler(sourceTrie, MonitoringOptions{Journal: sourceJournal}).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHandler.ServeHTTP(w, r)
		if r.URL.Path == "/api/journal/snapshot" {
			for index := 0; index < 2; index++ {
				if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: string(rune('a' + index)), Value: "wal"}); !response.OK {
					t.Errorf("post-snapshot SETSTR failed: %s", response.Message)
				}
			}
		}
	}))
	defer server.Close()

	targetTrie := CreateHatTrie()
	defer targetTrie.Destroy()
	targetJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer targetJournal.Close()
	var persistCalls atomic.Int64
	result, err := JoinCommandJournalSnapshot(context.Background(), targetTrie, targetJournal, CommandJournalJoinOptions{
		Source:     server.URL,
		Client:     server.Client(),
		Limit:      1,
		MaxBatches: 1,
		Persist:    func() error { persistCalls.Add(1); return nil },
	})
	if !errors.Is(err, ErrCommandJournalJoinIncomplete) {
		t.Fatalf("join error = %v, want ErrCommandJournalJoinIncomplete", err)
	}
	if result.Pull.Applied != 1 || !result.Pull.HasMore {
		t.Fatalf("incomplete join result = %#v, want one applied entry and HasMore", result)
	}
	if got := persistCalls.Load(); got != 0 {
		t.Fatalf("persist calls = %d, want 0 for incomplete join", got)
	}
}

func TestJoinCommandJournalSnapshotRejectsNilInputs(t *testing.T) {
	if _, err := JoinCommandJournalSnapshot(context.Background(), nil, nil, CommandJournalJoinOptions{}); err == nil {
		t.Fatal("JoinCommandJournalSnapshot(nil, nil) error = nil")
	}
}

func TestJoinCommandJournalSnapshotValidatesTimeoutBeforeDownload(t *testing.T) {
	targetTrie := CreateHatTrie()
	defer targetTrie.Destroy()
	targetJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer targetJournal.Close()
	_, err = JoinCommandJournalSnapshot(context.Background(), targetTrie, targetJournal, CommandJournalJoinOptions{
		Source:  "http://127.0.0.1:1",
		Timeout: -time.Nanosecond,
	})
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("negative timeout error = %v, want timeout validation", err)
	}
}
