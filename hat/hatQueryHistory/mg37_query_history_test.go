package hatQueryHistory_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatQueryHistory"
)

func TestMG37HistoryPersistsAndRedactsRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query-history.jsonl")
	history, err := hatQueryHistory.Open(hatQueryHistory.Options{Path: path, Sync: true})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	started := time.Unix(100, 0).UTC()
	if err := history.Append(hatQueryHistory.QueryRecord{
		QueryID:    "q-1",
		State:      hatQueryHistory.StateSucceeded,
		StartedAt:  started,
		FinishedAt: started.Add(time.Second),
		ErrorCode:  "",
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := history.Append(hatQueryHistory.QueryRecord{
		QueryID:    "q-2",
		State:      hatQueryHistory.StateFailed,
		StartedAt:  started,
		FinishedAt: started.Add(2 * time.Second),
		ErrorCode:  "query_failed",
	}); err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	if got := history.Snapshot(); len(got) != 2 || got[0].QueryID != "q-1" || got[1].ErrorCode != "query_failed" {
		t.Fatalf("Snapshot() = %#v", got)
	}
	if err := history.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.Contains(string(data), "SELECT") || strings.Contains(string(data), "password") || strings.Contains(string(data), "cancel_reason") {
		t.Fatalf("durable history contains unredacted content: %s", data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("history permissions = %o, want 600", info.Mode().Perm())
	}

	reopened, err := hatQueryHistory.Open(hatQueryHistory.Options{Path: path})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	if got := reopened.Snapshot(); len(got) != 2 || got[0].QueryID != "q-1" || got[1].QueryID != "q-2" {
		t.Fatalf("reopened Snapshot() = %#v", got)
	}
}

func TestMG37HistoryRotatesAndBoundsEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query-history.jsonl")
	history, err := hatQueryHistory.Open(hatQueryHistory.Options{
		Path:          path,
		MaxEntries:    2,
		MaxEntryBytes: 512,
		MaxFileBytes:  650,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer history.Close()
	for index := 1; index <= 8; index++ {
		if err := history.Append(hatQueryHistory.QueryRecord{QueryID: "query-" + string(rune('0'+index)), State: hatQueryHistory.StateSucceeded}); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}
	got := history.Snapshot()
	if len(got) != 2 || got[0].QueryID != "query-7" || got[1].QueryID != "query-8" {
		t.Fatalf("bounded Snapshot() = %#v", got)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("rotated history file error = %v", err)
	}
}

func TestMG37HistoryValidatesAndCloses(t *testing.T) {
	if _, err := hatQueryHistory.Open(hatQueryHistory.Options{}); !errors.Is(err, hatQueryHistory.ErrPathRequired) {
		t.Fatalf("missing path error = %v, want ErrPathRequired", err)
	}
	if _, err := hatQueryHistory.Open(hatQueryHistory.Options{Path: filepath.Join(t.TempDir(), "history"), MaxEntries: -1}); !errors.Is(err, hatQueryHistory.ErrInvalidOptions) {
		t.Fatalf("invalid options error = %v, want ErrInvalidOptions", err)
	}

	history, err := hatQueryHistory.Open(hatQueryHistory.Options{Path: filepath.Join(t.TempDir(), "history"), MaxEntryBytes: 256})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := history.Append(hatQueryHistory.QueryRecord{QueryID: "bad\nquery", State: hatQueryHistory.StateSucceeded}); !errors.Is(err, hatQueryHistory.ErrInvalidRecord) {
		t.Fatalf("invalid record error = %v, want ErrInvalidRecord", err)
	}
	if err := history.Append(hatQueryHistory.QueryRecord{QueryID: "q", State: "unknown"}); !errors.Is(err, hatQueryHistory.ErrInvalidRecord) {
		t.Fatalf("invalid state error = %v, want ErrInvalidRecord", err)
	}
	if err := history.Append(hatQueryHistory.QueryRecord{QueryID: "q", State: hatQueryHistory.StateSucceeded, ErrorCode: strings.Repeat("x", 300)}); !errors.Is(err, hatQueryHistory.ErrInvalidRecord) {
		t.Fatalf("long error code = %v, want ErrInvalidRecord", err)
	}
	if err := history.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := history.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := history.Append(hatQueryHistory.QueryRecord{QueryID: "q", State: hatQueryHistory.StateSucceeded}); !errors.Is(err, hatQueryHistory.ErrClosed) {
		t.Fatalf("Append() after Close error = %v, want ErrClosed", err)
	}
}

func TestMG37HistoryRejectsOversizedRecordsAndCorruptFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history")
	history, err := hatQueryHistory.Open(hatQueryHistory.Options{Path: path, MaxEntryBytes: 256})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := history.Append(hatQueryHistory.QueryRecord{QueryID: strings.Repeat("x", 250), State: hatQueryHistory.StateSucceeded}); !errors.Is(err, hatQueryHistory.ErrEntryTooLarge) {
		t.Fatalf("oversized entry error = %v, want ErrEntryTooLarge", err)
	}
	if err := history.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.WriteFile(path, []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := hatQueryHistory.Open(hatQueryHistory.Options{Path: path}); !errors.Is(err, hatQueryHistory.ErrCorrupt) {
		t.Fatalf("corrupt file error = %v, want ErrCorrupt", err)
	}
}

func TestMG37HistoryConcurrentAppends(t *testing.T) {
	history, err := hatQueryHistory.Open(hatQueryHistory.Options{Path: filepath.Join(t.TempDir(), "history"), MaxEntries: 256, MaxFileBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer history.Close()
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for index := 0; index < 25; index++ {
				if err := history.Append(hatQueryHistory.QueryRecord{QueryID: "query", State: hatQueryHistory.StateSucceeded}); err != nil {
					t.Errorf("worker %d Append() error = %v", worker, err)
					return
				}
			}
		}(worker)
	}
	group.Wait()
	if got := history.Len(); got != 256 {
		t.Fatalf("Len() = %d, want 256", got)
	}
}
