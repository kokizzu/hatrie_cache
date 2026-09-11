package hatSql

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSQLQueryLogPersistsSanitizedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.log")
	log, err := OpenSQLQueryLog(path)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Unix(100, 0).UTC()
	finished := started.Add(25 * time.Millisecond)
	status := SQLQueryStatus{
		QueryID:      "query-1",
		State:        SQLQueryStateCanceled,
		StartedAt:    started,
		FinishedAt:   finished,
		CancelReason: "private operator note",
	}
	if err := log.Append(status); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("query log permissions = %o, want 600", info.Mode().Perm())
	}
	if strings.Contains(string(data), "private operator note") || strings.Contains(string(data), "cancel_reason") {
		t.Fatalf("query log persisted private cancellation data: %s", data)
	}
	entries, err := log.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Read() entries = %d, want 1", len(entries))
	}
	if entries[0].QueryID != status.QueryID || entries[0].State != status.State || entries[0].DurationNanos != 25*time.Millisecond.Nanoseconds() {
		t.Fatalf("query log entry = %#v", entries[0])
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenSQLQueryLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err = reopened.Read()
	if err != nil || len(entries) != 1 || entries[0].QueryID != "query-1" {
		t.Fatalf("reopened Read() = %#v, %v", entries, err)
	}
}

func TestSQLQueryManagerWritesCompletedQueryLogWithoutQueryText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.log")
	log, err := OpenSQLQueryLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	manager := NewSQLQueryManagerWithOptions(SQLQueryManagerOptions{
		HistoryCapacity: 2,
		QueryLog:        log,
	})
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1)}}, nil
	})
	const privateSource = "private-source-name"
	if _, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('private-source-name')", resolver, nil, SQLQueryOptions{QueryID: "managed-1"}); err != nil {
		t.Fatal(err)
	}
	if err := log.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := manager.QueryLogError(); err != nil {
		t.Fatalf("QueryLogError() = %v", err)
	}
	entries, err := log.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].QueryID != "managed-1" || entries[0].State != SQLQueryStateSucceeded {
		t.Fatalf("manager query log = %#v", entries)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), privateSource) {
		t.Fatalf("query log persisted source text: %s", data)
	}
}

func TestSQLQueryLogRejectsMalformedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.log")
	if err := os.WriteFile(path, []byte("{not-json}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := OpenSQLQueryLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err := log.Read(); err == nil {
		t.Fatal("Read() accepted malformed record")
	}
}

func TestSQLQueryLogRejectsSymlinkPath(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.log")
	link := filepath.Join(directory, "query.log")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := OpenSQLQueryLog(link); err == nil {
		t.Fatal("OpenSQLQueryLog() accepted a symlink path")
	}
}
