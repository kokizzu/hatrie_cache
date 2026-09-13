package hatSql

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLQueryLogRotatesByBytesAndReadsRetainedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.log")
	entries := make([]SQLQueryLogEntry, 5)
	for index := range entries {
		entries[index] = SQLQueryLogEntry{
			QueryID:       fmt.Sprintf("query-%02d", index+1),
			State:         SQLQueryStateSucceeded,
			StartedAt:     time.Unix(int64(index+1), 0).UTC(),
			FinishedAt:    time.Unix(int64(index+1), int64(time.Millisecond)).UTC(),
			DurationNanos: int64(time.Millisecond),
		}
	}
	encoded, err := json.Marshal(entries[0])
	if err != nil {
		t.Fatal(err)
	}
	log, err := OpenSQLQueryLogWithOptions(path, SQLQueryLogOptions{
		MaxFileBytes:     int64(len(encoded) + 1),
		MaxRetainedFiles: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := log.AppendEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	for _, suffix := range []string{".1", ".2"} {
		if _, err := os.Stat(path + suffix); err != nil {
			t.Fatalf("rotated segment %s: %v", suffix, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("third rotated segment error = %v, want not exists", err)
	}

	reopened, err := OpenSQLQueryLogWithOptions(path, SQLQueryLogOptions{
		MaxFileBytes:     int64(len(encoded) + 1),
		MaxRetainedFiles: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("retained entries = %d, want 3", len(got))
	}
	for index, entry := range got {
		wantID := entries[index+2].QueryID
		if entry.QueryID != wantID {
			t.Errorf("entry %d query ID = %q, want %q", index, entry.QueryID, wantID)
		}
	}
}

func TestSQLQueryLogRotatesByAgeAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.log")
	options := SQLQueryLogOptions{MaxFileAge: time.Hour, MaxRetainedFiles: 1}
	log, err := OpenSQLQueryLogWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.AppendEntry(ch004QueryLogEntry("query-old")); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(1, 0)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLQueryLogWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.AppendEntry(ch004QueryLogEntry("query-new")); err != nil {
		t.Fatal(err)
	}
	entries, err := reopened.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].QueryID != "query-old" || entries[1].QueryID != "query-new" {
		t.Fatalf("age-rotated entries = %#v, want old then new", entries)
	}
}

func TestSQLQueryLogDefaultAppendDoesNotCreateArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.log")
	log, err := OpenSQLQueryLog(path)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := log.AppendEntry(ch004QueryLogEntry(fmt.Sprintf("query-%d", index))); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default archive error = %v, want not exists", err)
	}
}

func TestSQLQueryLogRotationOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		options SQLQueryLogOptions
	}{
		{name: "negative file bytes", options: SQLQueryLogOptions{MaxFileBytes: -1}},
		{name: "negative file age", options: SQLQueryLogOptions{MaxFileAge: -time.Second}},
		{name: "negative retained files", options: SQLQueryLogOptions{MaxRetainedFiles: -1}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := OpenSQLQueryLogWithOptions(filepath.Join(t.TempDir(), "query.log"), testCase.options)
			if err == nil {
				t.Fatal("OpenSQLQueryLogWithOptions() accepted invalid rotation options")
			}
		})
	}
}

func TestSQLQueryLogReadRejectsRotatedSymlink(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "query.log")
	log, err := OpenSQLQueryLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.AppendEntry(ch004QueryLogEntry("query-1")); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(directory, "private-records")
	if err := os.WriteFile(target, []byte("not a query log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path+".1"); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLQueryLogWithOptions(path, SQLQueryLogOptions{
		MaxFileAge:       time.Hour,
		MaxRetainedFiles: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Read(); err == nil {
		t.Fatal("Read() followed a rotated archive symlink")
	}
}

func ch004QueryLogEntry(queryID string) SQLQueryLogEntry {
	return SQLQueryLogEntry{
		QueryID:       queryID,
		State:         SQLQueryStateSucceeded,
		StartedAt:     time.Unix(100, 0).UTC(),
		FinishedAt:    time.Unix(100, int64(time.Millisecond)).UTC(),
		DurationNanos: int64(time.Millisecond),
	}
}
