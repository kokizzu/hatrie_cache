package hatriecache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCommandJournalReplaysMutatingCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views"}); !got.OK {
		t.Fatalf("journaled INC response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "GET", Key: "name"}); !got.OK || got.Value != "ivi" {
		t.Fatalf("journaled GET response = %#v, want ivi", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want 2", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("name"); got != "ivi" {
		t.Fatalf("replayed name = %q, want ivi", got)
	}
	if got := replayed.GetCounter("views"); got != 1 {
		t.Fatalf("replayed views = %d, want 1", got)
	}
}

func TestCommandJournalSkipsInvalidAndReadOnlyCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)

	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "GET", Key: "missing"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETINT", Key: "counter", Value: "bad"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "PUSHSLICE", Key: "slice"})

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("journal entries = %d, want 0", len(entries))
	}
}

func TestCommandJournalSnapshotCheckpointPreventsDoubleReplay(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "commands.journal")
	snapshotPath := filepath.Join(dir, "snapshot.json")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)

	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETINT", Key: "views", Value: "1"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "2"})
	if got := ht.GetCounter("views"); got != 3 {
		t.Fatalf("views before snapshot = %d, want 3", got)
	}
	if err := journal.SaveSnapshot(ht, snapshotPath); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if entries, err := readCommandJournalEntries(journalPath); err != nil || len(entries) != 1 || !entries[0].Checkpoint || entries[0].Sequence != 2 {
		t.Fatalf("entries after compact = %#v/%v, want checkpoint sequence 2", entries, err)
	}

	journal, err = OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal(after compact) error = %v", err)
	}
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "3"})

	loaded := newTestTrie(t)
	metadata, err := loaded.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != 2 {
		t.Fatalf("snapshot journal sequence = %d, want 2", metadata.JournalSequence)
	}
	if got := loaded.GetCounter("views"); got != 3 {
		t.Fatalf("loaded snapshot views = %d, want 3", got)
	}

	replayJournal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(loaded, metadata.JournalSequence); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := loaded.GetCounter("views"); got != 6 {
		t.Fatalf("loaded views after replay = %d, want 6", got)
	}
}

func TestCommandJournalIgnoresPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"})

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.WriteString(`{"version":1`); err != nil {
		file.Close()
		t.Fatalf("WriteString() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("name"); got != "ivi" {
		t.Fatalf("replayed name = %q, want ivi", got)
	}

	_ = replayJournal.ExecuteCommand(replayed, CacheCommandRequest{Command: "SETSTR", Key: "city", Value: "Singapore"})
	replayedAgain := newTestTrie(t)
	replayJournalAgain, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay again) error = %v", err)
	}
	if _, err := replayJournalAgain.Replay(replayedAgain, 0); err != nil {
		t.Fatalf("Replay(again) error = %v", err)
	}
	if got := replayedAgain.GetString("city"); got != "Singapore" {
		t.Fatalf("replayed city after partial tail append = %q, want Singapore", got)
	}
}

func TestOpenCommandJournalRejectsUnsupportedVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	if err := os.WriteFile(path, []byte(`{"version":99,"sequence":1,"request":{"command":"SETSTR","key":"name","value":"ivi"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := OpenCommandJournal(path); err == nil {
		t.Fatal("OpenCommandJournal(unsupported version) error = nil, want error")
	}
}
