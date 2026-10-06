package hatCache

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestCommandJournalBackupRetentionLeaseCapsSnapshotCompaction(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for sequence := 1; sequence <= 3; sequence++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "events",
			Value:   string(rune('0' + sequence)),
		})
		if !response.OK {
			t.Fatalf("journal write %d = %#v", sequence, response)
		}
	}

	lease, err := journal.AcquireBackupRetentionLease(1)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Sequence() != 1 {
		t.Fatalf("lease.Sequence() = %d, want 1", lease.Sequence())
	}
	if err := journal.SaveSnapshot(trie, filepath.Join(t.TempDir(), "snapshot.hc")); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Tail(0, 10); !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("Tail(0) error = %v, want compacted", err)
	}
	if tail, err := journal.Tail(1, 10); err != nil || len(tail.Entries) != 2 {
		t.Fatalf("Tail(1) = %#v, %v, want two retained entries", tail, err)
	}
	if !lease.Release() {
		t.Fatal("lease.Release() = false, want true")
	}
	if lease.Release() {
		t.Fatal("second lease.Release() = true, want false")
	}

	if err := journal.SaveSnapshot(trie, filepath.Join(t.TempDir(), "snapshot-released.hc")); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Tail(1, 10); !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("Tail(1) after release error = %v, want compacted", err)
	}
}

func TestCommandJournalBackupRetentionLeasesUseOldestCoordinate(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for sequence := 1; sequence <= 3; sequence++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "events",
			Value:   string(rune('0' + sequence)),
		})
		if !response.OK {
			t.Fatalf("journal write %d = %#v", sequence, response)
		}
	}

	oldest, err := journal.AcquireBackupRetentionLease(1)
	if err != nil {
		t.Fatal(err)
	}
	newest, err := journal.AcquireBackupRetentionLease(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.SaveSnapshot(trie, filepath.Join(t.TempDir(), "snapshot-oldest.hc")); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Tail(1, 10); err != nil {
		t.Fatalf("Tail(1) with oldest lease error = %v", err)
	}
	if !oldest.Release() || newest.Release() {
		t.Fatal("lease release results do not match active state")
	}
	if err := journal.SaveSnapshot(trie, filepath.Join(t.TempDir(), "snapshot-newest.hc")); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Tail(1, 10); !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("Tail(1) after oldest release error = %v, want compacted", err)
	}
}

func TestCommandJournalBackupRetentionLeaseRejectsInvalidCoordinates(t *testing.T) {
	var nilJournal *CommandJournal
	if _, err := nilJournal.AcquireBackupRetentionLease(0); !errors.Is(err, ErrNilCommandJournal) {
		t.Fatalf("nil journal error = %v, want ErrNilCommandJournal", err)
	}

	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := journal.AcquireBackupRetentionLease(1); err == nil {
		t.Fatal("future lease coordinate error = nil")
	}
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "events", Value: "one"}); !response.OK {
		t.Fatalf("journal write = %#v", response)
	}
	if _, err := journal.AcquireBackupRetentionLease(2); err == nil {
		t.Fatal("future lease coordinate after write error = nil")
	}
}

func TestCommandJournalSnapshotBackupRetentionLeaseCapturesCoordinate(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "events", Value: "one"}); !response.OK {
		t.Fatalf("journal write = %#v", response)
	}

	var snapshot bytes.Buffer
	manifest, lease, err := journal.WriteSnapshotWithManifestAndBackupRetentionLease(trie, &snapshot, SnapshotFormatBinary)
	if err != nil {
		t.Fatal(err)
	}
	if lease == nil || manifest.JournalSequence != 1 || lease.Sequence() != 1 {
		t.Fatalf("snapshot coordinate = %d, lease = %#v, want sequence 1", manifest.JournalSequence, lease)
	}
	if !lease.Release() {
		t.Fatal("lease.Release() = false, want true")
	}
}
