package hatCache

import (
	"path/filepath"
	"testing"
)

func TestRehearseRestoreComparesRecoveredStateChecksums(t *testing.T) {
	dir := t.TempDir()
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(dir, "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "before", Value: "snapshot"}); !response.OK {
		t.Fatalf("before SETSTR response = %#v, want ok", response)
	}
	if err := journal.SaveSnapshotWithFormat(trie, filepath.Join(dir, "snapshot.hc"), SnapshotFormatJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "after", Value: "journal"}); !response.OK {
		t.Fatalf("after SETSTR response = %#v, want ok", response)
	}

	report, err := RehearseRestore(dir, RestoreRehearsalOptions{
		WorkDir: filepath.Join(t.TempDir(), "rehearsal"),
	})
	if err != nil {
		t.Fatalf("RehearseRestore() error = %v", err)
	}
	if !report.OK || !report.StateChecksumsMatch {
		t.Fatalf("rehearsal report = %#v, want successful checksum comparison", report)
	}
	if report.SourceStateChecksum == "" || report.RestoredStateChecksum == "" {
		t.Fatalf("rehearsal checksums = %q/%q, want both populated", report.SourceStateChecksum, report.RestoredStateChecksum)
	}
	if report.SourceStateChecksum != report.RestoredStateChecksum || report.Backup.StateChecksum != report.Restored.StateChecksum {
		t.Fatalf("rehearsal checksum mismatch = %#v, want source/restored equality", report)
	}
}
