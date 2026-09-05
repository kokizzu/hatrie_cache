package hatCache

import (
	"path/filepath"
	"testing"
)

func TestVerifyBackupPathChecksZstdSegmentedDirectoryJournal(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "commands.journal")
	trie := newTestTrie(t)
	journal, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    8,
		SegmentCompression:  CommandJournalSegmentCompressionZstd,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	for _, key := range []string{"before:1", "before:2"} {
		if got := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: key, Value: "snapshot"}); !got.OK {
			t.Fatalf("journaled %s response = %#v, want ok", key, got)
		}
	}
	if err := journal.SaveSnapshotWithFormat(trie, filepath.Join(dir, "snapshot.hc"), SnapshotFormatJSON); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}
	if got := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "after", Value: "journal"}); !got.OK {
		t.Fatalf("journaled after response = %#v, want ok", got)
	}

	report, err := VerifyBackupPath(dir)
	if err != nil {
		t.Fatalf("VerifyBackupPath(zstd segmented directory) error = %v", err)
	}
	if !report.OK || report.Journal == nil || report.Journal.Entries != 3 {
		t.Fatalf("zstd segmented directory doctor report = %#v, want three verified journal entries", report)
	}
	if report.RecoveredKeys != 3 || report.JournalSequence != 3 {
		t.Fatalf("zstd segmented recovered keys/sequence = %d/%d, want 3/3", report.RecoveredKeys, report.JournalSequence)
	}
}
