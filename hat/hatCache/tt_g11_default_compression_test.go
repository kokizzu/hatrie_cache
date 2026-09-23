package hatCache

import (
	"path/filepath"
	"testing"
)

func TestTTG11DefaultArchivedJournalCompression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    8,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	for index := 0; index < 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "default-compression:" + string(rune('a'+index)),
			Value:   "value",
		})
		if !response.OK {
			_ = journal.Close()
			t.Fatalf("ExecuteCommand(%d) = %#v, want success", index, response)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments() error = %v", err)
	}
	if len(segments) == 0 {
		t.Fatal("default compression produced no archived segments")
	}
	for _, segment := range segments {
		if segment.compression != CommandJournalSegmentCompressionZstd {
			t.Fatalf("default segment compression = %q, want %q", segment.compression, CommandJournalSegmentCompressionZstd)
		}
	}
}

func TestTTG11ExplicitNoneCompressionRemainsAvailable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    8,
		SegmentCompression:  CommandJournalSegmentCompressionNone,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "none", Value: "value"})
	if !response.OK {
		_ = journal.Close()
		t.Fatalf("ExecuteCommand() = %#v, want success", response)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments() error = %v", err)
	}
	if len(segments) == 0 {
		t.Fatal("explicit none compression produced no archived segments")
	}
	for _, segment := range segments {
		if segment.compression != CommandJournalSegmentCompressionNone {
			t.Fatalf("explicit none segment compression = %q, want none", segment.compression)
		}
	}
}
