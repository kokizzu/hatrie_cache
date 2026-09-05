package hatCache

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hatrie_cache/hat/hatJournal"
)

func TestSegmentedCommandJournalZstdCompressionReplaysAndInspects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	options := CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    8,
		SegmentCompression:  CommandJournalSegmentCompressionZstd,
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	for index := 0; index < 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "compressed:" + strings.Repeat("key", 8),
			Value:   strings.Repeat("value", 32),
		})
		if !response.OK {
			journal.Close()
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
	if len(segments) < 2 {
		t.Fatalf("segments = %d, want at least 2 archived segments", len(segments))
	}
	for _, segment := range segments {
		if !strings.HasSuffix(segment.path, ".journal.zst") {
			t.Fatalf("segment path = %q, want zstd suffix", segment.path)
		}
		payload, err := os.ReadFile(segment.path)
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", segment.path, err)
		}
		if !bytes.HasPrefix(payload, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
			t.Fatalf("segment %q does not contain a zstd frame", segment.path)
		}
	}
	portableSegments, err := hatJournal.ListSegments(path)
	if err != nil {
		t.Fatalf("hatJournal.ListSegments() error = %v", err)
	}
	if len(portableSegments) != len(segments) {
		t.Fatalf("portable segments = %d, want %d", len(portableSegments), len(segments))
	}
	for _, segment := range portableSegments {
		if segment.Compression != hatJournal.SegmentCompressionZstd {
			t.Fatalf("portable segment compression = %q, want zstd", segment.Compression)
		}
	}

	inspection, err := InspectCommandJournal(path, options)
	if err != nil {
		t.Fatalf("InspectCommandJournal() error = %v", err)
	}
	if inspection.RecordCount != 5 || len(inspection.Segments) != len(segments) {
		t.Fatalf("inspection = %#v, want 5 records/%d segments", inspection, len(segments))
	}

	reopened, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	replayed := newTestTrie(t)
	if sequence, err := reopened.Replay(replayed, 0); err != nil || sequence != 3 {
		t.Fatalf("Replay() sequence/error = %d/%v, want 3/nil", sequence, err)
	}
	if got := replayed.GetString("compressed:" + strings.Repeat("key", 8)); got != strings.Repeat("value", 32) {
		t.Fatalf("replayed compressed value = %q, want original value", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close before corruption check = %v", err)
	}
	corrupted, err := os.OpenFile(segments[0].path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile(corrupted segment) error = %v", err)
	}
	info, err := corrupted.Stat()
	if err != nil || info.Size() < 1 {
		_ = corrupted.Close()
		t.Fatalf("Stat(corrupted segment) = %v/%v, want non-empty file", info, err)
	}
	if _, err := corrupted.WriteAt([]byte{0}, info.Size()-1); err != nil {
		_ = corrupted.Close()
		t.Fatalf("WriteAt(corrupted segment) error = %v", err)
	}
	if err := corrupted.Close(); err != nil {
		t.Fatalf("Close(corrupted segment) error = %v", err)
	}
	if _, err := InspectCommandJournal(path, options); err == nil {
		t.Fatal("InspectCommandJournal() accepted a corrupted zstd segment")
	}
}
