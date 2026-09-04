package hatCache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentedCommandJournalRetainsWithinByteBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	options := CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    16,
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	for index := 0; index < 8; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "retention-key",
			Value:   "retention-value",
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}
	trie.Destroy()
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments() error = %v", err)
	}
	if len(segments) < 3 {
		t.Fatalf("segments before byte pruning = %d, want at least 3", len(segments))
	}
	newestInfo, err := os.Stat(segments[len(segments)-1].path)
	if err != nil {
		t.Fatalf("Stat(newest segment) error = %v", err)
	}
	budget := newestInfo.Size()
	if budget <= 0 {
		t.Fatalf("newest segment size = %d, want positive", budget)
	}

	options.RetainedBytes = budget
	reopened, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions(reopen) error = %v", err)
	}
	defer reopened.Close()

	segments, err = listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments(after reopen) error = %v", err)
	}
	if len(segments) == 0 {
		t.Fatal("byte pruning removed every archived segment")
	}
	var totalBytes int64
	for _, segment := range segments {
		info, statErr := os.Stat(segment.path)
		if statErr != nil {
			t.Fatalf("Stat(%q) error = %v", segment.path, statErr)
		}
		totalBytes += info.Size()
	}
	if totalBytes > budget {
		t.Fatalf("retained segment bytes = %d, want <= %d", totalBytes, budget)
	}
}
