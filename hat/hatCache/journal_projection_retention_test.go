package hatCache

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestCommandJournalProjectionWatermarkCapsSnapshotCompaction(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for sequence := 1; sequence <= 3; sequence++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "events", Value: string(rune('0' + sequence))})
		if !response.OK {
			t.Fatalf("journal write %d = %#v", sequence, response)
		}
	}
	if err := journal.SetProjectionWatermark("event_totals", 1); err != nil {
		t.Fatal(err)
	}
	watermarks := journal.ProjectionWatermarks()
	if len(watermarks) != 1 || watermarks[0].Name != "event_totals" || watermarks[0].Sequence != 1 || watermarks[0].Lag != 2 {
		t.Fatalf("ProjectionWatermarks() = %#v", watermarks)
	}
	if err := journal.SaveSnapshot(trie, filepath.Join(t.TempDir(), "snapshot.hc")); err != nil {
		t.Fatal(err)
	}
	tail, err := journal.Tail(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if tail.CompactedThrough != 1 || len(tail.Entries) != 2 || tail.Entries[0].Sequence != 2 || tail.Entries[1].Sequence != 3 {
		t.Fatalf("Tail(1) = %#v", tail)
	}
	if _, err := journal.Tail(0, 10); !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("Tail(0) error = %v, want compacted", err)
	}
}

func TestCommandJournalProjectionWatermarksAreMonotonicAndRemovable(t *testing.T) {
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
	if err := journal.SetProjectionWatermark("event_totals", 1); err != nil {
		t.Fatal(err)
	}
	if err := journal.SetProjectionWatermark("event_totals", 0); err == nil {
		t.Fatal("SetProjectionWatermark() error = nil, want monotonicity failure")
	}
	if err := journal.SetProjectionWatermark("event_totals", 2); err == nil {
		t.Fatal("SetProjectionWatermark() error = nil, want future sequence failure")
	}
	if removed := journal.RemoveProjectionWatermark("event_totals"); !removed {
		t.Fatal("RemoveProjectionWatermark() = false, want true")
	}
	if watermarks := journal.ProjectionWatermarks(); len(watermarks) != 0 {
		t.Fatalf("ProjectionWatermarks() after removal = %#v", watermarks)
	}
}
