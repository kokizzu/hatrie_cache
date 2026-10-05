package hatCache

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCommandJournalReplicaRetentionProtectsUnacknowledgedSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	if err := journal.SetReplicaWatermark("node-b", 0); err != nil {
		t.Fatalf("SetReplicaWatermark(initial) error = %v", err)
	}
	trie := newTestTrie(t)
	for index := 0; index < 8; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "replica-retention-key-" + string(rune('a'+index)),
			Value:   "replica-retention-value",
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}
	trie.Destroy()

	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments(before ack) error = %v", err)
	}
	if len(segments) < 3 {
		t.Fatalf("segments before replica ack = %d, want at least 3", len(segments))
	}
	if err := journal.SetReplicaWatermark("node-b", 6); err != nil {
		t.Fatalf("SetReplicaWatermark(6) error = %v", err)
	}
	journal.mu.Lock()
	err = journal.pruneSegmentsLocked()
	journal.mu.Unlock()
	if err != nil {
		t.Fatalf("pruneSegmentsLocked() error = %v", err)
	}

	segments, err = listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments(after ack) error = %v", err)
	}
	if len(segments) == 0 {
		t.Fatal("replica acknowledgment removed every archived segment")
	}
	for _, segment := range segments {
		if segment.end <= 6 {
			t.Fatalf("segment %d-%d remains after acknowledgment through 6", segment.start, segment.end)
		}
	}
	if got := journal.ReplicaWatermarks(); !reflect.DeepEqual(got, []CommandJournalReplicaWatermark{{Replica: "node-b", Sequence: 6, Lag: 2}}) {
		t.Fatalf("ReplicaWatermarks() = %#v", got)
	}
}

func TestCommandJournalReplicaRetentionUsesMinimumAckAndValidatesProgress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	trie := newTestTrie(t)
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "replica-key", Value: "value"})
	trie.Destroy()
	if !response.OK {
		t.Fatalf("ExecuteCommand() = %#v, want ok", response)
	}
	if err := journal.SetReplicaWatermark(" node-b ", 0); err != nil {
		t.Fatalf("SetReplicaWatermark(node-b) error = %v", err)
	}
	if err := journal.SetReplicaWatermark("node-a", 1); err != nil {
		t.Fatalf("SetReplicaWatermark(node-a) error = %v", err)
	}
	if err := journal.SetReplicaWatermark("node-b", 1); err != nil {
		t.Fatalf("SetReplicaWatermark(node-b,1) error = %v", err)
	}
	if err := journal.SetReplicaWatermark("node-a", 0); err == nil {
		t.Fatal("SetReplicaWatermark() accepted regressed progress")
	}
	if err := journal.SetReplicaWatermark("node-a", 2); err == nil {
		t.Fatal("SetReplicaWatermark() accepted a future sequence")
	}
	if err := journal.SetReplicaWatermark(" ", 1); err == nil {
		t.Fatal("SetReplicaWatermark() accepted an empty name")
	}
	if err := journal.SetReplicaWatermark(strings.Repeat("x", MaxCommandJournalReplicaWatermarkNameBytes+1), 1); err == nil {
		t.Fatal("SetReplicaWatermark() accepted an oversized name")
	}

	journal.mu.Lock()
	through, protected := journal.replicaRetentionThroughLocked()
	journal.mu.Unlock()
	if !protected || through != 1 {
		t.Fatalf("replicaRetentionThroughLocked() = %d/%t, want 1/true", through, protected)
	}
	want := []CommandJournalReplicaWatermark{
		{Replica: "node-a", Sequence: 1, Lag: 0},
		{Replica: "node-b", Sequence: 1, Lag: 0},
	}
	if got := journal.ReplicaWatermarks(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ReplicaWatermarks() = %#v, want %#v", got, want)
	}
	if !journal.RemoveReplicaWatermark("node-a") || journal.RemoveReplicaWatermark("node-a") {
		t.Fatal("RemoveReplicaWatermark() did not report exact removal")
	}
	if !journal.RemoveReplicaWatermark("node-b") {
		t.Fatal("RemoveReplicaWatermark(node-b) = false, want true")
	}
	journal.mu.Lock()
	_, protected = journal.replicaRetentionThroughLocked()
	journal.mu.Unlock()
	if protected {
		t.Fatal("replica retention remained protected after removing every watermark")
	}
}

func TestCommandJournalReplicaRetentionDoesNotRemoveActiveJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
		SegmentMaxBytes:     1,
		RetainedSegments:    1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "active-key", Value: "value"})
	trie.Destroy()
	if !response.OK {
		t.Fatalf("ExecuteCommand() = %#v, want ok", response)
	}
	if err := journal.SetReplicaWatermark("node-a", journal.lastSequenceLocked()); err != nil {
		journal.Close()
		t.Fatalf("SetReplicaWatermark() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("active journal disappeared after retention = %v", err)
	}
}
