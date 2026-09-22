//go:build t212

package hatCache

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestT212ReplicaRetentionIsOptIn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	if err := journal.AcknowledgeReplicaThrough("replica-a", 0); !errors.Is(err, ErrReplicaRetentionDisabled) {
		t.Fatalf("AcknowledgeReplicaThrough() error = %v, want ErrReplicaRetentionDisabled", err)
	}
}

func TestT212ReplicaRetentionPinsSegmentsUntilEveryReplicaAcknowledges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	options := CommandJournalOptions{
		Format:                   CommandJournalFormatBinary,
		GroupCommitMaxBatch:      1,
		SegmentMaxBytes:          256,
		RetainedSegments:         1,
		ReplicaRetentionCapacity: 2,
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	if err := journal.AcknowledgeReplicaThrough("slow", 0); err != nil {
		t.Fatalf("AcknowledgeReplicaThrough(slow, 0) error = %v", err)
	}
	trie := newTestTrie(t)
	for index := 0; index < 6; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("t212:%d", index),
			Value:   strings.Repeat("value", 64),
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}

	sequence := journal.Sequence()
	if err := journal.AcknowledgeReplicaThrough("fast", sequence); err != nil {
		t.Fatalf("AcknowledgeReplicaThrough(fast) error = %v", err)
	}
	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments() error = %v", err)
	}
	if len(segments) <= options.RetainedSegments {
		t.Fatalf("segments while slow replica is behind = %d, want > %d", len(segments), options.RetainedSegments)
	}

	snapshot := journal.ReplicaRetentionSnapshot()
	if len(snapshot) != 2 || snapshot[0].Replica != "fast" || snapshot[1].Replica != "slow" {
		t.Fatalf("ReplicaRetentionSnapshot() = %#v, want sorted fast/slow entries", snapshot)
	}
	if snapshot[0].AcknowledgedThrough != sequence || snapshot[1].AcknowledgedThrough != 0 {
		t.Fatalf("ReplicaRetentionSnapshot() acknowledgements = %#v, want fast=%d slow=0", snapshot, sequence)
	}

	if err := journal.AcknowledgeReplicaThrough("third", sequence); !errors.Is(err, ErrReplicaRetentionCapacityExceeded) {
		t.Fatalf("third replica error = %v, want ErrReplicaRetentionCapacityExceeded", err)
	}
	if err := journal.AcknowledgeReplicaThrough("slow", sequence); err != nil {
		t.Fatalf("AcknowledgeReplicaThrough(slow, sequence) error = %v", err)
	}
	if err := journal.AcknowledgeReplicaThrough("slow", sequence-1); !errors.Is(err, ErrReplicaRetentionAckRegressed) {
		t.Fatalf("regressed acknowledgement error = %v, want ErrReplicaRetentionAckRegressed", err)
	}
	if err := journal.AcknowledgeReplicaThrough("slow", sequence+1); !errors.Is(err, ErrReplicaRetentionSequenceInvalid) {
		t.Fatalf("future acknowledgement error = %v, want ErrReplicaRetentionSequenceInvalid", err)
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
	if len(segments) > options.RetainedSegments {
		t.Fatalf("segments after every replica acknowledged = %d, want <= %d", len(segments), options.RetainedSegments)
	}

	snapshot[0].Replica = "mutated"
	if journal.ReplicaRetentionSnapshot()[0].Replica != "fast" {
		t.Fatalf("ReplicaRetentionSnapshot() returned mutable internal state")
	}
	if !journal.RemoveReplicaRetention("fast") {
		t.Fatalf("RemoveReplicaRetention(fast) = false, want true")
	}
	if journal.RemoveReplicaRetention("missing") {
		t.Fatalf("RemoveReplicaRetention(missing) = true, want false")
	}
}

func TestT212ReplicaRetentionValidatesOptions(t *testing.T) {
	for _, capacity := range []int{-1, MaxReplicaRetentionCapacity + 1} {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("invalid-%d.journal", capacity))
		_, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
			Format:                   CommandJournalFormatBinary,
			GroupCommitMaxBatch:      1,
			ReplicaRetentionCapacity: capacity,
		})
		if err == nil {
			t.Fatalf("capacity %d opened successfully, want validation error", capacity)
		}
	}
}

func TestT212ReplicaRetentionCapsLegacyCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:                   CommandJournalFormatBinary,
		GroupCommitMaxBatch:      1,
		ReplicaRetentionCapacity: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	trie := newTestTrie(t)
	for index := 0; index < 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("compact:%d", index),
			Value:   "value",
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}
	if err := journal.AcknowledgeReplicaThrough("replica-a", 1); err != nil {
		t.Fatalf("AcknowledgeReplicaThrough() error = %v", err)
	}
	journal.mu.Lock()
	err = journal.compactLocked(3)
	journal.mu.Unlock()
	if err != nil {
		t.Fatalf("compactLocked(3) error = %v", err)
	}
	tail, err := journal.Tail(0, 0)
	if !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("Tail(0) error = %v, want ErrCommandJournalCompacted", err)
	}
	if tail.CompactedThrough != 1 || tail.LastSequence != 3 {
		t.Fatalf("Tail(0) = %#v, want compacted through 1 with last sequence 3", tail)
	}
}

func TestT212ReplicaRetentionReopenWaitsForFreshAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	options := CommandJournalOptions{
		Format:                   CommandJournalFormatBinary,
		GroupCommitMaxBatch:      1,
		SegmentMaxBytes:          256,
		RetainedSegments:         1,
		ReplicaRetentionCapacity: 1,
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	if err := journal.AcknowledgeReplicaThrough("replica-a", 0); err != nil {
		journal.Close()
		t.Fatalf("initial AcknowledgeReplicaThrough() error = %v", err)
	}
	trie := newTestTrie(t)
	for index := 0; index < 6; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("reopen:%d", index),
			Value:   strings.Repeat("value", 64),
		})
		if !response.OK {
			journal.Close()
			t.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions(reopen) error = %v", err)
	}
	defer reopened.Close()
	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments(reopen) error = %v", err)
	}
	if len(segments) <= options.RetainedSegments {
		t.Fatalf("reopened segments before acknowledgement = %d, want > %d", len(segments), options.RetainedSegments)
	}
	if err := reopened.AcknowledgeReplicaThrough("replica-a", reopened.Sequence()); err != nil {
		t.Fatalf("fresh AcknowledgeReplicaThrough() error = %v", err)
	}
	reopened.mu.Lock()
	err = reopened.pruneSegmentsLocked()
	reopened.mu.Unlock()
	if err != nil {
		t.Fatalf("pruneSegmentsLocked(after reopen ack) error = %v", err)
	}
	segments, err = listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments(after reopen ack) error = %v", err)
	}
	if len(segments) > options.RetainedSegments {
		t.Fatalf("reopened segments after acknowledgement = %d, want <= %d", len(segments), options.RetainedSegments)
	}
}
