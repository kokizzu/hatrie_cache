package hatCache

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestT212ReplicaRetentionPinsUntilEveryReplicaAcknowledges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:                   CommandJournalFormatBinary,
		GroupCommitMaxBatch:      1,
		SegmentMaxBytes:          256,
		RetainedSegments:         1,
		ReplicaRetentionCapacity: 2,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	if err := journal.RegisterReplicaRetention("node-b", 0); err != nil {
		t.Fatalf("RegisterReplicaRetention(node-b) error = %v", err)
	}
	if err := journal.RegisterReplicaRetention("node-c", 0); err != nil {
		t.Fatalf("RegisterReplicaRetention(node-c) error = %v", err)
	}

	trie := newTestTrie(t)
	for index := 0; index < 6; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("t212:%d", index),
			Value:   strings.Repeat("value", 64),
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v, want success", index, response)
		}
	}
	segments, err := listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments() error = %v", err)
	}
	if len(segments) <= 1 {
		t.Fatalf("segments with two lagging replicas = %d, want more than retention limit", len(segments))
	}

	sequence := journal.Sequence()
	if err := journal.AcknowledgeReplicaThrough("node-b", sequence); err != nil {
		t.Fatalf("AcknowledgeReplicaThrough(node-b) error = %v", err)
	}
	segments, err = listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments(after node-b) error = %v", err)
	}
	if len(segments) <= 1 {
		t.Fatalf("segments with node-c lagging = %d, want more than retention limit", len(segments))
	}

	if err := journal.AcknowledgeReplicaThrough("node-c", sequence); err != nil {
		t.Fatalf("AcknowledgeReplicaThrough(node-c) error = %v", err)
	}
	segments, err = listCommandJournalSegments(path)
	if err != nil {
		t.Fatalf("listCommandJournalSegments(after node-c) error = %v", err)
	}
	if len(segments) > 1 {
		t.Fatalf("segments after every replica acknowledged = %d, want <= 1", len(segments))
	}
}

func TestT212ReplicaRetentionValidatesCapacityAndMonotonicAcknowledgements(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitMaxBatch:      1,
		ReplicaRetentionCapacity: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	if err := journal.RegisterReplicaRetention("node-b", 0); err != nil {
		t.Fatalf("RegisterReplicaRetention() error = %v", err)
	}
	trie := newTestTrie(t)
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "t212", Value: "value"}); !response.OK {
		t.Fatalf("ExecuteCommand() = %#v, want success", response)
	}
	if err := journal.RegisterReplicaRetention("node-c", 0); !errors.Is(err, ErrCommandJournalReplicaRetentionCapacityExceeded) {
		t.Fatalf("second replica error = %v, want capacity error", err)
	}
	if err := journal.AcknowledgeReplicaThrough("node-b", 2); !errors.Is(err, ErrCommandJournalReplicaRetentionFuture) {
		t.Fatalf("future acknowledgement error = %v, want future error", err)
	}
	if err := journal.AcknowledgeReplicaThrough("node-b", 1); err != nil {
		t.Fatalf("acknowledgement error = %v", err)
	}
	if err := journal.AcknowledgeReplicaThrough("node-b", 0); !errors.Is(err, ErrCommandJournalReplicaRetentionRegression) {
		t.Fatalf("regressed acknowledgement error = %v, want regression error", err)
	}
	if err := journal.RegisterReplicaRetention(" ", 0); !errors.Is(err, ErrCommandJournalReplicaRetentionIDInvalid) {
		t.Fatalf("empty replica error = %v, want id error", err)
	}
}

func TestT212ReplicaRetentionIsOptIn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{GroupCommitMaxBatch: 1})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	if err := journal.RegisterReplicaRetention("node-b", 0); !errors.Is(err, ErrCommandJournalReplicaRetentionDisabled) {
		t.Fatalf("default registration error = %v, want disabled error", err)
	}
}
