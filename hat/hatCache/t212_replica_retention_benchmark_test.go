//go:build t212

package hatCache

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkT212DefaultSegmentPrune(b *testing.B) {
	j, err := openT212BenchmarkJournal(b, 0, 1024)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		j.mu.Lock()
		err = j.pruneSegmentsLocked()
		j.mu.Unlock()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT212ReplicaRetentionFloor8(b *testing.B) {
	j, err := openT212BenchmarkJournal(b, 8, 1)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		j.mu.Lock()
		err = j.pruneSegmentsLocked()
		j.mu.Unlock()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT212ReplicaRetentionFloorLookup8(b *testing.B) {
	j := &CommandJournal{
		replicaRetentionCapacity:    8,
		replicaRetentionInitialized: true,
		replicaRetentionAcks:        make(map[string]uint64, 8),
	}
	for index := 0; index < 8; index++ {
		j.replicaRetentionAcks[fmt.Sprintf("replica-%02d", index)] = uint64(index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		j.mu.Lock()
		_, _ = j.replicaRetentionThroughLocked()
		j.mu.Unlock()
	}
}

func openT212BenchmarkJournal(b *testing.B, replicaCapacity, retainedSegments int) (*CommandJournal, error) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "commands.journal")
	j, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:                   CommandJournalFormatBinary,
		GroupCommitMaxBatch:      1,
		SegmentMaxBytes:          256,
		RetainedSegments:         retainedSegments,
		ReplicaRetentionCapacity: replicaCapacity,
	})
	if err != nil {
		return nil, err
	}
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < replicaCapacity; index++ {
		if err := j.AcknowledgeReplicaThrough(fmt.Sprintf("replica-%02d", index), 0); err != nil {
			j.Close()
			return nil, fmt.Errorf("AcknowledgeReplicaThrough(%d) = %v", index, err)
		}
	}
	for index := 0; index < 32; index++ {
		response := j.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     fmt.Sprintf("t212-benchmark:%d", index),
			Value:   strings.Repeat("value", 64),
		})
		if !response.OK {
			j.Close()
			return nil, fmt.Errorf("ExecuteCommand(%d) = %#v", index, response)
		}
	}
	return j, nil
}
