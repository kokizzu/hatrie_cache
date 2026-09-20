package hatCache

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const tt051PartitionCount = 4

func newTT051PartitionedTrie(t *testing.T, path string, partitions int) *HatTrie {
	t.Helper()
	trie, err := CreateHatTrieWithDiskDir(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := trie.ConfigureLocalPartitions(partitions); err != nil {
		trie.Destroy()
		t.Fatal(err)
	}
	return trie
}

func tt051KeysByPartition(t *testing.T, trie *HatTrie, partitions int) []string {
	t.Helper()
	keys := make([]string, partitions)
	seen := make(map[int]struct{}, partitions)
	for index := 0; len(seen) < partitions; index++ {
		key := fmt.Sprintf("tt051:key:%d", index)
		partition, enabled, err := trie.LocalPartitionForKey(key)
		if err != nil {
			t.Fatal(err)
		}
		if !enabled {
			t.Fatal("local partitions are disabled")
		}
		if _, exists := seen[partition]; exists {
			continue
		}
		seen[partition] = struct{}{}
		keys[partition] = key
	}
	return keys
}

func tt051AnotherKeyInPartition(t *testing.T, trie *HatTrie, partition int, excluded string) string {
	t.Helper()
	for index := 0; ; index++ {
		key := fmt.Sprintf("tt051:same:%d", index)
		if key == excluded {
			continue
		}
		candidate, enabled, err := trie.LocalPartitionForKey(key)
		if err != nil {
			t.Fatal(err)
		}
		if enabled && candidate == partition {
			return key
		}
	}
}

func TestPartitionedCommandJournalRoutesAndReplaysPerPartition(t *testing.T) {
	journalRoot := filepath.Join(t.TempDir(), "journals")
	trie := newTT051PartitionedTrie(t, filepath.Join(t.TempDir(), "trie"), tt051PartitionCount)
	defer trie.Destroy()
	keys := tt051KeysByPartition(t, trie, tt051PartitionCount)

	journal, err := OpenPartitionedCommandJournal(journalRoot, PartitionedCommandJournalOptions{
		Partitions: tt051PartitionCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	for partition, key := range keys {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     key,
			Value:   fmt.Sprintf("value-%d", partition),
		})
		if !response.OK {
			t.Fatalf("partition %d ExecuteCommand() = %#v", partition, response)
		}
		if got, err := journal.Sequence(partition); err != nil || got != 1 {
			t.Fatalf("partition %d sequence = %d/%v, want 1", partition, got, err)
		}
		path, err := journal.PartitionPath(partition)
		if err != nil {
			t.Fatal(err)
		}
		wantPath := filepath.Join(journalRoot, fmt.Sprintf("partition-%03d", partition), "commands.journal")
		if path != wantPath {
			t.Fatalf("partition %d journal path = %q, want %q", partition, path, wantPath)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	for partition := range keys {
		path := filepath.Join(journalRoot, fmt.Sprintf("partition-%03d", partition), "commands.journal")
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("partition %d journal stat: %v", partition, err)
		}
	}

	replayed := newTT051PartitionedTrie(t, filepath.Join(t.TempDir(), "replayed"), tt051PartitionCount)
	defer replayed.Destroy()
	reopened, err := OpenPartitionedCommandJournal(journalRoot, PartitionedCommandJournalOptions{
		Partitions: tt051PartitionCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	sequences, err := reopened.Replay(replayed, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if want := []uint64{1, 1, 1, 1}; !reflect.DeepEqual(sequences, want) {
		t.Fatalf("replay sequences = %#v, want %#v", sequences, want)
	}
	for partition, key := range keys {
		if got := replayed.GetString(key); got != fmt.Sprintf("value-%d", partition) {
			t.Fatalf("replayed partition %d value = %q", partition, got)
		}
	}
}

func TestPartitionedCommandJournalAllowsSamePartitionBatchAndRejectsCrossPartitionBatch(t *testing.T) {
	trie := newTT051PartitionedTrie(t, filepath.Join(t.TempDir(), "trie"), tt051PartitionCount)
	defer trie.Destroy()
	keys := tt051KeysByPartition(t, trie, tt051PartitionCount)
	journal, err := OpenPartitionedCommandJournal(filepath.Join(t.TempDir(), "journals"), PartitionedCommandJournalOptions{
		Partitions: tt051PartitionCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	samePartition := keys[0]
	samePartitionSecond := tt051AnotherKeyInPartition(t, trie, 0, samePartition)
	response := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: samePartition, Value: "a"},
			{Command: "SETSTR", Key: samePartitionSecond, Value: "b"},
		},
	})
	if !response.OK {
		t.Fatalf("same-partition batch = %#v", response)
	}

	response = journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: keys[0], Value: "cross-a"},
			{Command: "SETSTR", Key: keys[1], Value: "cross-b"},
		},
	})
	if response.OK || !strings.Contains(response.Message, "one local partition") {
		t.Fatalf("cross-partition batch = %#v", response)
	}
	if got, err := journal.Sequence(1); err != nil || got != 0 {
		t.Fatalf("rejected partition sequence = %d/%v, want 0", got, err)
	}
}

func TestPartitionedCommandJournalRejectsPartitionMismatch(t *testing.T) {
	trie := newTT051PartitionedTrie(t, filepath.Join(t.TempDir(), "trie"), 2)
	defer trie.Destroy()
	journal, err := OpenPartitionedCommandJournal(filepath.Join(t.TempDir(), "journals"), PartitionedCommandJournalOptions{
		Partitions: tt051PartitionCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "mismatch", Value: "value"})
	if response.OK || !strings.Contains(response.Message, "partition count") {
		t.Fatalf("partition mismatch response = %#v", response)
	}
}
