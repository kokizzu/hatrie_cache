package hatCache

import (
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
)

func newTT051BenchmarkTrie(b *testing.B, path string, partitions int) *HatTrie {
	b.Helper()
	trie, err := CreateHatTrieWithDiskDir(path, true)
	if err != nil {
		b.Fatal(err)
	}
	if partitions > 0 {
		if err := trie.ConfigureLocalPartitions(partitions); err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
	}
	return trie
}

func tt051BenchmarkKeys(b *testing.B, trie *HatTrie, partitions int) []string {
	b.Helper()
	keys := make([]string, partitions)
	seen := make(map[int]struct{}, partitions)
	for index := 0; len(seen) < partitions; index++ {
		key := "tt051:benchmark:" + strconv.Itoa(index)
		partition, enabled, err := trie.LocalPartitionForKey(key)
		if err != nil {
			b.Fatal(err)
		}
		if enabled {
			if _, exists := seen[partition]; !exists {
				seen[partition] = struct{}{}
				keys[partition] = key
			}
		}
	}
	return keys
}

func BenchmarkTT051GlobalSynchronousJournal(b *testing.B) {
	trie := newTT051BenchmarkTrie(b, filepath.Join(b.TempDir(), "trie"), 0)
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "tt051:key:" + strconv.Itoa(index%1024),
			Value:   "value",
		})
		if !response.OK {
			b.Fatal(response.Message)
		}
	}
}

func BenchmarkTT051GlobalSynchronousJournalParallel(b *testing.B) {
	trie := newTT051BenchmarkTrie(b, filepath.Join(b.TempDir(), "trie"), 0)
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	var index uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sequence := atomic.AddUint64(&index, 1)
			journal.ExecuteCommand(trie, CacheCommandRequest{
				Command: "SETSTR",
				Key:     "tt051:key:" + strconv.FormatUint(sequence%1024, 10),
				Value:   "value",
			})
		}
	})
}

func BenchmarkTT051PartitionedSynchronousJournal(b *testing.B) {
	const partitions = 4
	trie := newTT051BenchmarkTrie(b, filepath.Join(b.TempDir(), "trie"), partitions)
	defer trie.Destroy()
	keys := tt051BenchmarkKeys(b, trie, partitions)
	journal, err := OpenPartitionedCommandJournal(filepath.Join(b.TempDir(), "journals"), PartitionedCommandJournalOptions{
		Partitions: partitions,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     keys[index%partitions],
			Value:   "value",
		})
		if !response.OK {
			b.Fatal(response.Message)
		}
	}
}

func BenchmarkTT051PartitionedSynchronousJournalParallel(b *testing.B) {
	const partitions = 4
	trie := newTT051BenchmarkTrie(b, filepath.Join(b.TempDir(), "trie"), partitions)
	defer trie.Destroy()
	keys := tt051BenchmarkKeys(b, trie, partitions)
	journal, err := OpenPartitionedCommandJournal(filepath.Join(b.TempDir(), "journals"), PartitionedCommandJournalOptions{
		Partitions: partitions,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	var index uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sequence := atomic.AddUint64(&index, 1)
			journal.ExecuteCommand(trie, CacheCommandRequest{
				Command: "SETSTR",
				Key:     keys[sequence%uint64(partitions)],
				Value:   "value",
			})
		}
	})
}
