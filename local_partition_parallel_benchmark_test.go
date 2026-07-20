package hatriecache

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var (
	benchmarkWholeKeyspaceKeys    []string
	benchmarkWholeKeyspaceEntries []Entry
)

func BenchmarkLocalPartitionWholeKeyspace100k(b *testing.B) {
	keyCount := 100000
	if configured := os.Getenv("HATRIE_PARTITION_SCAN_BENCH_KEYS"); configured != "" {
		parsed, err := strconv.Atoi(configured)
		if err != nil || parsed <= 0 {
			b.Fatalf("invalid HATRIE_PARTITION_SCAN_BENCH_KEYS %q", configured)
		}
		keyCount = parsed
	}
	trie, err := CreateHatTrieWithDiskDir(b.TempDir(), false)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(trie.Destroy)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		b.Fatal(err)
	}
	value := strings.Repeat("v", 64)
	for index := 0; index < keyCount; index++ {
		trie.UpsertString(fmt.Sprintf("whole-keyspace:%08d", index), value)
	}
	set := trie.localPartitionSet()

	b.Run("SerialSortedKeys", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			keys, err := serialLocalPartitionKeys(set, "", true)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkWholeKeyspaceKeys = keys
		}
		b.ReportMetric(float64(keyCount), "keys/op")
	})
	b.Run("CurrentSortedKeys", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkWholeKeyspaceKeys = trie.Keys(true)
		}
		b.ReportMetric(float64(keyCount), "keys/op")
	})
	b.Run("SerialSortedEntries", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			entries, err := serialLocalPartitionEntries(set, "", true)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkWholeKeyspaceEntries = entries
		}
		b.ReportMetric(float64(keyCount), "entries/op")
	})
	b.Run("CurrentSortedEntries", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkWholeKeyspaceEntries = trie.Entries(true)
		}
		b.ReportMetric(float64(keyCount), "entries/op")
	})
}

func serialLocalPartitionKeys(set *localPartitionSet, prefix string, sortedKeys bool) ([]string, error) {
	result := make([]string, 0)
	for _, child := range set.tries {
		keys, err := child.KeysWithPrefixChecked(prefix, false)
		if err != nil {
			return nil, err
		}
		result = append(result, keys...)
	}
	if sortedKeys {
		sort.Strings(result)
	}
	return result, nil
}

func serialLocalPartitionEntries(set *localPartitionSet, prefix string, sortedEntries bool) ([]Entry, error) {
	result := make([]Entry, 0)
	for _, child := range set.tries {
		entries, err := child.EntriesWithPrefixChecked(prefix, false)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	if sortedEntries {
		sort.Slice(result, func(left int, right int) bool { return result[left].Key < result[right].Key })
	}
	return result, nil
}
