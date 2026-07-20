package hatriecache

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkLocalPartitionRestore100k(b *testing.B) {
	keys := localPartitionRestoreBenchmarkInt("HATRIE_PARTITION_RESTORE_KEYS", 100000)
	partitions := localPartitionRestoreBenchmarkInt("HATRIE_PARTITION_RESTORE_COUNT", 16)
	root := b.TempDir()
	source, err := CreateHatTrieWithDiskDir(filepath.Join(root, "source-trie"), false)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(source.Destroy)
	value := strings.Repeat("x", 256)
	for index := 0; index < keys; index++ {
		source.UpsertString(fmt.Sprintf("restore:%09d", index), value)
	}
	snapshotPath := filepath.Join(root, "source.snapshot")
	if err := source.SaveSnapshotWithFormat(snapshotPath, SnapshotFormatBinary); err != nil {
		b.Fatal(err)
	}
	store, err := OpenPebbleStore(filepath.Join(root, "source.pebble"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	if err := store.Save(source); err != nil {
		b.Fatal(err)
	}

	for _, test := range []struct {
		name string
		load func(*HatTrie) error
	}{
		{name: "Snapshot", load: func(target *HatTrie) error { return target.LoadSnapshot(snapshotPath) }},
		{name: "Pebble", load: func(target *HatTrie) error {
			_, err := store.LoadWithPolicy(target, LevelDBLoadPolicy{})
			return err
		}},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				target, err := CreateHatTrieWithDiskDir(filepath.Join(root, test.name+"-"+strconv.Itoa(iteration)), false)
				if err != nil {
					b.Fatal(err)
				}
				if err := target.ConfigureLocalPartitions(partitions); err != nil {
					target.Destroy()
					b.Fatal(err)
				}
				target.UpsertString("stale", "remove")
				b.StartTimer()
				err = test.load(target)
				b.StopTimer()
				if err != nil {
					target.Destroy()
					b.Fatal(err)
				}
				if target.Size() != keys || target.Exists("stale") || target.GetString("restore:000000000") != value {
					target.Destroy()
					b.Fatal("restored partition state mismatch")
				}
				target.Destroy()
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(keys), "keys/op")
			b.ReportMetric(float64(partitions), "partitions/op")
		})
	}
}

func localPartitionRestoreBenchmarkInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
