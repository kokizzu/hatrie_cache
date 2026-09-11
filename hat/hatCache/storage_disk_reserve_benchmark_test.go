package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkTT016PersistentStoreSave(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		reserve int64
	}{
		{name: "disabled", reserve: 0},
		{name: "enabled", reserve: 1},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			store, err := OpenPersistentStoreWithFormat(filepath.Join(b.TempDir(), "cache"), StorageBackendPebble, StorageFormatBinary)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			if err := ConfigurePersistentStoreDiskReserveBytes(store, benchmark.reserve); err != nil {
				b.Fatal(err)
			}
			trie := CreateHatTrie()
			defer trie.Destroy()
			for index := 0; index < 128; index++ {
				trie.UpsertString(fmt.Sprintf("key-%03d", index), "value")
			}
			if err := store.Save(trie); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; b.Loop(); iteration++ {
				trie.UpsertString("mutable", fmt.Sprintf("value-%d", iteration))
				if err := store.Save(trie); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
