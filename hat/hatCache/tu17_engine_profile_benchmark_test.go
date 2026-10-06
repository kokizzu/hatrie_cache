package hatCache

import (
	"path/filepath"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func BenchmarkTU17ExplicitLSMSelection(b *testing.B) {
	profile, err := hatStorage.ProfileForBackend(hatStorage.BackendPebble)
	if err != nil {
		b.Fatal(err)
	}
	for _, benchmark := range []struct {
		name string
		open func(string) (PersistentStore, error)
	}{
		{
			name: "backend-selector",
			open: func(path string) (PersistentStore, error) {
				return OpenPersistentStoreWithFormat(path, StorageBackendPebble, StorageFormatBinary)
			},
		},
		{
			name: "explicit-profile",
			open: func(path string) (PersistentStore, error) {
				return OpenPersistentStoreWithProfile(path, profile, StorageFormatBinary)
			},
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			root := b.TempDir()
			paths := make([]string, b.N)
			for index := range paths {
				paths[index] = filepath.Join(root, strconv.Itoa(index))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for _, path := range paths {
				store, err := benchmark.open(path)
				if err != nil {
					b.Fatal(err)
				}
				if err := store.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
