package hatCache

import (
	"path/filepath"
	"reflect"
	"testing"
)

var tt012StoreSink PersistentStore

func TestTT012PersistentSpaceStoreSetKeepsBackendChoicePerSpace(t *testing.T) {
	root := t.TempDir()
	configs := []PersistentSpaceStoreConfig{
		{
			Name:    "hot",
			Path:    filepath.Join(root, "hot.pebble"),
			Backend: StorageBackendPebble,
			Format:  StorageFormatBinary,
		},
		{
			Name:    "archive",
			Path:    filepath.Join(root, "archive.leveldb"),
			Backend: StorageBackendLevelDB,
			Format:  StorageFormatBinary,
		},
	}

	spaces, err := OpenPersistentSpaceStoreSet(configs)
	if err != nil {
		t.Fatalf("OpenPersistentSpaceStoreSet() error = %v", err)
	}
	hot, ok := spaces.Store("hot")
	if !ok || hot.Backend() != StorageBackendPebble {
		t.Fatalf("hot store = %#v/%v, want Pebble/true", hot, ok)
	}
	archive, ok := spaces.Store("archive")
	if !ok || archive.Backend() != StorageBackendLevelDB {
		t.Fatalf("archive store = %#v/%v, want LevelDB/true", archive, ok)
	}
	if got := spaces.Names(); !reflect.DeepEqual(got, []string{"archive", "hot"}) {
		t.Fatalf("Names() = %v, want sorted names", got)
	}

	hotTrie := newTestTrie(t)
	hotTrie.UpsertString("temperature", "hot")
	if err := hot.Save(hotTrie); err != nil {
		t.Fatalf("hot Save() error = %v", err)
	}
	archiveTrie := newTestTrie(t)
	archiveTrie.UpsertString("temperature", "archive")
	if err := archive.Save(archiveTrie); err != nil {
		t.Fatalf("archive Save() error = %v", err)
	}
	if err := spaces.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	spaces, err = OpenPersistentSpaceStoreSet(configs)
	if err != nil {
		t.Fatalf("OpenPersistentSpaceStoreSet(reopen) error = %v", err)
	}
	defer spaces.Close()
	for name, want := range map[string]string{"hot": "hot", "archive": "archive"} {
		store, ok := spaces.Store(name)
		if !ok {
			t.Fatalf("Store(%q) missing after reopen", name)
		}
		trie := newTestTrie(t)
		if loaded, err := store.Load(trie); err != nil || loaded != 1 || trie.GetString("temperature") != want {
			t.Fatalf("%s Load() = %d/%v value=%q, want 1/nil/%q", name, loaded, err, trie.GetString("temperature"), want)
		}
	}
}

func TestTT012PersistentSpaceStoreSetRejectsAmbiguousConfiguration(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "same")
	cases := []struct {
		name    string
		configs []PersistentSpaceStoreConfig
	}{
		{
			name: "empty name",
			configs: []PersistentSpaceStoreConfig{{
				Path:    path,
				Backend: StorageBackendPebble,
				Format:  StorageFormatBinary,
			}},
		},
		{
			name: "duplicate name",
			configs: []PersistentSpaceStoreConfig{
				{Name: "same", Path: filepath.Join(root, "one"), Backend: StorageBackendPebble, Format: StorageFormatBinary},
				{Name: "same", Path: filepath.Join(root, "two"), Backend: StorageBackendLevelDB, Format: StorageFormatBinary},
			},
		},
		{
			name: "duplicate path",
			configs: []PersistentSpaceStoreConfig{
				{Name: "one", Path: path, Backend: StorageBackendPebble, Format: StorageFormatBinary},
				{Name: "two", Path: path, Backend: StorageBackendPebble, Format: StorageFormatBinary},
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			spaces, err := OpenPersistentSpaceStoreSet(test.configs)
			if err == nil {
				_ = spaces.Close()
				t.Fatal("OpenPersistentSpaceStoreSet() error = nil")
			}
		})
	}
}

func BenchmarkTT012PersistentSpaceStoreLookup(b *testing.B) {
	root := b.TempDir()
	configs := make([]PersistentSpaceStoreConfig, 0, 4)
	for _, name := range []string{"hot", "archive", "search", "metrics"} {
		configs = append(configs, PersistentSpaceStoreConfig{
			Name:    name,
			Path:    filepath.Join(root, name),
			Backend: StorageBackendPebble,
			Format:  StorageFormatBinary,
		})
	}
	spaces, err := OpenPersistentSpaceStoreSet(configs)
	if err != nil {
		b.Fatalf("OpenPersistentSpaceStoreSet() error = %v", err)
	}
	defer spaces.Close()
	names := []string{"hot", "archive", "search", "metrics"}
	direct := make(map[string]PersistentStore, len(names))
	for _, name := range names {
		store, ok := spaces.Store(name)
		if !ok {
			b.Fatalf("Store(%q) missing", name)
		}
		direct[name] = store
	}

	b.Run("Registry", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			store, ok := spaces.Store(names[index&3])
			if !ok {
				b.Fatal("registry lookup failed")
			}
			tt012StoreSink = store
		}
	})
	b.Run("DirectMap", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			tt012StoreSink = direct[names[index&3]]
		}
	})
}
