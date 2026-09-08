package hatCache

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// TestBackupRestoreRoundTripsAllSnapshotValueTypes proves that every value
// accepted by the snapshot format survives each backup transport and store mode.
func TestBackupRestoreRoundTripsAllSnapshotValueTypes(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)

	for _, format := range []SnapshotFormat{
		SnapshotFormatBinary,
		SnapshotFormatGzipBinary,
		SnapshotFormatGzipBestBinary,
		SnapshotFormatJSON,
		SnapshotFormatGzipJSON,
		SnapshotFormatGzipBestJSON,
	} {
		t.Run("snapshot/"+string(format), func(t *testing.T) {
			source := newBackupRoundTripCorpus(t, now)
			want := backupRoundTripEntries(t, source)
			bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
			if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{Mode: BackupModeSnapshot, SnapshotFormat: format}); err != nil {
				t.Fatalf("CreateBackupBundle(%s) error = %v", format, err)
			}
			report, err := RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{})
			if err != nil {
				t.Fatalf("RestoreBackupBundle(%s) error = %v", format, err)
			}
			restored := newTestTrie(t)
			restored.now = func() time.Time { return now }
			if err := restored.LoadSnapshot(report.Snapshot); err != nil {
				t.Fatalf("LoadSnapshot(%s) error = %v", format, err)
			}
			assertBackupRoundTripEntries(t, want, backupRoundTripEntries(t, restored))
		})
	}

	t.Run("pebble-checkpoint", func(t *testing.T) {
		source := newBackupRoundTripCorpus(t, now)
		want := backupRoundTripEntries(t, source)
		store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
		if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{Mode: BackupModePebbleCheckpoint, PersistentStore: store}); err != nil {
			t.Fatalf("CreateBackupBundle(checkpoint) error = %v", err)
		}
		restored := restoreBackupRoundTripStore(t, bundlePath, now)
		assertBackupRoundTripEntries(t, want, backupRoundTripEntries(t, restored))
	})

	t.Run("pebble-incremental", func(t *testing.T) {
		source := newBackupRoundTripCorpus(t, now)
		want := backupRoundTripEntries(t, source)
		store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		repository := filepath.Join(t.TempDir(), "repository")
		if _, err := CreateBackupBundle(repository, source, nil, BackupBundleOptions{
			Mode:            BackupModePebbleIncremental,
			PersistentStore: store,
			DirtyTracker:    NewLevelDBDirtyTracker(),
		}); err != nil {
			t.Fatalf("CreateBackupBundle(incremental) error = %v", err)
		}
		restored := restoreBackupRoundTripStore(t, repository, now)
		assertBackupRoundTripEntries(t, want, backupRoundTripEntries(t, restored))
	})
}

func newBackupRoundTripCorpus(t *testing.T, now time.Time) *HatTrie {
	return newBackupRoundTripCorpusWithPrefix(t, now, "")
}

func TestPartitionLocalBackupRoundTripsAllSnapshotValueTypes(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	partition := BackupPartitionMetadata{Partitions: []string{"sg"}, KeyPrefixes: []string{"sg:"}}
	for _, format := range []SnapshotFormat{
		SnapshotFormatBinary,
		SnapshotFormatGzipBestBinary,
		SnapshotFormatJSON,
		SnapshotFormatGzipBestJSON,
	} {
		t.Run(string(format), func(t *testing.T) {
			source := newBackupRoundTripCorpusWithPrefix(t, now, "sg:")
			want := backupRoundTripEntries(t, source)
			bundlePath := filepath.Join(t.TempDir(), "sg-backup.tar.gz")
			manifest, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
				Mode:           BackupModeSnapshot,
				SnapshotFormat: format,
				Partition:      partition,
				PartitionLocal: true,
			})
			if err != nil {
				t.Fatalf("CreateBackupBundle(%s) error = %v", format, err)
			}
			if manifest.Partition == nil || !manifest.Partition.Local {
				t.Fatalf("manifest partition = %#v, want local metadata", manifest.Partition)
			}
			report, err := RestoreBackupBundle(bundlePath, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{
				Partition: &BackupPartitionMetadata{Partitions: []string{"sg"}},
			})
			if err != nil {
				t.Fatalf("RestoreBackupBundle(%s) error = %v", format, err)
			}
			restored := newTestTrie(t)
			restored.now = func() time.Time { return now }
			if err := restored.LoadSnapshot(report.Snapshot); err != nil {
				t.Fatalf("LoadSnapshot(%s) error = %v", format, err)
			}
			assertBackupRoundTripEntries(t, want, backupRoundTripEntries(t, restored))
		})
	}
}

func newBackupRoundTripCorpusWithPrefix(t *testing.T, now time.Time, prefix string) *HatTrie {
	t.Helper()
	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	key := func(name string) string { return prefix + name }
	ht.UpsertCounter(key("counter"), 42)
	ht.UpsertString(key("string"), "value")
	ht.UpsertBytes(key("bytes"), []byte("payload"))
	ht.UpsertMap(key("map"), Map{"name": "ivi", "age": json.Number("32")})
	ht.UpsertSlice(key("slice"), Slice{"a", json.Number("2")})
	ht.UpsertSet(key("set"), Set{"a", json.Number("2"), "a"})
	ht.UpsertPriorityQueue(key("priority"), PriorityQueue{{Priority: 5, Value: json.Number("2")}, {Priority: 1, Value: "urgent"}})
	if err := ht.UpsertBloomFilter(key("bloom"), 1000, 0.001); err != nil {
		t.Fatal(err)
	}
	ht.AddBloomFilter(key("bloom"), "alpha", "beta")
	if err := ht.UpsertCountMinSketch(key("freq"), 128, 4); err != nil {
		t.Fatal(err)
	}
	ht.IncrementCountMinSketch(key("freq"), "alpha", 5)
	if err := ht.UpsertHyperLogLog(key("card"), 10); err != nil {
		t.Fatal(err)
	}
	ht.AddHyperLogLog(key("card"), "alpha", "beta")
	if err := ht.UpsertTopK(key("top"), 3); err != nil {
		t.Fatal(err)
	}
	ht.AddTopK(key("top"), "alpha", 5)
	if err := ht.UpsertQuantileSketch(key("latency"), 0.01); err != nil {
		t.Fatal(err)
	}
	ht.AddQuantileSketch(key("latency"), 10, 20, 30)
	if err := ht.UpsertFenwickTree(key("scores"), 8); err != nil {
		t.Fatal(err)
	}
	ht.AddFenwickTree(key("scores"), 2, 5)
	ht.AddFenwickTree(key("scores"), 6, 7)
	if err := ht.UpsertCuckooFilter(key("cuckoo"), 128, 0.001); err != nil {
		t.Fatal(err)
	}
	ht.AddCuckooFilter(key("cuckoo"), "alpha", "beta")
	if err := ht.UpsertXorFilter(key("xor"), 8); err != nil {
		t.Fatal(err)
	}
	if _, err := ht.AddXorFilter(key("xor"), "alpha", "beta"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ht.BuildXorFilter(key("xor")); err != nil || !ok {
		t.Fatalf("BuildXorFilter() = %v/%v, want success", err, ok)
	}
	ht.UpsertRadixTree(key("radix"))
	ht.PutRadixTree(key("radix"), "user:100/profile", Map{"status": "active"})
	ht.PutRadixTree(key("radix"), "user:101/profile", json.Number("42"))
	ht.UpsertRoaringBitmap(key("bitmap"))
	ht.AddRoaringBitmap(key("bitmap"), 1, 1<<16+7)
	ht.UpsertSparseBitset(key("bitset"))
	ht.AddSparseBitset(key("bitset"), 1, 1<<32+7, ^uint64(0))
	if err := ht.UpsertReservoirSample(key("sample"), 3); err != nil {
		t.Fatal(err)
	}
	ht.AddReservoirSample(key("sample"), "alpha", "beta", "gamma", "delta")
	if !ht.Expire(key("string"), time.Minute) {
		t.Fatal("Expire(string) = false")
	}
	return ht
}

func restoreBackupRoundTripStore(t *testing.T, source string, now time.Time) *HatTrie {
	t.Helper()
	report, err := RestoreBackupBundle(source, filepath.Join(t.TempDir(), "restored"), BackupBundleRestoreOptions{})
	if err != nil {
		t.Fatalf("RestoreBackupBundle() error = %v", err)
	}
	store, err := OpenPersistentStore(report.Store)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restored := newTestTrie(t)
	restored.now = func() time.Time { return now }
	if _, err := store.Load(restored); err != nil {
		t.Fatal(err)
	}
	return restored
}

func backupRoundTripEntries(t *testing.T, ht *HatTrie) map[string]snapshotEntry {
	t.Helper()
	capture, err := ht.captureSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	entries := make(map[string]snapshotEntry, capture.count)
	for _, page := range capture.pages {
		for _, entry := range page {
			if entry.ExpiresAt != nil {
				expiresAt := entry.ExpiresAt.UTC()
				entry.ExpiresAt = &expiresAt
			}
			entries[entry.Key] = entry
		}
	}
	return entries
}

func assertBackupRoundTripEntries(t *testing.T, want map[string]snapshotEntry, got map[string]snapshotEntry) {
	t.Helper()
	if len(want) != 19 {
		t.Fatalf("backup corpus contains %d entries, want every 19 supported value types", len(want))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored canonical entries differ\n got: %#v\nwant: %#v", got, want)
	}
}
