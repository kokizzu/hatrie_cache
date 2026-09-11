package hatCache

import (
	"strings"
	"testing"
)

func TestBackupBundleSelectsKeyPrefixesAndRestores(t *testing.T) {
	for _, format := range []SnapshotFormat{SnapshotFormatJSON, SnapshotFormatBinary} {
		t.Run(string(format), func(t *testing.T) {
			source := CreateHatTrie()
			defer source.Destroy()
			source.UpsertString("region:sg/user:1", "Singapore")
			source.UpsertCounter("region:sg/visits", 42)
			source.UpsertBytes("region:sg/blob", []byte("payload"))
			source.UpsertString("region:us/user:1", "United States")
			source.UpsertString("global/config", "excluded")

			bundlePath := t.TempDir() + "/selected.tar.gz"
			manifest, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
				Mode:           BackupModeSnapshot,
				SnapshotFormat: format,
				KeyPrefixes:    []string{"region:sg/"},
			})
			if err != nil {
				t.Fatalf("CreateBackupBundle(%s) error = %v", format, err)
			}
			if len(manifest.KeyPrefixes) != 1 || manifest.KeyPrefixes[0] != "region:sg/" {
				t.Fatalf("manifest.KeyPrefixes = %#v, want [region:sg/]", manifest.KeyPrefixes)
			}
			storedManifest, err := readBackupBundleManifest(bundlePath)
			if err != nil {
				t.Fatalf("readBackupBundleManifest() error = %v", err)
			}
			if len(storedManifest.KeyPrefixes) != 1 || storedManifest.KeyPrefixes[0] != "region:sg/" {
				t.Fatalf("stored manifest.KeyPrefixes = %#v, want [region:sg/]", storedManifest.KeyPrefixes)
			}

			restoreDir := t.TempDir() + "/restored"
			report, err := RestoreBackupBundle(bundlePath, restoreDir, BackupBundleRestoreOptions{})
			if err != nil {
				t.Fatalf("RestoreBackupBundle() error = %v", err)
			}
			restored := newTestTrie(t)
			defer restored.Destroy()
			if err := restored.LoadSnapshot(report.Snapshot); err != nil {
				t.Fatalf("LoadSnapshot() error = %v", err)
			}
			keys, err := restored.KeysWithPrefixChecked("", true)
			if err != nil {
				t.Fatalf("restored KeysWithPrefixChecked() error = %v", err)
			}
			want := []string{"region:sg/blob", "region:sg/user:1", "region:sg/visits"}
			if strings.Join(keys, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("restored keys = %#v, want %#v", keys, want)
			}
			if restored.Exists("region:us/user:1") {
				t.Fatal("restored excluded region:us key")
			}
			if restored.Exists("global/config") {
				t.Fatal("restored excluded global key")
			}
		})
	}
}

func TestBackupBundleRejectsSelectivePrefixesForPersistentModes(t *testing.T) {
	for _, mode := range []BackupMode{BackupModePebbleCheckpoint, BackupModePebbleIncremental} {
		t.Run(string(mode), func(t *testing.T) {
			_, err := CreateBackupBundle(t.TempDir()+"/selected", newTestTrie(t), nil, BackupBundleOptions{
				Mode:        mode,
				KeyPrefixes: []string{"region:sg/"},
			})
			if err == nil || !strings.Contains(err.Error(), "key prefixes require snapshot mode") {
				t.Fatalf("CreateBackupBundle(%s) error = %v, want snapshot-mode error", mode, err)
			}
		})
	}
}

func TestBackupBundleRejectsInvalidSelectivePrefixes(t *testing.T) {
	for _, prefixes := range [][]string{{""}, {"region:sg/", "region:sg/"}} {
		_, err := CreateBackupBundle(t.TempDir()+"/selected", newTestTrie(t), nil, BackupBundleOptions{
			Mode:        BackupModeSnapshot,
			KeyPrefixes: prefixes,
		})
		if err == nil {
			t.Fatalf("CreateBackupBundle(%#v) error = nil, want validation error", prefixes)
		}
	}
}

func TestBackupBundleRejectsCombinedSelectivePartitionLocal(t *testing.T) {
	_, err := CreateBackupBundle(t.TempDir()+"/selected", newTestTrie(t), nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		KeyPrefixes:    []string{"region:sg/"},
		Partition:      BackupPartitionMetadata{Partitions: []string{"sg"}, KeyPrefixes: []string{"region:sg/"}},
		PartitionLocal: true,
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined with partition-local mode") {
		t.Fatalf("CreateBackupBundle() error = %v, want partition-local conflict", err)
	}
}
