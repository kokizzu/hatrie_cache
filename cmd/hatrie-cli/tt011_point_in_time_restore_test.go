package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"hatrie_cache/hat/hatCache"
)

func TestRunRestoreBundlePassesMaxJournalSequence(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("state", "snapshot")
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := hatCache.CreateBackupBundle(bundlePath, trie, nil, hatCache.BackupBundleOptions{SnapshotFormat: hatCache.SnapshotFormatJSON}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	var stdout, stderr bytes.Buffer
	err := runRestoreBundle([]string{
		"-bundle", bundlePath,
		"-data-dir", filepath.Join(t.TempDir(), "restored"),
		"-max-journal-sequence", "1",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "after backup sequence") {
		t.Fatalf("runRestoreBundle(max journal sequence) error = %v, want sequence validation", err)
	}
}
