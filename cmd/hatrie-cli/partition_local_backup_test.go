package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	hatCache "hatrie_cache/hat/hatCache"
)

func TestRunBackupPartitionLocalSendsFilteredBackupRequest(t *testing.T) {
	var gotRequest struct {
		PartitionLocal bool `json:"partition_local"`
		Partition      struct {
			Partitions  []string `json:"partitions"`
			KeyPrefixes []string `json:"key_prefixes"`
		} `json:"partition"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/backup" {
			t.Fatalf("request path = %q, want /api/backup", req.URL.Path)
		}
		if err := json.NewDecoder(req.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode(request) error = %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"version":1}`))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	err := runBackup(context.Background(), server.Client(), server.URL, []string{
		"-path", "backup/sg.tar.gz",
		"-mode", "snapshot",
		"-partition-local",
		"-partitions", "sg",
		"-partition-prefixes", "sg:",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runBackup() error = %v; stderr=%s", err, stderr.String())
	}
	if !gotRequest.PartitionLocal || len(gotRequest.Partition.Partitions) != 1 || gotRequest.Partition.Partitions[0] != "sg" || len(gotRequest.Partition.KeyPrefixes) != 1 || gotRequest.Partition.KeyPrefixes[0] != "sg:" {
		t.Fatalf("backup request = %#v, want local sg partition", gotRequest)
	}
}

func TestRunRestoreBundlePartitionSelectorRestoresOnlySelectedRegion(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("sg:key", "local")
	trie.UpsertString("us:key", "foreign")
	bundlePath := filepath.Join(t.TempDir(), "sg.tar.gz")
	if _, err := hatCache.CreateBackupBundle(bundlePath, trie, nil, hatCache.BackupBundleOptions{
		Mode:           hatCache.BackupModeSnapshot,
		Partition:      hatCache.BackupPartitionMetadata{Partitions: []string{"sg"}, KeyPrefixes: []string{"sg:"}},
		PartitionLocal: true,
	}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	var stdout, stderr bytes.Buffer
	if err := runRestoreBundle([]string{
		"-bundle", bundlePath,
		"-data-dir", dataDir,
		"-partitions", "sg",
		"-partition-prefixes", "sg:",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("runRestoreBundle() error = %v; stderr=%s", err, stderr.String())
	}
	restored := hatCache.CreateHatTrie()
	defer restored.Destroy()
	if err := restored.LoadSnapshot(filepath.Join(dataDir, "snapshot.hc")); err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	if got := restored.GetString("sg:key"); got != "local" {
		t.Fatalf("restored local value = %q, want local", got)
	}
	if got := restored.GetString("us:key"); got != "" {
		t.Fatalf("restored foreign value = %q, want missing", got)
	}
}
