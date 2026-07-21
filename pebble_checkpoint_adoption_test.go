package hatriecache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPebbleStoreAdoptCheckpointReplacesOpenGeneration(t *testing.T) {
	root := t.TempDir()
	activePath := filepath.Join(root, "active.pebble")
	active, err := OpenPebbleStore(activePath)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	oldTrie := newTestTrie(t)
	oldTrie.UpsertString("stale", "remove")
	oldTrie.UpsertString("generation", "old")
	if err := active.Save(oldTrie); err != nil {
		t.Fatal(err)
	}

	source, err := OpenPebbleStore(filepath.Join(root, "source.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	newTrie := newTestTrie(t)
	newTrie.UpsertString("generation", "new")
	newTrie.UpsertString("fresh", "value")
	checkpoint := filepath.Join(root, "checkpoint.pebble")
	if err := source.SaveCheckpoint(newTrie, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	if err := active.AdoptCheckpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	if active.Path() != activePath {
		t.Fatalf("active path = %q, want %q", active.Path(), activePath)
	}
	if _, err := os.Stat(checkpoint); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("adopted checkpoint Stat() error = %v, want not exist", err)
	}
	restored := newTestTrie(t)
	if _, err := active.Load(restored); err != nil {
		t.Fatal(err)
	}
	if restored.Exists("stale") || restored.GetString("generation") != "new" || restored.GetString("fresh") != "value" {
		t.Fatalf("adopted state = %#v", restored.Entries(true))
	}

	restored.UpsertString("after", "write")
	if err := active.SaveKeys(restored, []string{"after"}); err != nil {
		t.Fatal(err)
	}
	if err := active.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPebbleStore(activePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after := newTestTrie(t)
	if _, err := reopened.Load(after); err != nil {
		t.Fatal(err)
	}
	if after.GetString("after") != "write" || after.GetString("generation") != "new" {
		t.Fatalf("reopened adopted state = %#v", after.Entries(true))
	}
}

func TestPebbleStoreAdoptCheckpointRollsBackPublishedFailure(t *testing.T) {
	root := t.TempDir()
	active, err := OpenPebbleStore(filepath.Join(root, "active.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	oldTrie := newTestTrie(t)
	oldTrie.UpsertString("generation", "old")
	if err := active.Save(oldTrie); err != nil {
		t.Fatal(err)
	}

	source, err := OpenPebbleStore(filepath.Join(root, "source.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	newTrie := newTestTrie(t)
	newTrie.UpsertString("generation", "new")
	checkpoint := filepath.Join(root, "checkpoint.pebble")
	if err := source.SaveCheckpoint(newTrie, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("injected adoption failure")
	active.checkpointAdoptHook = func(stage string) error {
		if stage == "after-publish" {
			return wantErr
		}
		return nil
	}
	if err := active.AdoptCheckpoint(checkpoint); !errors.Is(err, wantErr) {
		t.Fatalf("AdoptCheckpoint() error = %v, want %v", err, wantErr)
	}
	restored := newTestTrie(t)
	if _, err := active.Load(restored); err != nil {
		t.Fatal(err)
	}
	if got := restored.GetString("generation"); got != "old" {
		t.Fatalf("generation after rollback = %q, want old", got)
	}
	if _, err := os.Stat(checkpoint); err != nil {
		t.Fatalf("rolled-back checkpoint Stat() error = %v", err)
	}
}

func TestOpenPebbleStoreRecoversInterruptedCheckpointAdoption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active.pebble")
	store, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatal(err)
	}
	trie := newTestTrie(t)
	trie.UpsertString("generation", "old")
	if err := store.Save(trie); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	oldPath := pebbleCheckpointAdoptionOldPath(path)
	if err := os.Rename(path, oldPath); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	loaded := newTestTrie(t)
	if _, err := recovered.Load(loaded); err != nil {
		t.Fatal(err)
	}
	if got := loaded.GetString("generation"); got != "old" {
		t.Fatalf("recovered generation = %q, want old", got)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery old path Stat() error = %v, want not exist", err)
	}
}
