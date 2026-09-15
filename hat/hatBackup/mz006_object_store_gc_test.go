package hatBackup_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func TestMZ006ObjectStoreGarbageCollectionPlansAndDeletesOnlyOrphans(t *testing.T) {
	store := newMZ006ObjectStore()
	keepHash := strings.Repeat("1", 64)
	orphanHash := strings.Repeat("2", 64)
	store.put("backup/objects/"+keepHash, []byte("keep"))
	store.put("backup/objects/"+orphanHash, []byte("orphan"))
	store.put("backup/objects/incomplete", []byte("leave unknown objects alone"))
	store.put("backup/manifest.json", []byte("metadata"))
	target, err := hatBackup.NewObjectStoreTarget(store, "backup")
	if err != nil {
		t.Fatal(err)
	}
	retention, err := hatBackup.PlanBackupRetention([]hatBackup.BundleManifest{
		mz006Manifest("base", "", false, keepHash),
	}, "base", 1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := target.PlanGarbageCollection(context.Background(), retention)
	if err != nil {
		t.Fatalf("PlanGarbageCollection() error = %v", err)
	}
	wantDelete := "backup/objects/" + orphanHash
	if len(plan.DeleteObjectKeys) != 1 || plan.DeleteObjectKeys[0] != wantDelete {
		t.Fatalf("DeleteObjectKeys = %#v, want [%q]", plan.DeleteObjectKeys, wantDelete)
	}
	if len(store.deleted) != 0 {
		t.Fatalf("planning deleted objects = %#v", store.deleted)
	}
	if len(plan.SkippedObjectKeys) != 1 || plan.SkippedObjectKeys[0] != "backup/objects/incomplete" {
		t.Fatalf("SkippedObjectKeys = %#v", plan.SkippedObjectKeys)
	}
	deleted, err := target.ApplyGarbageCollection(context.Background(), plan)
	if err != nil {
		t.Fatalf("ApplyGarbageCollection() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if store.has(wantDelete) || !store.has("backup/objects/"+keepHash) || !store.has("backup/objects/incomplete") || !store.has("backup/manifest.json") {
		t.Fatalf("garbage collection changed an unexpected object: %#v", store.keys())
	}
}

func TestMZ006ObjectStoreGarbageCollectionRejectsUnsafeInputs(t *testing.T) {
	store := newMZ006ObjectStore()
	target, err := hatBackup.NewObjectStoreTarget(store, "backup")
	if err != nil {
		t.Fatal(err)
	}
	pathManifest := mz006Manifest("base", "", false, strings.Repeat("3", 64))
	pathManifest.ObjectLayout = string(hatBackup.ObjectStoreLayoutPath)
	retention, err := hatBackup.PlanBackupRetention([]hatBackup.BundleManifest{pathManifest}, "base", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.PlanGarbageCollection(context.Background(), retention); !errors.Is(err, hatBackup.ErrObjectStoreGarbageCollectionUnsafe) {
		t.Fatalf("path-layout plan error = %v, want ErrObjectStoreGarbageCollectionUnsafe", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := target.PlanGarbageCollection(ctx, hatBackup.BackupRetentionPlan{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled plan error = %v, want context.Canceled", err)
	}
	if _, err := target.ApplyGarbageCollection(context.Background(), hatBackup.ObjectStoreGarbageCollectionPlan{
		Prefix:           "other",
		DeleteObjectKeys: []string{"other/objects/" + strings.Repeat("4", 64)},
	}); !errors.Is(err, hatBackup.ErrObjectStoreGarbageCollectionPlanInvalid) {
		t.Fatalf("wrong-prefix apply error = %v, want ErrObjectStoreGarbageCollectionPlanInvalid", err)
	}
	if _, err := target.ApplyGarbageCollection(context.Background(), hatBackup.ObjectStoreGarbageCollectionPlan{
		Prefix:           "backup",
		DeleteObjectKeys: []string{"backup/objects/../manifest.json"},
	}); !errors.Is(err, hatBackup.ErrObjectStoreGarbageCollectionPlanInvalid) {
		t.Fatalf("manifest delete error = %v, want ErrObjectStoreGarbageCollectionPlanInvalid", err)
	}
}

func TestMZ006ObjectStoreGarbageCollectionRefusesMissingKeepAndUnsupportedStore(t *testing.T) {
	keepHash := strings.Repeat("a", 64)
	retention, err := hatBackup.PlanBackupRetention([]hatBackup.BundleManifest{
		mz006Manifest("base", "", false, keepHash),
	}, "base", 1)
	if err != nil {
		t.Fatal(err)
	}
	store := newMZ006ObjectStore()
	store.put("backup/objects/"+strings.ToUpper(keepHash), []byte("noncanonical spelling"))
	store.put("backup/objects/"+strings.Repeat("6", 64), []byte("orphan"))
	target, err := hatBackup.NewObjectStoreTarget(store, "backup")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.PlanGarbageCollection(context.Background(), retention); !errors.Is(err, hatBackup.ErrObjectStoreGarbageCollectionKeepMissing) {
		t.Fatalf("missing keep plan error = %v, want ErrObjectStoreGarbageCollectionKeepMissing", err)
	}

	minimal, err := hatBackup.NewObjectStoreTarget(mz006MinimalObjectStore{}, "backup")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := minimal.PlanGarbageCollection(context.Background(), retention); !errors.Is(err, hatBackup.ErrObjectStoreGarbageCollectionUnsupported) {
		t.Fatalf("minimal store plan error = %v, want ErrObjectStoreGarbageCollectionUnsupported", err)
	}
}

func mz006Manifest(id, parent string, incremental bool, hash string) hatBackup.BundleManifest {
	return hatBackup.BundleManifest{
		Version:           hatBackup.BundleVersion,
		Mode:              hatBackup.ModePebbleIncremental,
		BackupID:          id,
		ParentBackupID:    parent,
		Incremental:       incremental,
		Store:             "store",
		StorageBackend:    "pebble",
		StorageFormat:     "sst",
		StorageIdentity:   "db",
		StorageGeneration: 1,
		ObjectLayout:      string(hatBackup.ObjectStoreLayoutContentAddressed),
		Files:             []hatBackup.BundleFile{{Path: "part/rows.bin", Size: 5, SHA256: hash}},
	}
}

type mz006ObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	deleted []string
}

func newMZ006ObjectStore() *mz006ObjectStore {
	return &mz006ObjectStore{objects: make(map[string][]byte)}
}

func (store *mz006ObjectStore) Put(_ context.Context, key string, body io.Reader, _ int64) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	store.put(key, data)
	return nil
}

func (store *mz006ObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	store.mu.Lock()
	data, ok := store.objects[key]
	store.mu.Unlock()
	if !ok {
		return nil, errors.New("object not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (store *mz006ObjectStore) List(_ context.Context, prefix string) ([]hatBackup.ObjectStoreObject, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	objects := make([]hatBackup.ObjectStoreObject, 0)
	for key, data := range store.objects {
		if strings.HasPrefix(key, prefix) {
			objects = append(objects, hatBackup.ObjectStoreObject{Key: key, Size: int64(len(data))})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

func (store *mz006ObjectStore) Delete(_ context.Context, key string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.objects[key]; !ok {
		return nil
	}
	delete(store.objects, key)
	store.deleted = append(store.deleted, key)
	return nil
}

func (store *mz006ObjectStore) put(key string, data []byte) {
	store.mu.Lock()
	store.objects[key] = append([]byte(nil), data...)
	store.mu.Unlock()
}

func (store *mz006ObjectStore) has(key string) bool {
	store.mu.Lock()
	_, ok := store.objects[key]
	store.mu.Unlock()
	return ok
}

func (store *mz006ObjectStore) keys() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0, len(store.objects))
	for key := range store.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type mz006MinimalObjectStore struct{}

func (mz006MinimalObjectStore) Put(context.Context, string, io.Reader, int64) error {
	return nil
}

func (mz006MinimalObjectStore) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("object not found")
}
