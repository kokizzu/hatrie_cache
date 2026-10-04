package hatSchema

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
)

type tu20Legacy struct {
	Name string
	Age  int
}

type tu20Current struct {
	Name   string
	Age    int
	Active bool
}

type tu20Backend struct {
	mu          sync.Mutex
	old         map[string]tu20Legacy
	next        map[string]tu20Current
	dualWrites  int
	newWrites   int
	cutovers    int
	rollbacks   int
	failDualKey string
	stallScan   bool
	keys        []string
}

func newTU20Backend(rows map[string]tu20Legacy) *tu20Backend {
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return &tu20Backend{old: rows, next: make(map[string]tu20Current), keys: keys}
}

func (backend *tu20Backend) ReadOld(_ context.Context, key string) (tu20Legacy, bool, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	value, ok := backend.old[key]
	return value, ok, nil
}

func (backend *tu20Backend) ReadNew(_ context.Context, key string) (tu20Current, bool, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	value, ok := backend.next[key]
	return value, ok, nil
}

func (backend *tu20Backend) ScanOld(_ context.Context, after string, limit int) ([]OnlineSpaceUpgradeRecord[tu20Legacy], string, bool, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.stallScan {
		return nil, after, false, nil
	}
	start := 0
	if after != "" {
		for index, key := range backend.keys {
			if key == after {
				start = index + 1
				break
			}
		}
	}
	if start >= len(backend.keys) {
		return nil, after, true, nil
	}
	end := start + limit
	if end > len(backend.keys) {
		end = len(backend.keys)
	}
	records := make([]OnlineSpaceUpgradeRecord[tu20Legacy], 0, end-start)
	for _, key := range backend.keys[start:end] {
		records = append(records, OnlineSpaceUpgradeRecord[tu20Legacy]{Key: key, Value: backend.old[key]})
	}
	return records, backend.keys[end-1], end == len(backend.keys), nil
}

func (backend *tu20Backend) WriteDual(_ context.Context, key string, old tu20Legacy, next tu20Current) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.failDualKey == key {
		return errors.New("dual write failed")
	}
	backend.old[key] = old
	backend.next[key] = next
	backend.dualWrites++
	return nil
}

func (backend *tu20Backend) WriteNew(_ context.Context, key string, next tu20Current) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.next[key] = next
	backend.newWrites++
	return nil
}

func (backend *tu20Backend) Cutover(_ context.Context, _ string, _ uint64) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.cutovers++
	return nil
}

func (backend *tu20Backend) Rollback(_ context.Context, _ string, _ uint64) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.rollbacks++
	return nil
}

func newTU20Upgrade(backend *tu20Backend, batchSize int) (*OnlineSpaceUpgrade[tu20Legacy, tu20Current], error) {
	return NewOnlineSpaceUpgrade(OnlineSpaceUpgradeOptions[tu20Legacy, tu20Current]{
		Space:           "users",
		PreviousVersion: 1,
		NextVersion:     2,
		BatchSize:       batchSize,
		Convert: func(value tu20Legacy) (tu20Current, error) {
			return tu20Current{Name: value.Name, Age: value.Age, Active: true}, nil
		},
		Backend: backend,
	})
}

func TestOnlineSpaceUpgradeReadsOldAndDualWritesBeforeCutover(t *testing.T) {
	backend := newTU20Backend(map[string]tu20Legacy{
		"1": {Name: "Ada", Age: 37},
		"2": {Name: "Grace", Age: 28},
	})
	upgrade, err := newTU20Upgrade(backend, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := upgrade.Read(context.Background(), "1")
	if err != nil || !ok || !reflect.DeepEqual(got, tu20Current{Name: "Ada", Age: 37, Active: true}) {
		t.Fatalf("Read(old fallback) = %#v/%v/%v", got, ok, err)
	}
	if err := upgrade.Write(context.Background(), "3", tu20Legacy{Name: "Lin", Age: 30}); err != nil {
		t.Fatalf("Write(dual) error = %v", err)
	}
	if backend.dualWrites != 1 || len(backend.next) != 1 || backend.next["3"].Name != "Lin" {
		t.Fatalf("dual write state = writes=%d next=%#v", backend.dualWrites, backend.next)
	}
	progress, err := upgrade.RunBatch(context.Background())
	if err != nil || progress.Phase != OnlineSpaceUpgradeReady || progress.Migrated != 2 {
		t.Fatalf("RunBatch() = %#v/%v, want ready with two rows", progress, err)
	}
	if err := upgrade.Cutover(context.Background()); err != nil {
		t.Fatalf("Cutover() error = %v", err)
	}
	if got, ok, err := upgrade.Read(context.Background(), "1"); err != nil || !ok || !got.Active {
		t.Fatalf("Read(new after cutover) = %#v/%v/%v", got, ok, err)
	}
	if err := upgrade.Write(context.Background(), "3", tu20Legacy{Name: "Lin", Age: 30}); err != nil {
		t.Fatalf("Write(new after cutover) error = %v", err)
	}
	if backend.newWrites != 1 || backend.dualWrites != 3 {
		t.Fatalf("write routing = dual=%d new=%d, want dual=3/new=1", backend.dualWrites, backend.newWrites)
	}
}

func TestOnlineSpaceUpgradeResumesFromCheckpointAndDoesNotAdvanceOnFailure(t *testing.T) {
	backend := newTU20Backend(map[string]tu20Legacy{"1": {Name: "Ada"}, "2": {Name: "Grace"}, "3": {Name: "Lin"}})
	upgrade, err := newTU20Upgrade(backend, 1)
	if err != nil {
		t.Fatal(err)
	}
	backend.failDualKey = "1"
	if _, err := upgrade.RunBatch(context.Background()); err == nil {
		t.Fatal("RunBatch() succeeded with a failing dual write")
	}
	if progress := upgrade.Progress(); progress.Cursor != "" || progress.Migrated != 0 {
		t.Fatalf("failed batch advanced progress = %#v", progress)
	}
	backend.failDualKey = ""
	first, err := upgrade.RunBatch(context.Background())
	if err != nil || first.Cursor != "1" || first.Migrated != 1 {
		t.Fatalf("first successful batch = %#v/%v", first, err)
	}
	checkpoint := upgrade.Checkpoint()
	resumed, err := newTU20Upgrade(backend, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Restore(checkpoint); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	for !resumed.Progress().Ready {
		if _, err := resumed.RunBatch(context.Background()); err != nil {
			t.Fatalf("resumed RunBatch() error = %v", err)
		}
	}
	if progress := resumed.Progress(); progress.Migrated != 3 || progress.Cursor != "3" {
		t.Fatalf("resumed progress = %#v", progress)
	}
}

func TestOnlineSpaceUpgradeRejectsStalledProgressAndSupportsRollback(t *testing.T) {
	backend := newTU20Backend(map[string]tu20Legacy{"1": {Name: "Ada"}})
	upgrade, err := newTU20Upgrade(backend, 1)
	if err != nil {
		t.Fatal(err)
	}
	backend.stallScan = true
	if _, err := upgrade.RunBatch(context.Background()); !errors.Is(err, ErrOnlineSpaceUpgradeProgress) {
		t.Fatalf("stalled RunBatch() error = %v, want progress error", err)
	}
	if err := upgrade.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if upgrade.Progress().Phase != OnlineSpaceUpgradeRolledBack || backend.rollbacks != 1 {
		t.Fatalf("rollback state = %#v/%d", upgrade.Progress(), backend.rollbacks)
	}
	if err := upgrade.Rollback(context.Background()); err != nil {
		t.Fatalf("idempotent Rollback() error = %v", err)
	}
	if err := upgrade.Cutover(context.Background()); !errors.Is(err, ErrOnlineSpaceUpgradePhase) {
		t.Fatalf("Cutover(after rollback) error = %v", err)
	}
}

func TestOnlineSpaceUpgradeRollbackReadsOldValue(t *testing.T) {
	backend := newTU20Backend(map[string]tu20Legacy{"1": {Name: "Ada", Age: 37}})
	upgrade, err := newTU20Upgrade(backend, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := upgrade.Write(context.Background(), "1", tu20Legacy{Name: "Ada", Age: 37}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	backend.mu.Lock()
	backend.next["1"] = tu20Current{Name: "stale", Active: false}
	backend.mu.Unlock()
	if err := upgrade.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	got, ok, err := upgrade.Read(context.Background(), "1")
	if err != nil || !ok || got.Name != "Ada" || !got.Active {
		t.Fatalf("Read(after rollback) = %#v/%v/%v, want converted old value", got, ok, err)
	}
}

func TestOnlineSpaceUpgradeValidatesOptionsAndContext(t *testing.T) {
	backend := newTU20Backend(nil)
	if _, err := NewOnlineSpaceUpgrade(OnlineSpaceUpgradeOptions[tu20Legacy, tu20Current]{}); !errors.Is(err, ErrOnlineSpaceUpgradeInvalid) {
		t.Fatalf("empty options error = %v", err)
	}
	upgrade, err := newTU20Upgrade(backend, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := upgrade.RunBatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled RunBatch() error = %v", err)
	}
	if _, _, err := upgrade.Read(context.Background(), ""); !errors.Is(err, ErrOnlineSpaceUpgradeInvalid) {
		t.Fatalf("empty Read() key error = %v", err)
	}
}
