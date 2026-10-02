package hatDataStructure

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
)

type onlineTupleUpgradeTestStore struct {
	mu   sync.Mutex
	rows map[string]VersionedTuple
}

func newOnlineTupleUpgradeTestStore(rows map[string]VersionedTuple) *onlineTupleUpgradeTestStore {
	owned := make(map[string]VersionedTuple, len(rows))
	for key, tuple := range rows {
		owned[key] = tuple.Clone()
	}
	return &onlineTupleUpgradeTestStore{rows: owned}
}

func (store *onlineTupleUpgradeTestStore) Get(ctx context.Context, key []byte) (VersionedTuple, bool, error) {
	if err := ctx.Err(); err != nil {
		return VersionedTuple{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	tuple, ok := store.rows[string(key)]
	if !ok {
		return VersionedTuple{}, false, nil
	}
	return tuple.Clone(), true, nil
}

func (store *onlineTupleUpgradeTestStore) Put(ctx context.Context, key []byte, tuple VersionedTuple) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.rows[string(key)] = tuple.Clone()
	return nil
}

func (store *onlineTupleUpgradeTestStore) Scan(ctx context.Context, visit func([]byte, VersionedTuple) error) error {
	store.mu.Lock()
	keys := make([]string, 0, len(store.rows))
	for key := range store.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]VersionedTuple, len(keys))
	for index, key := range keys {
		rows[index] = store.rows[key].Clone()
	}
	store.mu.Unlock()
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit([]byte(key), rows[index]); err != nil {
			return err
		}
	}
	return nil
}

func onlineTupleUpgradeTestFormats(t *testing.T) (TupleFormat, TupleFormat, TupleFormat) {
	t.Helper()
	v1, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
	})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldString},
	})
	if err != nil {
		t.Fatal(err)
	}
	v3, err := NewTupleFormat(3, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldString},
	})
	if err != nil {
		t.Fatal(err)
	}
	return v1, v2, v3
}

func onlineTupleUpgradeTestPlan(t *testing.T, v1, v2 TupleFormat) *TupleMigrationPlan {
	t.Helper()
	plan, err := NewTupleMigrationPlan("accounts", []TupleMigrationStep{{
		From: v1,
		To:   v2,
		Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
			values, err := v1.Unpack(tuple.Tuple())
			if err != nil {
				return VersionedTuple{}, err
			}
			values = append(values, TupleString("active"))
			return NewVersionedTuple(destination, values)
		},
		Rollback: func(tuple VersionedTuple, source TupleFormat) (VersionedTuple, error) {
			values, err := v2.Unpack(tuple.Tuple())
			if err != nil {
				return VersionedTuple{}, err
			}
			return NewVersionedTuple(source, values[:2])
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func onlineTupleUpgradeTestTuple(t *testing.T, format TupleFormat, id int64, name string) VersionedTuple {
	t.Helper()
	tuple, err := NewVersionedTuple(format, []TupleFieldValue{TupleInt64(id), TupleString(name)})
	if err != nil {
		t.Fatal(err)
	}
	return tuple
}

func TestOnlineTupleUpgradeReadsWritesBatchesAndCutsOver(t *testing.T) {
	v1, v2, _ := onlineTupleUpgradeTestFormats(t)
	plan := onlineTupleUpgradeTestPlan(t, v1, v2)
	store := newOnlineTupleUpgradeTestStore(map[string]VersionedTuple{
		"a": onlineTupleUpgradeTestTuple(t, v1, 1, "alice"),
		"b": onlineTupleUpgradeTestTuple(t, v1, 2, "bob"),
		"e": onlineTupleUpgradeTestTuple(t, v1, 5, "erin"),
		"c": func() VersionedTuple {
			tuple, err := NewVersionedTuple(v2, []TupleFieldValue{TupleInt64(3), TupleString("carol"), TupleString("active")})
			if err != nil {
				t.Fatal(err)
			}
			return tuple
		}(),
	})
	upgrade, err := NewOnlineTupleUpgrade(plan, v1.Version(), v2.Version())
	if err != nil {
		t.Fatal(err)
	}
	if err := upgrade.Begin(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	read, found, err := upgrade.Read(ctx, store, []byte("a"))
	if err != nil || !found {
		t.Fatalf("read repair = %#v/%v/%v", read, found, err)
	}
	if err := read.Validate(v2); err != nil {
		t.Fatalf("read-repaired tuple validation: %v", err)
	}
	stored, _, err := store.Get(ctx, []byte("a"))
	if err != nil || stored.Version() != v2.Version() {
		t.Fatalf("stored read repair = version %d/%v, want v2", stored.Version(), err)
	}

	if err := upgrade.Write(ctx, store, []byte("d"), onlineTupleUpgradeTestTuple(t, v1, 4, "dora")); err != nil {
		t.Fatal(err)
	}
	stored, _, err = store.Get(ctx, []byte("d"))
	if err != nil || stored.Version() != v2.Version() {
		t.Fatalf("stored normalized write = version %d/%v, want v2", stored.Version(), err)
	}

	first, err := upgrade.MigrateBatch(ctx, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete || first.Migrated != 1 || first.State != OnlineTupleUpgradeRunning {
		t.Fatalf("first batch = %#v, want one incomplete migration", first)
	}
	second, err := upgrade.MigrateBatch(ctx, store, 16)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Complete || second.State != OnlineTupleUpgradeReady {
		t.Fatalf("second batch = %#v, want ready completion", second)
	}
	if err := upgrade.Cutover(); err != nil {
		t.Fatal(err)
	}
	if got := upgrade.State(); got != OnlineTupleUpgradeCutover {
		t.Fatalf("state after cutover = %v, want cutover", got)
	}
	if err := upgrade.Write(ctx, store, []byte("e"), onlineTupleUpgradeTestTuple(t, v1, 5, "erin")); !errors.Is(err, ErrOnlineTupleUpgradeVersion) {
		t.Fatalf("old-format write after cutover = %v, want version error", err)
	}
	stats := upgrade.Stats()
	if stats.ReadRepairs != 1 || stats.Migrated != 2 || stats.Current == 0 {
		t.Fatalf("stats = %#v, want read repair, migrations, and current count", stats)
	}
}

func TestOnlineTupleUpgradeCancellationCanResume(t *testing.T) {
	v1, v2, _ := onlineTupleUpgradeTestFormats(t)
	upgrade, err := NewOnlineTupleUpgrade(onlineTupleUpgradeTestPlan(t, v1, v2), v1.Version(), v2.Version())
	if err != nil {
		t.Fatal(err)
	}
	if err := upgrade.Begin(); err != nil {
		t.Fatal(err)
	}
	store := newOnlineTupleUpgradeTestStore(map[string]VersionedTuple{
		"a": onlineTupleUpgradeTestTuple(t, v1, 1, "alice"),
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := upgrade.MigrateBatch(ctx, store, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled batch error = %v, want context.Canceled", err)
	}
	if got := upgrade.State(); got != OnlineTupleUpgradeRunning {
		t.Fatalf("state after cancellation = %v, want running", got)
	}
	result, err := upgrade.MigrateBatch(context.Background(), store, 10)
	if err != nil || !result.Complete {
		t.Fatalf("resumed batch = %#v/%v, want complete", result, err)
	}
}

func TestOnlineTupleUpgradeRejectsMixedVersionsAndInvalidTransitions(t *testing.T) {
	v1, v2, v3 := onlineTupleUpgradeTestFormats(t)
	plan := onlineTupleUpgradeTestPlan(t, v1, v2)
	if _, err := NewOnlineTupleUpgrade(plan, v1.Version(), v1.Version()); !errors.Is(err, ErrOnlineTupleUpgradeInvalid) {
		t.Fatalf("equal versions error = %v, want invalid", err)
	}
	upgrade, err := NewOnlineTupleUpgrade(plan, v1.Version(), v2.Version())
	if err != nil {
		t.Fatal(err)
	}
	if err := upgrade.Cutover(); !errors.Is(err, ErrOnlineTupleUpgradeState) {
		t.Fatalf("cutover before begin = %v, want state error", err)
	}
	if err := upgrade.Begin(); err != nil {
		t.Fatal(err)
	}
	mixed, err := NewVersionedTuple(v3, []TupleFieldValue{TupleInt64(3), TupleString("carol"), TupleString("active")})
	if err != nil {
		t.Fatal(err)
	}
	store := newOnlineTupleUpgradeTestStore(map[string]VersionedTuple{"bad": mixed})
	if _, err := upgrade.MigrateBatch(context.Background(), store, 10); !errors.Is(err, ErrOnlineTupleUpgradeVersion) {
		t.Fatalf("mixed version error = %v, want version error", err)
	}
	if got := upgrade.State(); got != OnlineTupleUpgradeFailed {
		t.Fatalf("state after mixed version = %v, want failed", got)
	}
	if _, _, err := upgrade.Read(context.Background(), store, []byte("bad")); !errors.Is(err, ErrOnlineTupleUpgradeState) {
		t.Fatalf("read after failure = %v, want state error", err)
	}
}

func TestOnlineTupleUpgradeStatsSnapshotIsIndependent(t *testing.T) {
	v1, v2, _ := onlineTupleUpgradeTestFormats(t)
	upgrade, err := NewOnlineTupleUpgrade(onlineTupleUpgradeTestPlan(t, v1, v2), v1.Version(), v2.Version())
	if err != nil {
		t.Fatal(err)
	}
	first := upgrade.Stats()
	second := upgrade.Stats()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("stats snapshots differ: %#v and %#v", first, second)
	}
}
