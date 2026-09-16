package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"testing"
	"time"
)

type ch007SchedulerClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *ch007SchedulerClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *ch007SchedulerClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func newCH007SchedulerTable(t *testing.T, clock *ch007SchedulerClock) *TypedTable {
	t.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: time.Second,
			Clock:    clock.Now,
		},
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func TestCH007TTLSchedulerIsExplicitAndPurgesRegisteredTables(t *testing.T) {
	clock := &ch007SchedulerClock{now: time.Unix(100, 0)}
	table := newCH007SchedulerTable(t, clock)
	if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	runs := make(chan TypedTableTTLRun, 4)
	scheduler, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{
		PollInterval: 5 * time.Millisecond,
		Now:          clock.Now,
		OnRun: func(run TypedTableTTLRun) {
			if run.Expired > 0 {
				runs <- run
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("events", table); err != nil {
		t.Fatal(err)
	}
	select {
	case run := <-runs:
		t.Fatalf("unstarted scheduler purged rows: %#v", run)
	case <-time.After(20 * time.Millisecond):
	}
	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.Set(time.Unix(101, 0))
	select {
	case run := <-runs:
		if run.Name != "events" || run.Expired != 1 || run.Error != "" {
			t.Fatalf("background TTL run = %#v, want one successful expiry", run)
		}
	case <-time.After(time.Second):
		t.Fatal("background scheduler did not purge expired row")
	}
	if rows := table.Rows(); len(rows) != 0 {
		t.Fatalf("rows after background purge = %#v, want empty", rows)
	}
	status, ok := scheduler.Status("events")
	if !ok || status.Expired != 1 || status.Error != "" {
		t.Fatalf("scheduler status = %#v/%v, want one successful expiry", status, ok)
	}
	if err := scheduler.Close(); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCH007TTLSchedulerRunOnceRespectsBoundsAndCancellation(t *testing.T) {
	clock := &ch007SchedulerClock{now: time.Unix(100, 0)}
	first := newCH007SchedulerTable(t, clock)
	second := newCH007SchedulerTable(t, clock)
	for _, table := range []*TypedTable{first, second} {
		if _, err := table.Upsert("old", []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	scheduler, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{MaxTablesPerCycle: 1, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	if err := scheduler.Register("first", first); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("second", second); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if runs, err := scheduler.RunOnce(runCtx); !errors.Is(err, context.Canceled) || len(runs) != 0 {
		t.Fatalf("canceled RunOnce() = %#v/%v, want no work and canceled", runs, err)
	}
	clock.Set(time.Unix(101, 0))
	runs, err := scheduler.RunOnce(context.Background())
	if err != nil || len(runs) != 1 || runs[0].Name != "first" || runs[0].Expired != 1 {
		t.Fatalf("bounded RunOnce() = %#v/%v, want first table only", runs, err)
	}
	if err := scheduler.Unregister("first"); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Unregister("missing"); !errors.Is(err, ErrTypedTableTTLSchedulerNotFound) {
		t.Fatalf("Unregister(missing) = %v, want not found", err)
	}
}

func TestCH007ProcessingTTLDeadlineSnapshotRoundTripIsAtomic(t *testing.T) {
	clock := &ch007SchedulerClock{now: time.Unix(100, 0)}
	table := newCH007SchedulerTable(t, clock)
	if _, err := table.Upsert("early", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	clock.Set(time.Unix(102, 0))
	if _, err := table.Upsert("late", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	encoded, err := table.MarshalTTLState()
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 {
		t.Fatal("MarshalTTLState() returned empty data")
	}
	encodedAgain, err := table.MarshalTTLState()
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(encodedAgain) {
		t.Fatal("MarshalTTLState() is not deterministic")
	}

	restoredClock := &ch007SchedulerClock{now: time.Unix(200, 0)}
	restored := newCH007SchedulerTable(t, restoredClock)
	for _, row := range []struct {
		key   string
		value int64
	}{
		{key: "early", value: 1},
		{key: "late", value: 2},
	} {
		if _, err := restored.Upsert(row.key, []TypedTableValue{TypedInt64(row.value)}); err != nil {
			t.Fatal(err)
		}
	}
	corrupted := append([]byte(nil), encoded...)
	corrupted[len(corrupted)-1] ^= 1
	if err := restored.RestoreTTLState(corrupted); err == nil {
		t.Fatal("RestoreTTLState() accepted a corrupted snapshot")
	}
	if changes, err := restored.PurgeExpired(time.Unix(110, 0)); err != nil || len(changes) != 0 {
		t.Fatalf("corrupt restore changed deadlines: %#v/%v", changes, err)
	}
	if err := restored.RestoreTTLState(encoded); err != nil {
		t.Fatal(err)
	}
	if changes, err := restored.PurgeExpired(time.Unix(101, 0)); err != nil || len(changes) != 1 || changes[0].Key != "early" {
		t.Fatalf("first restored purge = %#v/%v, want early", changes, err)
	}
	if changes, err := restored.PurgeExpired(time.Unix(103, 0)); err != nil || len(changes) != 1 || changes[0].Key != "late" {
		t.Fatalf("second restored purge = %#v/%v, want late", changes, err)
	}

	eventTable, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL:  TypedTableTTLOptions{Mode: TypedTableTTLEventTime, Field: "event_at", Lifetime: time.Second},
		Columns: []TypedTableColumn{
			{Name: "event_at", Kind: TypedTableInt64},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eventTable.MarshalTTLState(); !errors.Is(err, ErrTypedTableTTLStateUnsupported) {
		t.Fatalf("event-time MarshalTTLState() = %v, want unsupported", err)
	}
}

func TestCH007TTLStateRejectsImpossibleRowCountBeforeAllocation(t *testing.T) {
	clock := &ch007SchedulerClock{now: time.Unix(100, 0)}
	table := newCH007SchedulerTable(t, clock)
	if _, err := table.Upsert("row", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	encoded, err := table.MarshalTTLState()
	if err != nil {
		t.Fatal(err)
	}
	rowCountOffset := len(typedTableTTLStateMagic) + 1 + 1 + 2 + 8 + 4 + len(table.schema.Name)
	binary.LittleEndian.PutUint32(encoded[rowCountOffset:], 1_000_000)
	binary.LittleEndian.PutUint32(encoded[len(encoded)-4:], crc32.ChecksumIEEE(encoded[:len(encoded)-4]))
	if err := table.RestoreTTLState(encoded); !errors.Is(err, ErrTypedTableTTLStateInvalid) {
		t.Fatalf("impossible row count restore = %v, want invalid", err)
	}
}

func TestCH007TTLSchedulerRejectsInvalidLifecycleRequests(t *testing.T) {
	if _, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{PollInterval: -time.Second}); !errors.Is(err, ErrTypedTableTTLSchedulerInvalid) {
		t.Fatalf("negative poll interval = %v, want invalid", err)
	}
	scheduler, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	clock := &ch007SchedulerClock{now: time.Unix(100, 0)}
	table := newCH007SchedulerTable(t, clock)
	if err := scheduler.Register("events", table); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("events", table); !errors.Is(err, ErrTypedTableTTLSchedulerExists) {
		t.Fatalf("duplicate Register() = %v, want exists", err)
	}
	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Start(context.Background()); !errors.Is(err, ErrTypedTableTTLSchedulerStarted) {
		t.Fatalf("duplicate Start() = %v, want started", err)
	}
	if err := scheduler.Close(); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("after-close", table); !errors.Is(err, ErrTypedTableTTLSchedulerClosed) {
		t.Fatalf("Register after Close() = %v, want closed", err)
	}
}
