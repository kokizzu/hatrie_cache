package hatSql

import (
	"context"
	"testing"
	"time"
)

func newCH009TTLRollupTable(t *testing.T, now *time.Time) *TypedTable {
	t.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: time.Second,
			Clock: func() time.Time {
				return *now
			},
		},
		Columns: []TypedTableColumn{
			{Name: "group", Kind: TypedTableString},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	return table
}

func TestCH009TTLRollupAggregatesExpiredRowsAndDeduplicates(t *testing.T) {
	now := time.Unix(1000, 0)
	table := newCH009TTLRollupTable(t, &now)
	for key, values := range map[string][]TypedTableValue{
		"a-1": {TypedString("a"), TypedInt64(2)},
		"a-2": {TypedString("a"), TypedInt64(3)},
	} {
		if _, err := table.Upsert(key, values); err != nil {
			t.Fatalf("upsert %s: %v", key, err)
		}
	}

	now = now.Add(1200 * time.Millisecond)
	changes, err := table.PurgeExpired(now)
	if err != nil {
		t.Fatalf("purge expired: %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("purged changes = %d, want 2", len(changes))
	}

	rollup, err := NewTypedTableTTLRollup(table, TypedTableAggregateDefinition{
		GroupBy:                []string{"group"},
		SumField:               "value",
		DictionaryEncodeGroups: true,
	})
	if err != nil {
		t.Fatalf("new rollup: %v", err)
	}
	if err := rollup.ApplyExpired(changes); err != nil {
		t.Fatalf("apply expired: %v", err)
	}
	if err := rollup.ApplyExpired(changes); err != nil {
		t.Fatalf("replay expired: %v", err)
	}
	if got := rollup.Checkpoint(); got != changes[len(changes)-1].Sequence {
		t.Fatalf("checkpoint = %d, want %d", got, changes[len(changes)-1].Sequence)
	}

	rows := rollup.Rows()
	if len(rows) != 1 {
		t.Fatalf("rollup rows = %d, want 1", len(rows))
	}
	if rows[0]["group"] != "a" || rows[0]["count"] != int64(2) || rows[0]["sum"] != float64(5) {
		t.Fatalf("rollup row = %#v, want group a count 2 sum 5", rows[0])
	}
}

func TestCH009TTLRollupSchedulerRegistrationIsOptIn(t *testing.T) {
	now := time.Unix(2000, 0)
	table := newCH009TTLRollupTable(t, &now)
	if _, err := table.Upsert("a", []TypedTableValue{TypedString("a"), TypedInt64(4)}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	now = now.Add(2 * time.Second)

	withoutRollup, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{
		Now: func() time.Time {
			return now
		},
	})
	if err != nil {
		t.Fatalf("new scheduler without rollup: %v", err)
	}
	if err := withoutRollup.Register("events", table); err != nil {
		t.Fatalf("register without rollup: %v", err)
	}
	if _, err := withoutRollup.RunOnce(context.Background()); err != nil {
		t.Fatalf("run without rollup: %v", err)
	}
	rollup, err := NewTypedTableTTLRollup(table, TypedTableAggregateDefinition{GroupBy: []string{"group"}})
	if err != nil {
		t.Fatalf("new opt-in rollup: %v", err)
	}
	if len(rollup.Rows()) != 0 {
		t.Fatal("ordinary scheduler registration unexpectedly applied a rollup")
	}

	now = time.Unix(3000, 0)
	table = newCH009TTLRollupTable(t, &now)
	if _, err := table.Upsert("b", []TypedTableValue{TypedString("b"), TypedInt64(6)}); err != nil {
		t.Fatalf("upsert opt-in row: %v", err)
	}
	now = now.Add(2 * time.Second)
	rollup, err = NewTypedTableTTLRollup(table, TypedTableAggregateDefinition{GroupBy: []string{"group"}})
	if err != nil {
		t.Fatalf("new scheduler rollup: %v", err)
	}
	withRollup, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{
		Now: func() time.Time {
			return now
		},
	})
	if err != nil {
		t.Fatalf("new scheduler with rollup: %v", err)
	}
	if err := withRollup.RegisterWithRollup("events", table, rollup); err != nil {
		t.Fatalf("register with rollup: %v", err)
	}
	runs, err := withRollup.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run with rollup: %v", err)
	}
	if len(runs) != 1 || runs[0].Expired != 1 || runs[0].RolledUp != 1 {
		t.Fatalf("runs = %#v, want one expired and rolled-up row", runs)
	}
	if rows := rollup.Rows(); len(rows) != 1 || rows[0]["group"] != "b" || rows[0]["count"] != int64(1) {
		t.Fatalf("scheduled rollup rows = %#v, want group b count 1", rows)
	}
}

func TestCH009TTLRollupRejectsInvalidChangesWithoutMutation(t *testing.T) {
	now := time.Unix(4000, 0)
	table := newCH009TTLRollupTable(t, &now)
	rollup, err := NewTypedTableTTLRollup(table, TypedTableAggregateDefinition{GroupBy: []string{"group"}})
	if err != nil {
		t.Fatalf("new rollup: %v", err)
	}
	if err := rollup.ApplyExpired([]TypedTableChange{{
		Sequence:  1,
		Operation: "INSERT",
		After:     []TypedTableValue{TypedString("a"), TypedInt64(1)},
	}}); err == nil {
		t.Fatal("insert change was accepted as an expired row")
	}
	if rollup.Checkpoint() != 0 || len(rollup.Rows()) != 0 {
		t.Fatalf("invalid change mutated rollup: checkpoint=%d rows=%#v", rollup.Checkpoint(), rollup.Rows())
	}
}

func TestCH009TTLRollupRejectsColumnTTLCombination(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events-with-column-ttl",
		Columns: []TypedTableColumn{{
			Name: "payload",
			Kind: TypedTableString,
			TTL: TypedTableTTLOptions{
				Mode:     TypedTableTTLProcessingTime,
				Lifetime: time.Hour,
			},
		}},
	})
	if err != nil {
		t.Fatalf("new table: %v", err)
	}
	if _, err := NewTypedTableTTLRollup(table, TypedTableAggregateDefinition{}); err == nil {
		t.Fatal("rollup accepted a table with column TTL")
	}
}
