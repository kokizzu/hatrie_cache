package hatSql

import (
	"testing"
	"time"
)

func TestCH225ProcessingTTLExpiryIndexTracksLiveRows(t *testing.T) {
	clock := func() time.Time { return time.Unix(100, 0) }
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: time.Second,
			Clock:    clock,
		},
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if got := len(table.ttl.expiryHeap); got != 2 {
		t.Fatalf("expiry heap length after inserts = %d, want 2", got)
	}

	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if got := len(table.ttl.expiryHeap); got != 1 {
		t.Fatalf("expiry heap length after delete = %d, want 1", got)
	}

	if _, err := table.Upsert("b", []TypedTableValue{TypedInt64(3)}); err != nil {
		t.Fatal(err)
	}
	if got := len(table.ttl.expiryHeap); got != 1 {
		t.Fatalf("expiry heap length after update = %d, want 1", got)
	}
}

func TestCH225EventTTLExpiryIndexTracksValueChanges(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLEventTime,
			Field:    "event_at",
			Lifetime: time.Second,
		},
		Columns: []TypedTableColumn{
			{Name: "event_at", Kind: TypedTableInt64},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(100), TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if got := len(table.ttl.expiryHeap); got != 1 {
		t.Fatalf("expiry heap length after event insert = %d, want 1", got)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedNull(), TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if got := len(table.ttl.expiryHeap); got != 0 {
		t.Fatalf("expiry heap length after NULL event time = %d, want 0", got)
	}
}

func TestCH225PurgeExpiredPreservesPhysicalOrder(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLEventTime,
			Field:    "event_at",
			Lifetime: time.Second,
		},
		Columns: []TypedTableColumn{
			{Name: "event_at", Kind: TypedTableInt64},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		event time.Duration
	}{
		{key: "a", event: 3 * time.Second},
		{key: "b", event: time.Second},
		{key: "c", event: 2 * time.Second},
	} {
		if _, err := table.Upsert(row.key, []TypedTableValue{
			TypedInt64(row.event.Nanoseconds()),
			TypedInt64(1),
		}); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := table.PurgeExpired(time.Unix(0, 10*int64(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 {
		t.Fatalf("PurgeExpired() returned %d changes, want 3", len(changes))
	}
	for index, want := range []string{"a", "b", "c"} {
		if changes[index].Key != want {
			t.Fatalf("change %d key = %q, want %q", index, changes[index].Key, want)
		}
	}
}

func TestCH225PatchCompactionRebuildsTTLExpiryIndex(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		TTL: TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: time.Second,
			Clock:    func() time.Time { return time.Unix(100, 0) },
		},
		PatchParts: TypedTablePatchOptions{Enabled: true, MergeThreshold: 100},
		Columns:    []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if got := len(table.ttl.expiryHeap); got != 2 {
		t.Fatalf("expiry heap after logical delete = %d, want 2", got)
	}
	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	if got := len(table.ttl.expiryHeap); got != 2 {
		t.Fatalf("expiry heap after patch compaction = %d, want 2", got)
	}
	changes, err := table.PurgeExpired(time.Unix(102, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].Key != "a" || changes[1].Key != "c" {
		t.Fatalf("PurgeExpired() after compaction = %#v, want a,c", changes)
	}
}
