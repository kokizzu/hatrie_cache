package hatSql

import "testing"

func TestTypedTableStorageEvents(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableInt64},
		},
		PatchParts: TypedTablePatchOptions{
			Enabled:        true,
			MergeThreshold: 100,
		},
		StorageEvents: TypedTableStorageEventLogOptions{
			Enabled:  true,
			Capacity: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, enabled := table.StorageEvents(0); !enabled {
		t.Fatal("storage event log should be enabled")
	}

	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	events, enabled := table.StorageEvents(0)
	if !enabled || len(events) != 2 {
		t.Fatalf("events after delete = (%v, %v), want two retained events", events, enabled)
	}
	if events[0].Kind != TypedTableStorageEventBasePartCreated {
		t.Fatalf("first retained event kind = %q, want %q", events[0].Kind, TypedTableStorageEventBasePartCreated)
	}
	if events[1].Kind != TypedTableStorageEventPatchPartCreated {
		t.Fatalf("second retained event kind = %q, want %q", events[1].Kind, TypedTableStorageEventPatchPartCreated)
	}
	if events[1].PhysicalRowsBefore != 2 || events[1].PhysicalRowsAfter != 2 || events[1].PendingDeletes != 1 || events[1].DeletedRows != 1 {
		t.Fatalf("patch event = %+v, want physical rows 2/2 and one pending delete", events[1])
	}
	if events[1].TableSequence != 3 || events[1].At.IsZero() {
		t.Fatalf("patch event metadata = %+v, want table sequence 3 and timestamp", events[1])
	}

	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	events, enabled = table.StorageEvents(0)
	if !enabled || len(events) != 2 {
		t.Fatalf("events after merge = (%v, %v), want two retained events", events, enabled)
	}
	if events[1].Kind != TypedTableStorageEventPatchPartMerged {
		t.Fatalf("second retained event kind = %q, want %q", events[1].Kind, TypedTableStorageEventPatchPartMerged)
	}
	if events[1].Sequence != events[0].Sequence+1 || events[1].TableSequence != 3 {
		t.Fatalf("event sequence metadata = %+v, want consecutive event sequence at table sequence 3", events[1])
	}
	if events[1].PhysicalRowsBefore != 2 || events[1].PhysicalRowsAfter != 1 || events[1].PendingDeletes != 0 || events[1].DeletedRows != 1 {
		t.Fatalf("merge event = %+v, want physical rows 2/1 and one removed row", events[1])
	}
	if events[1].Duration < 0 {
		t.Fatalf("merge duration = %s, want non-negative duration", events[1].Duration)
	}
	if rows := table.Rows(); len(rows) != 1 {
		t.Fatalf("rows after merge = %d, want 1", len(rows))
	}

	latest, enabled := table.StorageEvents(1)
	if !enabled || len(latest) != 1 || latest[0].Sequence != events[1].Sequence {
		t.Fatalf("latest event = (%v, %v), want only the newest event", latest, enabled)
	}
}

func TestTypedTableStorageEventsDisabledByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events-disabled",
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableInt64},
		},
		PatchParts: TypedTablePatchOptions{Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if events, enabled := table.StorageEvents(0); enabled || events != nil {
		t.Fatalf("default storage events = (%v, %v), want disabled and nil", events, enabled)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	if events, enabled := table.StorageEvents(0); enabled || events != nil {
		t.Fatalf("events after disabled mutations = (%v, %v), want disabled and nil", events, enabled)
	}
}

func TestTypedTableStorageEventsDefaultCapacity(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events-default-capacity",
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableInt64},
		},
		StorageEvents: TypedTableStorageEventLogOptions{Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if capacity := table.Schema().StorageEvents.Capacity; capacity != typedTableStorageEventLogDefaultCapacity {
		t.Fatalf("default event capacity = %d, want %d", capacity, typedTableStorageEventLogDefaultCapacity)
	}
}
