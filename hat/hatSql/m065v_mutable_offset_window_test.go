package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMutableIncrementalOffsetWindowAppliesChanges(t *testing.T) {
	window, err := NewMutableIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		Offset:       1,
		DefaultValue: int64(-1),
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	inserted, err := window.Apply([]IncrementalOffsetWindowMutation{
		{Operation: IncrementalOffsetWindowInsert, Row: mutableOffsetRow("a", 1, 10)},
		{Operation: IncrementalOffsetWindowInsert, Row: mutableOffsetRow("b", 2, 20)},
		{Operation: IncrementalOffsetWindowInsert, Row: mutableOffsetRow("c", 3, 30)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableOffsetValue(t, inserted, "a", int64(-1))
	assertMutableOffsetValue(t, inserted, "b", int64(10))
	assertMutableOffsetValue(t, inserted, "c", int64(20))

	updated, err := window.Apply([]IncrementalOffsetWindowMutation{{
		Operation: IncrementalOffsetWindowUpdate,
		Key:       "b",
		Row:       mutableOffsetRow("b", 2, 200),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updated) != 4 {
		t.Fatalf("value update changes = %#v, want updated base row and dependent retraction/insertion", updated)
	}
	if !hasMutableOffsetBaseChange(updated, "b", -1, int64(20)) || !hasMutableOffsetBaseChange(updated, "b", 1, int64(200)) {
		t.Fatalf("updated base row was not retracted and inserted: %#v", updated)
	}
	assertMutableOffsetDiffPair(t, updated, "c", int64(20), int64(200))

	deleted, err := window.Apply([]IncrementalOffsetWindowMutation{{
		Operation: IncrementalOffsetWindowDelete,
		Key:       "b",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 3 {
		t.Fatalf("delete changes = %#v, want b removal and c replacement", deleted)
	}
	assertMutableOffsetDiffPair(t, deleted, "c", int64(200), int64(10))
	if !hasMutableOffsetChange(deleted, "b", -1, int64(10)) {
		t.Fatalf("delete did not retract b: %#v", deleted)
	}
}

func TestMutableIncrementalOffsetWindowIsAtomicAndSupportsLead(t *testing.T) {
	window, err := NewMutableIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLead,
		OutputColumn: "lead_value",
		Offset:       1,
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]IncrementalOffsetWindowMutation{
		{Operation: IncrementalOffsetWindowInsert, Row: mutableOffsetRowPartition("a", "p", 1, 10)},
		{Operation: IncrementalOffsetWindowInsert, Row: mutableOffsetRowPartition("b", "p", 2, 20)},
	}); err != nil {
		t.Fatal(err)
	}
	before, err := window.Apply([]IncrementalOffsetWindowMutation{
		{Operation: IncrementalOffsetWindowUpdate, Key: "a", Row: mutableOffsetRowPartition("a", "p", 1, 11)},
		{Operation: IncrementalOffsetWindowDelete, Key: "missing"},
	})
	if !errors.Is(err, ErrIncrementalOffsetWindowMissingKey) || before != nil {
		t.Fatalf("invalid batch result = %#v, error = %v", before, err)
	}
	changes, err := window.Apply([]IncrementalOffsetWindowMutation{{
		Operation: IncrementalOffsetWindowInsert,
		Row:       mutableOffsetRowPartition("c", "p", 3, 30),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !hasMutableOffsetChange(changes, "b", -1, nil) || !hasMutableOffsetChange(changes, "b", 1, int64(30)) {
		t.Fatalf("atomic failure changed lead state: %#v", changes)
	}
}

func TestMutableIncrementalOffsetWindowRebuildsOrderAndPartitionChanges(t *testing.T) {
	window, err := NewMutableIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		Offset:       1,
		DefaultValue: int64(-1),
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]IncrementalOffsetWindowMutation{
		{Operation: IncrementalOffsetWindowInsert, Row: mutableOffsetRow("a", 2, 10)},
		{Operation: IncrementalOffsetWindowInsert, Row: mutableOffsetRow("b", 4, 20)},
	}); err != nil {
		t.Fatal(err)
	}
	orderChange, err := window.Apply([]IncrementalOffsetWindowMutation{{
		Operation: IncrementalOffsetWindowUpdate,
		Key:       "b",
		Row:       mutableOffsetRow("b", 1, 20),
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableOffsetDiffPair(t, orderChange, "a", int64(-1), int64(20))
	assertMutableOffsetDiffPair(t, orderChange, "b", int64(10), int64(-1))

	partitionChange, err := window.Apply([]IncrementalOffsetWindowMutation{{
		Operation: IncrementalOffsetWindowUpdate,
		Key:       "b",
		Row:       mutableOffsetRowPartition("b", "q", 1, 20),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !hasMutableOffsetBaseChange(partitionChange, "b", -1, int64(20)) || !hasMutableOffsetBaseChange(partitionChange, "b", 1, int64(20)) {
		t.Fatalf("partition move did not replace b: %#v", partitionChange)
	}

	invalid, err := window.Apply([]IncrementalOffsetWindowMutation{
		{Operation: IncrementalOffsetWindowUpdate, Key: "a", Row: mutableOffsetRow("a", 2, 100)},
		{Operation: IncrementalOffsetWindowDelete, Key: "missing"},
	})
	if !errors.Is(err, ErrIncrementalOffsetWindowMissingKey) || invalid != nil {
		t.Fatalf("invalid rebuild batch result = %#v, error = %v", invalid, err)
	}
}

func mutableOffsetRow(key string, order, value int64) Row {
	return mutableOffsetRowPartition(key, "p", order, value)
}

func mutableOffsetRowPartition(key, partition string, order, value int64) Row {
	return Row{"id": key, "partition": partition, "order": order, "value": value}
}

func assertMutableOffsetValue(t *testing.T, changes []DifferentialRow, key string, want interface{}) {
	t.Helper()
	for _, change := range changes {
		if change.Key == key && change.Diff == 1 && reflect.DeepEqual(change.Row["lag_value"], want) {
			return
		}
		if change.Key == key && change.Diff == 1 && reflect.DeepEqual(change.Row["lead_value"], want) {
			return
		}
	}
	t.Fatalf("missing positive value change for %q = %#v, changes %#v", key, want, changes)
}

func assertMutableOffsetDiffPair(t *testing.T, changes []DifferentialRow, key string, oldValue, newValue interface{}) {
	t.Helper()
	if !hasMutableOffsetChange(changes, key, -1, oldValue) || !hasMutableOffsetChange(changes, key, 1, newValue) {
		t.Fatalf("changes for %q = %#v, want %v -> %v", key, changes, oldValue, newValue)
	}
}

func hasMutableOffsetChange(changes []DifferentialRow, key string, diff int64, value interface{}) bool {
	for _, change := range changes {
		if change.Key != key || change.Diff != diff {
			continue
		}
		if reflect.DeepEqual(change.Row["lag_value"], value) || reflect.DeepEqual(change.Row["lead_value"], value) {
			return true
		}
	}
	return false
}

func hasMutableOffsetBaseChange(changes []DifferentialRow, key string, diff int64, value interface{}) bool {
	for _, change := range changes {
		if change.Key == key && change.Diff == diff && reflect.DeepEqual(change.Row["value"], value) {
			return true
		}
	}
	return false
}
