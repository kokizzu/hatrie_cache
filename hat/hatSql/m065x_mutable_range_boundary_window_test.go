package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestMutableIncrementalRangeBoundaryWindowTracksUpdatesAndDeletes(t *testing.T) {
	for _, testCase := range []struct {
		name string
		kind IncrementalRangeBoundaryWindowKind
	}{
		{name: "first", kind: IncrementalRangeFirstValue},
		{name: "last", kind: IncrementalRangeLastValue},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			window, err := NewMutableIncrementalRangeBoundaryWindow(IncrementalRangeBoundaryWindowDefinition{
				Kind:           testCase.kind,
				OutputColumn:   "result",
				FramePreceding: 2,
				PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
				ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
			})
			if err != nil {
				t.Fatal(err)
			}

			initial, err := window.Apply([]IncrementalRangeBoundaryWindowMutation{
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "a", "partition": "p", "order": int64(1), "value": "A"}},
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "b", "partition": "p", "order": int64(2), "value": "B"}},
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "c", "partition": "p", "order": int64(3), "value": "C"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"a", "b", "c"} {
				want := interface{}("A")
				if testCase.kind == IncrementalRangeLastValue {
					want = map[string]interface{}{"a": "A", "b": "B", "c": "C"}[key]
				}
				assertMutableRangeBoundaryValue(t, initial, key, 1, want)
			}

			var updated []DifferentialRow
			if testCase.kind == IncrementalRangeFirstValue {
				updated, err = window.Apply([]IncrementalRangeBoundaryWindowMutation{{
					Operation: IncrementalRangeBoundaryWindowUpdate,
					Key:       "a",
					Row:       Row{"id": "a", "partition": "p", "order": int64(1), "value": "A2"},
				}})
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{"a", "b", "c"} {
					assertMutableRangeBoundaryValue(t, updated, key, -1, "A")
					assertMutableRangeBoundaryValue(t, updated, key, 1, "A2")
				}
			} else {
				updated, err = window.Apply([]IncrementalRangeBoundaryWindowMutation{{
					Operation: IncrementalRangeBoundaryWindowUpdate,
					Key:       "c",
					Row:       Row{"id": "c", "partition": "p", "order": int64(3), "value": "C2"},
				}})
				if err != nil {
					t.Fatal(err)
				}
				assertMutableRangeBoundaryValue(t, updated, "c", -1, "C")
				assertMutableRangeBoundaryValue(t, updated, "c", 1, "C2")
			}

			deleted, err := window.Apply([]IncrementalRangeBoundaryWindowMutation{{
				Operation: IncrementalRangeBoundaryWindowDelete,
				Key:       "a",
			}})
			if err != nil {
				t.Fatal(err)
			}
			if testCase.kind == IncrementalRangeFirstValue {
				for _, key := range []string{"b", "c"} {
					assertMutableRangeBoundaryValue(t, deleted, key, -1, "A2")
					assertMutableRangeBoundaryValue(t, deleted, key, 1, "B")
				}
			} else {
				assertMutableRangeBoundaryValue(t, deleted, "a", -1, "A")
			}
		})
	}
}

func TestMutableIncrementalRangeBoundaryWindowBatchedStableUpdates(t *testing.T) {
	for _, testCase := range []struct {
		name string
		kind IncrementalRangeBoundaryWindowKind
	}{
		{name: "first", kind: IncrementalRangeFirstValue},
		{name: "last", kind: IncrementalRangeLastValue},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			partitionCalls := 0
			orderCalls := 0
			rowKeyCalls := 0
			valueCalls := 0
			definition := IncrementalRangeBoundaryWindowDefinition{
				Kind:           testCase.kind,
				OutputColumn:   "result",
				FramePreceding: 2,
				PartitionKey: func(row Row) (string, error) {
					partitionCalls++
					return row["partition"].(string), nil
				},
				OrderKey: func(row Row) (interface{}, error) {
					orderCalls++
					return row["order"], nil
				},
				RowKey: func(row Row) (string, error) {
					rowKeyCalls++
					return row["id"].(string), nil
				},
				ValueKey: func(row Row) (interface{}, error) {
					valueCalls++
					return row["value"], nil
				},
			}
			window, err := NewMutableIncrementalRangeBoundaryWindow(definition)
			if err != nil {
				t.Fatal(err)
			}
			sequentialDefinition := definition
			sequentialDefinition.PartitionKey = func(row Row) (string, error) { return row["partition"].(string), nil }
			sequentialDefinition.OrderKey = func(row Row) (interface{}, error) { return row["order"], nil }
			sequentialDefinition.RowKey = func(row Row) (string, error) { return row["id"].(string), nil }
			sequentialDefinition.ValueKey = func(row Row) (interface{}, error) { return row["value"], nil }
			sequential, err := NewMutableIncrementalRangeBoundaryWindow(sequentialDefinition)
			if err != nil {
				t.Fatal(err)
			}
			inserts := []IncrementalRangeBoundaryWindowMutation{
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "a", "partition": "p", "order": int64(1), "value": "A"}},
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "b", "partition": "p", "order": int64(2), "value": "B"}},
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "c", "partition": "p", "order": int64(3), "value": "C"}},
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "d", "partition": "p", "order": int64(4), "value": "D"}},
				{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "e", "partition": "p", "order": int64(5), "value": "E"}},
			}
			if _, err := window.Apply(inserts); err != nil {
				t.Fatal(err)
			}
			if _, err := sequential.Apply(inserts); err != nil {
				t.Fatal(err)
			}
			partitionCalls = 0
			orderCalls = 0
			rowKeyCalls = 0
			valueCalls = 0

			mutations := []IncrementalRangeBoundaryWindowMutation{
				{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "b", Row: Row{"id": "b", "partition": "p", "order": int64(2), "value": "B2"}},
				{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "d", Row: Row{"id": "d", "partition": "p", "order": int64(4), "value": "D2"}},
			}
			if _, err := window.Apply(mutations); err != nil {
				t.Fatal(err)
			}
			for _, mutation := range mutations {
				if _, err := sequential.Apply([]IncrementalRangeBoundaryWindowMutation{mutation}); err != nil {
					t.Fatal(err)
				}
			}
			if partitionCalls != 2 || orderCalls != 2 || rowKeyCalls != 2 || valueCalls != 2 {
				t.Fatalf("stable boundary callback calls = partition:%d order:%d row-key:%d value:%d, want 2 each", partitionCalls, orderCalls, rowKeyCalls, valueCalls)
			}
			if !reflect.DeepEqual(window.outputs, sequential.outputs) {
				t.Fatalf("batched outputs = %#v, sequential outputs = %#v", window.outputs, sequential.outputs)
			}
		})
	}
}

func TestMutableIncrementalRangeBoundaryWindowBatchedStableUpdatesMatchPeersAndPartitions(t *testing.T) {
	for _, descending := range []bool{false, true} {
		for _, kind := range []IncrementalRangeBoundaryWindowKind{IncrementalRangeFirstValue, IncrementalRangeLastValue} {
			t.Run(testMutableRangeBoundaryName(kind, descending), func(t *testing.T) {
				definition := IncrementalRangeBoundaryWindowDefinition{
					Kind:           kind,
					OutputColumn:   "result",
					FramePreceding: 2,
					Descending:     descending,
					PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
				}
				batched, err := NewMutableIncrementalRangeBoundaryWindow(definition)
				if err != nil {
					t.Fatal(err)
				}
				sequential, err := NewMutableIncrementalRangeBoundaryWindow(definition)
				if err != nil {
					t.Fatal(err)
				}
				inserts := []IncrementalRangeBoundaryWindowMutation{
					{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "a", "partition": "p", "order": int64(1), "value": "A"}},
					{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "b", "partition": "p", "order": int64(1), "value": "B"}},
					{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "c", "partition": "p", "order": int64(2), "value": "C"}},
					{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "d", "partition": "p", "order": int64(3), "value": "D"}},
					{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "e", "partition": "q", "order": int64(1), "value": "E"}},
					{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "f", "partition": "q", "order": int64(2), "value": "F"}},
				}
				if _, err := batched.Apply(inserts); err != nil {
					t.Fatal(err)
				}
				if _, err := sequential.Apply(inserts); err != nil {
					t.Fatal(err)
				}
				mutations := []IncrementalRangeBoundaryWindowMutation{
					{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "b", Row: Row{"id": "b", "partition": "p", "order": int64(1), "value": "B2"}},
					{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "c", Row: Row{"id": "c", "partition": "p", "order": int64(2), "value": "C2"}},
				}
				if _, err := batched.Apply(mutations); err != nil {
					t.Fatal(err)
				}
				for _, mutation := range mutations {
					if _, err := sequential.Apply([]IncrementalRangeBoundaryWindowMutation{mutation}); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(batched.outputs, sequential.outputs) {
					t.Fatalf("batched outputs = %#v, sequential outputs = %#v", batched.outputs, sequential.outputs)
				}
			})
		}
	}
}

func TestMutableIncrementalRangeBoundaryWindowIsAtomicAndSupportsDescending(t *testing.T) {
	window, err := NewMutableIncrementalRangeBoundaryWindow(IncrementalRangeBoundaryWindowDefinition{
		Kind:           IncrementalRangeLastValue,
		OutputColumn:   "result",
		FramePreceding: 1,
		Descending:     true,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := window.Apply([]IncrementalRangeBoundaryWindowMutation{
		{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "a", "order": int64(3), "value": "A"}},
		{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "b", "order": int64(2), "value": "B"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeBoundaryValue(t, initial, "a", 1, "A")
	assertMutableRangeBoundaryValue(t, initial, "b", 1, "B")

	_, err = window.Apply([]IncrementalRangeBoundaryWindowMutation{
		{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "missing", Row: Row{"id": "missing", "order": int64(1), "value": "X"}},
		{Operation: IncrementalRangeBoundaryWindowDelete, Key: "also-missing"},
	})
	if !errors.Is(err, ErrMutableIncrementalRangeBoundaryWindowMissingKey) {
		t.Fatalf("invalid batch error = %v", err)
	}

	changes, err := window.Apply([]IncrementalRangeBoundaryWindowMutation{{
		Operation: IncrementalRangeBoundaryWindowInsert,
		Row:       Row{"id": "c", "order": int64(1), "value": "C"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeBoundaryValue(t, changes, "c", 1, "C")
}

func TestMutableIncrementalRangeBoundaryWindowMatchesAppendOnlyRebuild(t *testing.T) {
	for _, descending := range []bool{false, true} {
		for _, kind := range []IncrementalRangeBoundaryWindowKind{IncrementalRangeFirstValue, IncrementalRangeLastValue} {
			t.Run(testMutableRangeBoundaryName(kind, descending), func(t *testing.T) {
				definition := IncrementalRangeBoundaryWindowDefinition{
					Kind:           kind,
					OutputColumn:   "result",
					FramePreceding: 2,
					PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
					Descending:     descending,
				}
				window, err := NewMutableIncrementalRangeBoundaryWindow(definition)
				if err != nil {
					t.Fatal(err)
				}
				state := make(map[string]Row)
				apply := func(mutations ...IncrementalRangeBoundaryWindowMutation) {
					t.Helper()
					if _, err := window.Apply(mutations); err != nil {
						t.Fatal(err)
					}
					for _, mutation := range mutations {
						switch mutation.Operation {
						case IncrementalRangeBoundaryWindowInsert, IncrementalRangeBoundaryWindowUpdate:
							state[mutation.Row["id"].(string)] = cloneMutableIncrementalRangeBoundaryWindowRow(mutation.Row)
						case IncrementalRangeBoundaryWindowDelete:
							delete(state, mutation.Key)
						}
					}
					want := rebuildMutableIncrementalRangeBoundaryExpected(t, definition, state)
					if !reflect.DeepEqual(window.outputs, want) {
						t.Fatalf("state mismatch after %#v: got %#v want %#v", mutations, window.outputs, want)
					}
				}

				apply(
					IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "a", "partition": "p1", "order": int64(5), "value": "A"}},
					IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "b", "partition": "p1", "order": int64(5), "value": "B"}},
					IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "c", "partition": "p1", "order": int64(4), "value": "C"}},
					IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "d", "partition": "p1", "order": int64(3), "value": "D"}},
					IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "e", "partition": "p2", "order": int64(2), "value": "E"}},
					IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "f", "partition": "p2", "order": int64(1), "value": "F"}},
				)
				apply(IncrementalRangeBoundaryWindowMutation{
					Operation: IncrementalRangeBoundaryWindowUpdate,
					Key:       "b",
					Row:       Row{"id": "b", "partition": "p1", "order": int64(5), "value": "B2"},
				})
				apply(IncrementalRangeBoundaryWindowMutation{
					Operation: IncrementalRangeBoundaryWindowUpdate,
					Key:       "c",
					Row:       Row{"id": "c", "partition": "p1", "order": int64(6), "value": "C6"},
				})
				apply(IncrementalRangeBoundaryWindowMutation{
					Operation: IncrementalRangeBoundaryWindowUpdate,
					Key:       "d",
					Row:       Row{"id": "d", "partition": "p2", "order": int64(3), "value": "D2"},
				})
				apply(IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowDelete, Key: "a"})
				apply(IncrementalRangeBoundaryWindowMutation{Operation: IncrementalRangeBoundaryWindowInsert, Row: Row{"id": "g", "partition": "p2", "order": int64(3), "value": "G"}})
			})
		}
	}
}

func testMutableRangeBoundaryName(kind IncrementalRangeBoundaryWindowKind, descending bool) string {
	name := "first"
	if kind == IncrementalRangeLastValue {
		name = "last"
	}
	if descending {
		name += "-descending"
	}
	return name
}

func rebuildMutableIncrementalRangeBoundaryExpected(t *testing.T, definition IncrementalRangeBoundaryWindowDefinition, state map[string]Row) map[string]Row {
	t.Helper()
	rows := make([]Row, 0, len(state))
	for _, row := range state {
		rows = append(rows, cloneMutableIncrementalRangeBoundaryWindowRow(row))
	}
	sort.Slice(rows, func(left, right int) bool {
		leftPartition, _ := definition.PartitionKey(rows[left])
		rightPartition, _ := definition.PartitionKey(rows[right])
		if leftPartition != rightPartition {
			return leftPartition < rightPartition
		}
		leftOrder := rows[left]["order"].(int64)
		rightOrder := rows[right]["order"].(int64)
		if leftOrder != rightOrder {
			if definition.Descending {
				return leftOrder > rightOrder
			}
			return leftOrder < rightOrder
		}
		return rows[left]["id"].(string) < rows[right]["id"].(string)
	})
	appendWindow, err := NewIncrementalRangeBoundaryWindow(definition)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := appendWindow.Append(rows)
	if err != nil {
		t.Fatal(err)
	}
	outputs := make(map[string]Row, len(rows))
	for _, change := range changes {
		if change.Diff > 0 {
			outputs[change.Key] = cloneMutableIncrementalRangeBoundaryWindowRow(change.Row)
		} else if change.Diff < 0 {
			delete(outputs, change.Key)
		}
	}
	return outputs
}

func assertMutableRangeBoundaryValue(t *testing.T, changes []DifferentialRow, key string, diff int64, want interface{}) {
	t.Helper()
	for _, change := range changes {
		if change.Key == key && change.Diff == diff && change.Row["result"] == want {
			return
		}
	}
	t.Fatalf("missing boundary change key=%q diff=%d value=%v in %#v", key, diff, want, changes)
}
