package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestMutableIncrementalRangeNthValueWindowTracksUpdatesAndDeletes(t *testing.T) {
	window, err := NewMutableIncrementalRangeNthValueWindow(IncrementalRangeNthValueWindowDefinition{
		Position:       2,
		OutputColumn:   "result",
		FramePreceding: 2,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	initial, err := window.Apply([]IncrementalRangeNthValueWindowMutation{
		{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "a", "order": int64(1), "value": "A"}},
		{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "b", "order": int64(2), "value": "B"}},
		{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "c", "order": int64(3), "value": "C"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeNthValue(t, initial, "a", 1, nil)
	assertMutableRangeNthValue(t, initial, "b", 1, "B")
	assertMutableRangeNthValue(t, initial, "c", 1, "B")

	updated, err := window.Apply([]IncrementalRangeNthValueWindowMutation{{
		Operation: IncrementalRangeNthValueWindowUpdate,
		Key:       "b",
		Row:       Row{"id": "b", "order": int64(2), "value": "B2"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeNthValue(t, updated, "b", -1, "B")
	assertMutableRangeNthValue(t, updated, "b", 1, "B2")
	assertMutableRangeNthValue(t, updated, "c", -1, "B")
	assertMutableRangeNthValue(t, updated, "c", 1, "B2")

	deleted, err := window.Apply([]IncrementalRangeNthValueWindowMutation{{
		Operation: IncrementalRangeNthValueWindowDelete,
		Key:       "a",
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeNthValue(t, deleted, "a", -1, nil)
	assertMutableRangeNthValue(t, deleted, "b", -1, "B2")
	assertMutableRangeNthValue(t, deleted, "b", 1, nil)
	assertMutableRangeNthValue(t, deleted, "c", -1, "B2")
	assertMutableRangeNthValue(t, deleted, "c", 1, "C")
}

func TestMutableIncrementalRangeNthValueWindowIsAtomicAndSupportsDescending(t *testing.T) {
	window, err := NewMutableIncrementalRangeNthValueWindow(IncrementalRangeNthValueWindowDefinition{
		Position:       1,
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
	initial, err := window.Apply([]IncrementalRangeNthValueWindowMutation{
		{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "a", "order": int64(3), "value": "A"}},
		{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "b", "order": int64(2), "value": "B"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeNthValue(t, initial, "a", 1, "A")
	assertMutableRangeNthValue(t, initial, "b", 1, "A")

	_, err = window.Apply([]IncrementalRangeNthValueWindowMutation{
		{Operation: IncrementalRangeNthValueWindowUpdate, Key: "missing", Row: Row{"id": "missing", "order": int64(1), "value": "X"}},
		{Operation: IncrementalRangeNthValueWindowDelete, Key: "also-missing"},
	})
	if !errors.Is(err, ErrMutableIncrementalRangeNthValueWindowMissingKey) {
		t.Fatalf("invalid batch error = %v", err)
	}

	changes, err := window.Apply([]IncrementalRangeNthValueWindowMutation{{
		Operation: IncrementalRangeNthValueWindowInsert,
		Row:       Row{"id": "c", "order": int64(1), "value": "C"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeNthValue(t, changes, "c", 1, "B")
}

func TestMutableIncrementalRangeNthValueWindowMatchesAppendOnlyRebuild(t *testing.T) {
	for _, descending := range []bool{false, true} {
		for _, position := range []int{1, 2, 3} {
			t.Run(testMutableRangeNthValueName(position, descending), func(t *testing.T) {
				definition := IncrementalRangeNthValueWindowDefinition{
					Position:       position,
					OutputColumn:   "result",
					FramePreceding: 2,
					PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
					Descending:     descending,
				}
				window, err := NewMutableIncrementalRangeNthValueWindow(definition)
				if err != nil {
					t.Fatal(err)
				}
				state := make(map[string]Row)
				apply := func(mutations ...IncrementalRangeNthValueWindowMutation) {
					t.Helper()
					if _, err := window.Apply(mutations); err != nil {
						t.Fatal(err)
					}
					for _, mutation := range mutations {
						switch mutation.Operation {
						case IncrementalRangeNthValueWindowInsert, IncrementalRangeNthValueWindowUpdate:
							state[mutation.Row["id"].(string)] = cloneMutableIncrementalRangeNthValueWindowRow(mutation.Row)
						case IncrementalRangeNthValueWindowDelete:
							delete(state, mutation.Key)
						}
					}
					want := rebuildMutableIncrementalRangeNthValueExpected(t, definition, state)
					if !reflect.DeepEqual(window.outputs, want) {
						t.Fatalf("state mismatch after %#v: got %#v want %#v", mutations, window.outputs, want)
					}
				}

				apply(
					IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "a", "partition": "p1", "order": int64(5), "value": "A"}},
					IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "b", "partition": "p1", "order": int64(5), "value": "B"}},
					IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "c", "partition": "p1", "order": int64(4), "value": "C"}},
					IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "d", "partition": "p1", "order": int64(3), "value": "D"}},
					IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "e", "partition": "p2", "order": int64(2), "value": "E"}},
					IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "f", "partition": "p2", "order": int64(1), "value": "F"}},
				)
				apply(IncrementalRangeNthValueWindowMutation{
					Operation: IncrementalRangeNthValueWindowUpdate,
					Key:       "b",
					Row:       Row{"id": "b", "partition": "p1", "order": int64(5), "value": "B2"},
				})
				apply(IncrementalRangeNthValueWindowMutation{
					Operation: IncrementalRangeNthValueWindowUpdate,
					Key:       "c",
					Row:       Row{"id": "c", "partition": "p1", "order": int64(6), "value": "C6"},
				})
				apply(IncrementalRangeNthValueWindowMutation{
					Operation: IncrementalRangeNthValueWindowUpdate,
					Key:       "d",
					Row:       Row{"id": "d", "partition": "p2", "order": int64(3), "value": "D2"},
				})
				apply(IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowDelete, Key: "a"})
				apply(IncrementalRangeNthValueWindowMutation{Operation: IncrementalRangeNthValueWindowInsert, Row: Row{"id": "g", "partition": "p2", "order": int64(3), "value": "G"}})
			})
		}
	}
}

func testMutableRangeNthValueName(position int, descending bool) string {
	name := "position-" + string(rune('0'+position))
	if descending {
		name += "-descending"
	}
	return name
}

func rebuildMutableIncrementalRangeNthValueExpected(t *testing.T, definition IncrementalRangeNthValueWindowDefinition, state map[string]Row) map[string]Row {
	t.Helper()
	rows := make([]Row, 0, len(state))
	for _, row := range state {
		rows = append(rows, cloneMutableIncrementalRangeNthValueWindowRow(row))
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
	appendWindow, err := NewIncrementalRangeNthValueWindow(definition)
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
			outputs[change.Key] = cloneMutableIncrementalRangeNthValueWindowRow(change.Row)
		} else if change.Diff < 0 {
			delete(outputs, change.Key)
		}
	}
	return outputs
}

func assertMutableRangeNthValue(t *testing.T, changes []DifferentialRow, key string, diff int64, want interface{}) {
	t.Helper()
	for _, change := range changes {
		if change.Key != key || change.Diff != diff {
			continue
		}
		value, ok := change.Row["result"]
		if ok && reflect.DeepEqual(value, want) {
			return
		}
		if !ok && want == nil {
			return
		}
	}
	t.Fatalf("missing NTH_VALUE change key=%q diff=%d value=%v in %#v", key, diff, want, changes)
}
