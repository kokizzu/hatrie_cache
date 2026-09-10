package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestMutableIncrementalFrameWindowAppliesInsertUpdateDelete(t *testing.T) {
	window, err := NewMutableIncrementalFrameWindow(m065lFrameWindowDefinition(IncrementalWindowFrameSumInt64, 2))
	if err != nil {
		t.Fatalf("NewMutableIncrementalFrameWindow() error = %v", err)
	}
	rows := []Row{
		{"id": "a1", "partition": "a", "order": int64(1), "value": int64(10)},
		{"id": "a2", "partition": "a", "order": int64(2), "value": int64(20)},
		{"id": "a3", "partition": "a", "order": int64(3), "value": int64(30)},
		{"id": "b1", "partition": "b", "order": int64(1), "value": int64(100)},
		{"id": "b2", "partition": "b", "order": int64(2), "value": int64(200)},
	}
	state := make(map[string]Row)
	changes, err := window.Apply(m065lInsertMutations(rows))
	if err != nil {
		t.Fatalf("insert error = %v", err)
	}
	applyM065LDifferentialChanges(state, changes)
	assertM065LFrameState(t, state, rows)

	updated := Row{"id": "a2", "partition": "a", "order": int64(2), "value": int64(50)}
	changes, err = window.Apply([]IncrementalFrameWindowMutation{{
		Kind: IncrementalFrameWindowUpdate,
		Key:  "a2",
		Row:  updated,
	}})
	if err != nil {
		t.Fatalf("update error = %v", err)
	}
	applyM065LDifferentialChanges(state, changes)
	rows[1] = updated
	assertM065LFrameState(t, state, rows)
	for _, change := range changes {
		if change.Key == "b1" || change.Key == "b2" {
			t.Fatalf("unaffected partition emitted change: %#v", change)
		}
	}

	inserted := Row{"id": "a0", "partition": "a", "order": int64(0), "value": int64(5)}
	changes, err = window.Apply([]IncrementalFrameWindowMutation{{
		Kind: IncrementalFrameWindowInsert,
		Key:  "a0",
		Row:  inserted,
	}})
	if err != nil {
		t.Fatalf("out-of-order insert error = %v", err)
	}
	applyM065LDifferentialChanges(state, changes)
	rows = append(rows, inserted)
	assertM065LFrameState(t, state, rows)

	changes, err = window.Apply([]IncrementalFrameWindowMutation{{
		Kind: IncrementalFrameWindowDelete,
		Key:  "a2",
	}})
	if err != nil {
		t.Fatalf("delete error = %v", err)
	}
	applyM065LDifferentialChanges(state, changes)
	rows = m065lRemoveFrameRow(rows, "a2")
	assertM065LFrameState(t, state, rows)
}

func TestMutableIncrementalFrameWindowIsAtomicAndAppendOnlyRemainsCheap(t *testing.T) {
	definition := m065lFrameWindowDefinition(IncrementalWindowFrameCount, 1)
	appendOnly, err := NewIncrementalFrameWindow(definition)
	if err != nil {
		t.Fatalf("NewIncrementalFrameWindow() error = %v", err)
	}
	if _, err := appendOnly.Apply([]IncrementalFrameWindowMutation{{Kind: IncrementalFrameWindowInsert, Key: "a", Row: Row{"id": "a", "partition": "p", "order": int64(1)}}}); !errors.Is(err, ErrIncrementalFrameWindowMutationsDisabled) {
		t.Fatalf("append-only Apply() error = %v, want %v", err, ErrIncrementalFrameWindowMutationsDisabled)
	}

	window, err := NewMutableIncrementalFrameWindow(definition)
	if err != nil {
		t.Fatalf("NewMutableIncrementalFrameWindow() error = %v", err)
	}
	seed := Row{"id": "a", "partition": "p", "order": int64(1)}
	if _, err := window.Apply([]IncrementalFrameWindowMutation{{Kind: IncrementalFrameWindowInsert, Key: "a", Row: seed}}); err != nil {
		t.Fatalf("seed insert error = %v", err)
	}
	bad := Row{"id": "different", "partition": "p", "order": int64(1)}
	if _, err := window.Apply([]IncrementalFrameWindowMutation{{Kind: IncrementalFrameWindowUpdate, Key: "a", Row: bad}}); !errors.Is(err, ErrIncrementalFrameWindowMutationKeyMismatch) {
		t.Fatalf("mismatched update error = %v, want %v", err, ErrIncrementalFrameWindowMutationKeyMismatch)
	}
	changes, err := window.Apply([]IncrementalFrameWindowMutation{{
		Kind: IncrementalFrameWindowUpdate,
		Key:  "a",
		Row:  Row{"id": "a", "partition": "p", "order": int64(1)},
	}})
	if err != nil {
		t.Fatalf("post-error update = %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("post-error no-op update changes = %#v, want none", changes)
	}
}

func TestMutableIncrementalFrameWindowSupportsAllAggregateKinds(t *testing.T) {
	kinds := []IncrementalFrameWindowKind{
		IncrementalWindowFrameCount,
		IncrementalWindowFrameSumInt64,
		IncrementalWindowFrameMinInt64,
		IncrementalWindowFrameMaxInt64,
		IncrementalWindowFrameAvgInt64,
		IncrementalWindowFrameCountDistinctInt64,
	}
	seed := []Row{
		{"id": "a1", "partition": "a", "order": int64(1), "value": int64(2)},
		{"id": "a2", "partition": "a", "order": int64(2), "value": int64(2)},
		{"id": "a3", "partition": "a", "order": int64(3), "value": nil},
		{"id": "a4", "partition": "a", "order": int64(4), "value": int64(5)},
	}
	for _, kind := range kinds {
		kind := kind
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			window, err := NewMutableIncrementalFrameWindow(m065lFrameWindowDefinition(kind, 2))
			if err != nil {
				t.Fatalf("NewMutableIncrementalFrameWindow() error = %v", err)
			}
			state := make(map[string]Row)
			changes, err := window.Apply(m065lInsertMutations(seed))
			if err != nil {
				t.Fatalf("seed insert error = %v", err)
			}
			applyM065LDifferentialChanges(state, changes)
			if want := m065lExpectedFrameValues(seed, 2, kind); !reflect.DeepEqual(state, want) {
				t.Fatalf("seed state = %#v, want %#v", state, want)
			}

			updated := Row{"id": "a2", "partition": "a", "order": int64(2), "value": int64(9)}
			changes, err = window.Apply([]IncrementalFrameWindowMutation{{Kind: IncrementalFrameWindowUpdate, Key: "a2", Row: updated}})
			if err != nil {
				t.Fatalf("update error = %v", err)
			}
			applyM065LDifferentialChanges(state, changes)
			current := append([]Row(nil), seed...)
			current[1] = updated
			if want := m065lExpectedFrameValues(current, 2, kind); !reflect.DeepEqual(state, want) {
				t.Fatalf("updated state = %#v, want %#v", state, want)
			}

			changes, err = window.Apply([]IncrementalFrameWindowMutation{{Kind: IncrementalFrameWindowDelete, Key: "a1"}})
			if err != nil {
				t.Fatalf("delete error = %v", err)
			}
			applyM065LDifferentialChanges(state, changes)
			current = m065lRemoveFrameRow(current, "a1")
			if want := m065lExpectedFrameValues(current, 2, kind); !reflect.DeepEqual(state, want) {
				t.Fatalf("deleted state = %#v, want %#v", state, want)
			}
		})
	}
}

func TestMutableIncrementalFrameWindowMovesRowsAndKeepsFailedRebuildAtomic(t *testing.T) {
	window, err := NewMutableIncrementalFrameWindow(m065lFrameWindowDefinition(IncrementalWindowFrameSumInt64, 1))
	if err != nil {
		t.Fatalf("NewMutableIncrementalFrameWindow() error = %v", err)
	}
	rows := []Row{
		{"id": "a1", "partition": "a", "order": int64(1), "value": int64(10)},
		{"id": "a2", "partition": "a", "order": int64(2), "value": int64(20)},
		{"id": "b1", "partition": "b", "order": int64(1), "value": int64(100)},
	}
	state := make(map[string]Row)
	changes, err := window.Apply(m065lInsertMutations(rows))
	if err != nil {
		t.Fatalf("seed insert error = %v", err)
	}
	applyM065LDifferentialChanges(state, changes)

	moved := Row{"id": "a2", "partition": "b", "order": int64(2), "value": int64(20)}
	changes, err = window.Apply([]IncrementalFrameWindowMutation{{
		Kind: IncrementalFrameWindowUpdate,
		Key:  "a2",
		Row:  moved,
	}})
	if err != nil {
		t.Fatalf("partition move error = %v", err)
	}
	applyM065LDifferentialChanges(state, changes)
	rows[1] = moved
	assertM065LFrameState(t, state, rows)
	for _, change := range changes {
		if change.Key != "a2" {
			t.Fatalf("partition move changed unaffected row: %#v", change)
		}
	}

	definition := m065lFrameWindowDefinition(IncrementalWindowFrameSumInt64, 1)
	definition.OrderKey = func(row Row) (interface{}, error) {
		if row["order"] == int64(99) {
			return nil, errors.New("synthetic order failure")
		}
		return row["order"], nil
	}
	atomicWindow, err := NewMutableIncrementalFrameWindow(definition)
	if err != nil {
		t.Fatalf("atomic window construction error = %v", err)
	}
	atomicRow := Row{"id": "x", "partition": "x", "order": int64(1), "value": int64(7)}
	if _, err := atomicWindow.Apply(m065lInsertMutations([]Row{atomicRow})); err != nil {
		t.Fatalf("atomic seed insert error = %v", err)
	}
	badRow := Row{"id": "x", "partition": "x", "order": int64(99), "value": int64(7)}
	if _, err := atomicWindow.Apply([]IncrementalFrameWindowMutation{{
		Kind: IncrementalFrameWindowUpdate,
		Key:  "x",
		Row:  badRow,
	}}); err == nil || !strings.Contains(err.Error(), "synthetic order failure") {
		t.Fatalf("failed rebuild error = %v, want synthetic order failure", err)
	}
	changes, err = atomicWindow.Apply([]IncrementalFrameWindowMutation{{
		Kind: IncrementalFrameWindowUpdate,
		Key:  "x",
		Row:  atomicRow,
	}})
	if err != nil {
		t.Fatalf("post-error no-op update error = %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("post-error no-op update changes = %#v, want none", changes)
	}
}

func m065lFrameWindowDefinition(kind IncrementalFrameWindowKind, preceding int) IncrementalFrameWindowDefinition {
	definition := IncrementalFrameWindowDefinition{
		Kind:           kind,
		OutputColumn:   "frame_value",
		FramePreceding: preceding,
		PartitionKey: func(row Row) (string, error) {
			return row["partition"].(string), nil
		},
		OrderKey: func(row Row) (interface{}, error) {
			return row["order"], nil
		},
		RowKey: func(row Row) (string, error) {
			return row["id"].(string), nil
		},
	}
	if kind != IncrementalWindowFrameCount {
		definition.ValueKey = func(row Row) (interface{}, error) {
			return row["value"], nil
		}
	}
	return definition
}

func m065lInsertMutations(rows []Row) []IncrementalFrameWindowMutation {
	mutations := make([]IncrementalFrameWindowMutation, len(rows))
	for index, row := range rows {
		mutations[index] = IncrementalFrameWindowMutation{
			Kind: IncrementalFrameWindowInsert,
			Key:  row["id"].(string),
			Row:  row,
		}
	}
	return mutations
}

func applyM065LDifferentialChanges(state map[string]Row, changes []DifferentialRow) {
	for _, change := range changes {
		switch change.Diff {
		case -1:
			delete(state, change.Key)
		case 1:
			state[change.Key] = cloneIncrementalFrameWindowRow(change.Row)
		default:
			panic(fmt.Sprintf("unexpected differential weight %d", change.Diff))
		}
	}
}

func assertM065LFrameState(t *testing.T, actual map[string]Row, rows []Row) {
	t.Helper()
	want := m065lExpectedFrameValues(rows, 2, IncrementalWindowFrameSumInt64)
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("state = %#v, want %#v", actual, want)
	}
}

func m065lExpectedFrameValues(rows []Row, preceding int, kind IncrementalFrameWindowKind) map[string]Row {
	sorted := append([]Row(nil), rows...)
	sort.SliceStable(sorted, func(left, right int) bool {
		if sorted[left]["partition"] != sorted[right]["partition"] {
			return sorted[left]["partition"].(string) < sorted[right]["partition"].(string)
		}
		if sorted[left]["order"].(int64) != sorted[right]["order"].(int64) {
			return sorted[left]["order"].(int64) < sorted[right]["order"].(int64)
		}
		return sorted[left]["id"].(string) < sorted[right]["id"].(string)
	})
	want := make(map[string]Row, len(sorted))
	for start := 0; start < len(sorted); {
		end := start + 1
		for end < len(sorted) && sorted[end]["partition"] == sorted[start]["partition"] {
			end++
		}
		for index := start; index < end; index++ {
			frameStart := index - preceding
			if frameStart < start {
				frameStart = start
			}
			var sum int64
			var count int64
			var validCount int64
			var minimum, maximum int64
			var hasValue bool
			distinct := make(map[int64]struct{})
			for frameIndex := frameStart; frameIndex <= index; frameIndex++ {
				if kind == IncrementalWindowFrameCount {
					count++
					continue
				}
				value, ok := sorted[frameIndex]["value"].(int64)
				if !ok {
					continue
				}
				sum += value
				validCount++
				if !hasValue {
					minimum = value
					maximum = value
					hasValue = true
				} else if value < minimum {
					minimum = value
				}
				if value > maximum {
					maximum = value
				}
				distinct[value] = struct{}{}
			}
			var aggregate interface{}
			switch kind {
			case IncrementalWindowFrameCount:
				aggregate = int64(index - frameStart + 1)
			case IncrementalWindowFrameSumInt64:
				if hasValue {
					aggregate = sum
				}
			case IncrementalWindowFrameMinInt64:
				if hasValue {
					aggregate = minimum
				}
			case IncrementalWindowFrameMaxInt64:
				if hasValue {
					aggregate = maximum
				}
			case IncrementalWindowFrameAvgInt64:
				if hasValue {
					aggregate = float64(sum) / float64(validCount)
				}
			case IncrementalWindowFrameCountDistinctInt64:
				aggregate = int64(len(distinct))
			}
			output := cloneIncrementalFrameWindowRow(sorted[index])
			output["frame_value"] = aggregate
			want[sorted[index]["id"].(string)] = output
		}
		start = end
	}
	return want
}

func m065lRemoveFrameRow(rows []Row, key string) []Row {
	filtered := make([]Row, 0, len(rows)-1)
	for _, row := range rows {
		if row["id"] != key {
			filtered = append(filtered, row)
		}
	}
	return filtered
}
