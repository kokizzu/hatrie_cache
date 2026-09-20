package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestMutableIncrementalRangeWindowAppliesSumChanges(t *testing.T) {
	window, err := NewMutableIncrementalRangeWindow(IncrementalRangeWindowDefinition{
		Kind:           IncrementalRangeWindowSumInt64,
		OutputColumn:   "range_sum",
		FramePreceding: 2,
		PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	inserted, err := window.Apply([]IncrementalRangeWindowMutation{
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("a", 1, 10)},
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("b", 2, 20)},
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("c", 3, 30)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeValue(t, inserted, "a", int64(10))
	assertMutableRangeValue(t, inserted, "b", int64(30))
	assertMutableRangeValue(t, inserted, "c", int64(60))

	updated, err := window.Apply([]IncrementalRangeWindowMutation{{
		Operation: IncrementalRangeWindowUpdate,
		Key:       "b",
		Row:       mutableRangeRow("b", 2, 200),
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangePair(t, updated, "b", int64(30), int64(210))
	assertMutableRangePair(t, updated, "c", int64(60), int64(240))

	deleted, err := window.Apply([]IncrementalRangeWindowMutation{{
		Operation: IncrementalRangeWindowDelete,
		Key:       "b",
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangePair(t, deleted, "c", int64(240), int64(40))
	if !hasMutableRangeChange(deleted, "b", -1, int64(210)) {
		t.Fatalf("delete did not retract b: %#v", deleted)
	}
}

func TestMutableIncrementalRangeWindowBatchedStableSumUpdates(t *testing.T) {
	valueCalls := 0
	orderCalls := 0
	rowKeyCalls := 0
	window, err := NewMutableIncrementalRangeWindow(IncrementalRangeWindowDefinition{
		Kind:           IncrementalRangeWindowSumInt64,
		OutputColumn:   "range_sum",
		FramePreceding: 2,
		PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
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
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]IncrementalRangeWindowMutation{
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("a", 1, 10)},
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("b", 2, 20)},
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("c", 3, 30)},
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("d", 4, 40)},
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("e", 5, 50)},
	}); err != nil {
		t.Fatal(err)
	}
	valueCalls = 0
	orderCalls = 0
	rowKeyCalls = 0

	changes, err := window.Apply([]IncrementalRangeWindowMutation{
		{Operation: IncrementalRangeWindowUpdate, Key: "b", Row: mutableRangeRow("b", 2, 200)},
		{Operation: IncrementalRangeWindowUpdate, Key: "d", Row: mutableRangeRow("d", 4, 400)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if valueCalls != 4 {
		t.Fatalf("batched stable SUM value callback calls = %d, want 4", valueCalls)
	}
	if orderCalls != 2 {
		t.Fatalf("batched stable SUM order callback calls = %d, want 2", orderCalls)
	}
	if rowKeyCalls != 2 {
		t.Fatalf("batched stable SUM row-key callback calls = %d, want 2", rowKeyCalls)
	}
	assertMutableRangePair(t, changes, "b", int64(30), int64(210))
	assertMutableRangePair(t, changes, "c", int64(60), int64(240))
	assertMutableRangePair(t, changes, "d", int64(90), int64(630))
	assertMutableRangePair(t, changes, "e", int64(120), int64(480))
	if hasMutableRangeChange(changes, "a", -1, int64(10)) || hasMutableRangeChange(changes, "a", 1, int64(10)) {
		t.Fatalf("unaffected row changed: %#v", changes)
	}
}

func TestMutableIncrementalRangeWindowBatchedStableSumMatchesSequential(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprintf("descending=%t", descending), func(t *testing.T) {
			definition := IncrementalRangeWindowDefinition{
				Kind:           IncrementalRangeWindowSumInt64,
				OutputColumn:   "range_sum",
				FramePreceding: 2,
				Descending:     descending,
				PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
				ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
			}
			batched, err := NewMutableIncrementalRangeWindow(definition)
			if err != nil {
				t.Fatal(err)
			}
			sequential, err := NewMutableIncrementalRangeWindow(definition)
			if err != nil {
				t.Fatal(err)
			}
			inserts := []IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("a", 1, 10)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("b", 2, 20)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("c", 3, 30)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("d", 4, 40)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("e", 5, 50)},
			}
			if _, err := batched.Apply(inserts); err != nil {
				t.Fatal(err)
			}
			if _, err := sequential.Apply(inserts); err != nil {
				t.Fatal(err)
			}
			mutations := []IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowUpdate, Key: "b", Row: mutableRangeRow("b", 2, 200)},
				{Operation: IncrementalRangeWindowUpdate, Key: "d", Row: mutableRangeRow("d", 4, 400)},
			}
			if _, err := batched.Apply(mutations); err != nil {
				t.Fatal(err)
			}
			for _, mutation := range mutations {
				if _, err := sequential.Apply([]IncrementalRangeWindowMutation{mutation}); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(batched.outputs, sequential.outputs) {
				t.Fatalf("batched outputs = %#v, sequential outputs = %#v", batched.outputs, sequential.outputs)
			}
		})
	}
}

func TestMutableIncrementalRangeWindowBatchedStableExtremaUpdates(t *testing.T) {
	for _, kind := range []IncrementalRangeWindowKind{IncrementalRangeWindowMinInt64, IncrementalRangeWindowMaxInt64} {
		t.Run(fmt.Sprintf("kind=%d", kind), func(t *testing.T) {
			partitionCalls := 0
			orderCalls := 0
			rowKeyCalls := 0
			definition := IncrementalRangeWindowDefinition{
				Kind:           kind,
				OutputColumn:   "range_value",
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
				ValueKey: func(row Row) (interface{}, error) { return row["value"], nil },
			}
			window, err := NewMutableIncrementalRangeWindow(definition)
			if err != nil {
				t.Fatal(err)
			}
			sequentialDefinition := definition
			sequentialDefinition.PartitionKey = func(row Row) (string, error) { return row["partition"].(string), nil }
			sequentialDefinition.OrderKey = func(row Row) (interface{}, error) { return row["order"], nil }
			sequentialDefinition.RowKey = func(row Row) (string, error) { return row["id"].(string), nil }
			sequential, err := NewMutableIncrementalRangeWindow(sequentialDefinition)
			if err != nil {
				t.Fatal(err)
			}
			inserts := []IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "a", "partition": "p", "order": int64(1), "value": int64(10)}},
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "b", "partition": "p", "order": int64(2), "value": int64(20)}},
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "c", "partition": "p", "order": int64(3), "value": int64(30)}},
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "d", "partition": "p", "order": int64(4), "value": int64(40)}},
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "e", "partition": "p", "order": int64(5), "value": int64(50)}},
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
			mutations := []IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowUpdate, Key: "b", Row: Row{"id": "b", "partition": "p", "order": int64(2), "value": int64(200)}},
				{Operation: IncrementalRangeWindowUpdate, Key: "d", Row: Row{"id": "d", "partition": "p", "order": int64(4), "value": int64(400)}},
			}
			if _, err := window.Apply(mutations); err != nil {
				t.Fatal(err)
			}
			for _, mutation := range mutations {
				if _, err := sequential.Apply([]IncrementalRangeWindowMutation{mutation}); err != nil {
					t.Fatal(err)
				}
			}
			if partitionCalls != 2 || orderCalls != 2 || rowKeyCalls != 2 {
				t.Fatalf("stable extrema callback calls = partition:%d order:%d row-key:%d, want 2 each", partitionCalls, orderCalls, rowKeyCalls)
			}
			if !reflect.DeepEqual(window.outputs, sequential.outputs) {
				t.Fatalf("batched outputs = %#v, sequential outputs = %#v", window.outputs, sequential.outputs)
			}
		})
	}
}

func TestMutableIncrementalRangeWindowBatchedStableExtremaMatchesPeersDescendingAndNulls(t *testing.T) {
	for _, kind := range []IncrementalRangeWindowKind{IncrementalRangeWindowMinInt64, IncrementalRangeWindowMaxInt64} {
		for _, descending := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind=%d/descending=%t", kind, descending), func(t *testing.T) {
				definition := IncrementalRangeWindowDefinition{
					Kind:           kind,
					OutputColumn:   "range_value",
					FramePreceding: 2,
					Descending:     descending,
					PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
				}
				batched, err := NewMutableIncrementalRangeWindow(definition)
				if err != nil {
					t.Fatal(err)
				}
				sequential, err := NewMutableIncrementalRangeWindow(definition)
				if err != nil {
					t.Fatal(err)
				}
				inserts := []IncrementalRangeWindowMutation{
					{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "a", "partition": "p", "order": int64(1), "value": int64(10)}},
					{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "b", "partition": "p", "order": int64(1), "value": nil}},
					{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "c", "partition": "p", "order": int64(2), "value": int64(30)}},
					{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "d", "partition": "p", "order": int64(3), "value": int64(5)}},
					{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "e", "partition": "q", "order": int64(1), "value": int64(100)}},
				}
				if _, err := batched.Apply(inserts); err != nil {
					t.Fatal(err)
				}
				if _, err := sequential.Apply(inserts); err != nil {
					t.Fatal(err)
				}
				mutations := []IncrementalRangeWindowMutation{
					{Operation: IncrementalRangeWindowUpdate, Key: "b", Row: Row{"id": "b", "partition": "p", "order": int64(1), "value": int64(20)}},
					{Operation: IncrementalRangeWindowUpdate, Key: "d", Row: Row{"id": "d", "partition": "p", "order": int64(3), "value": int64(-50)}},
				}
				if _, err := batched.Apply(mutations); err != nil {
					t.Fatal(err)
				}
				for _, mutation := range mutations {
					if _, err := sequential.Apply([]IncrementalRangeWindowMutation{mutation}); err != nil {
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

func TestMutableIncrementalRangeWindowBatchedStableExtremaInvalidIsAtomic(t *testing.T) {
	for _, kind := range []IncrementalRangeWindowKind{IncrementalRangeWindowMinInt64, IncrementalRangeWindowMaxInt64} {
		t.Run(fmt.Sprintf("kind=%d", kind), func(t *testing.T) {
			window, err := NewMutableIncrementalRangeWindow(IncrementalRangeWindowDefinition{
				Kind:           kind,
				OutputColumn:   "range_value",
				FramePreceding: 2,
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
				ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := window.Apply([]IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("a", 1, 10)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("b", 2, 20)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("c", 3, 30)},
			}); err != nil {
				t.Fatal(err)
			}
			before := make(map[string]Row, len(window.outputs))
			for key, row := range window.outputs {
				before[key] = cloneIncrementalRangeWindowRow(row)
			}
			_, err = window.Apply([]IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowUpdate, Key: "a", Row: mutableRangeRow("a", 1, 100)},
				{Operation: IncrementalRangeWindowUpdate, Key: "b", Row: Row{"id": "b", "order": int64(2), "value": "invalid"}},
			})
			if !errors.Is(err, ErrIncrementalRangeWindowExtremaValueInvalid) {
				t.Fatalf("invalid extrema batch error = %v", err)
			}
			if !reflect.DeepEqual(window.outputs, before) {
				t.Fatalf("invalid extrema batch changed outputs: got %#v want %#v", window.outputs, before)
			}
		})
	}
}

func TestMutableIncrementalRangeWindowBatchedStableDistinctAndAvgUpdates(t *testing.T) {
	for _, kind := range []IncrementalRangeWindowKind{IncrementalRangeWindowCountDistinctInt64, IncrementalRangeWindowAvgInt64} {
		t.Run(fmt.Sprintf("kind=%d", kind), func(t *testing.T) {
			newWindow := func(valueCalls *int) *MutableIncrementalRangeWindow {
				window, err := NewMutableIncrementalRangeWindow(IncrementalRangeWindowDefinition{
					Kind:           kind,
					OutputColumn:   "range_value",
					FramePreceding: 2,
					PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey: func(row Row) (interface{}, error) {
						*valueCalls = *valueCalls + 1
						return row["value"], nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return window
			}
			rows := []Row{
				{"id": "a", "partition": "p", "order": int64(1), "value": int64(1)},
				{"id": "b", "partition": "p", "order": int64(2), "value": int64(1)},
				{"id": "c", "partition": "p", "order": int64(3), "value": int64(2)},
				{"id": "d", "partition": "p", "order": int64(4), "value": int64(3)},
			}
			inserts := make([]IncrementalRangeWindowMutation, len(rows))
			for index, row := range rows {
				inserts[index] = IncrementalRangeWindowMutation{Operation: IncrementalRangeWindowInsert, Row: row}
			}
			updates := []IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowUpdate, Key: "b", Row: Row{"id": "b", "partition": "p", "order": int64(2), "value": int64(3)}},
				{Operation: IncrementalRangeWindowUpdate, Key: "d", Row: Row{"id": "d", "partition": "p", "order": int64(4), "value": int64(2)}},
			}

			batchedValueCalls := 0
			batched := newWindow(&batchedValueCalls)
			if _, err := batched.Apply(inserts); err != nil {
				t.Fatal(err)
			}
			batchedValueCalls = 0
			if _, err := batched.Apply(updates); err != nil {
				t.Fatal(err)
			}
			if batchedValueCalls != len(updates) {
				t.Fatalf("stable batch value calls = %d, want %d", batchedValueCalls, len(updates))
			}

			sequentialValueCalls := 0
			sequential := newWindow(&sequentialValueCalls)
			if _, err := sequential.Apply(inserts); err != nil {
				t.Fatal(err)
			}
			for _, update := range updates {
				if _, err := sequential.Apply([]IncrementalRangeWindowMutation{update}); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(batched.outputs, sequential.outputs) {
				t.Fatalf("batched outputs = %#v, sequential outputs = %#v", batched.outputs, sequential.outputs)
			}
		})
	}
}

func TestMutableIncrementalRangeWindowBatchedStableDistinctAndAvgMatchesPeersDescendingAndNulls(t *testing.T) {
	for _, kind := range []IncrementalRangeWindowKind{IncrementalRangeWindowCountDistinctInt64, IncrementalRangeWindowAvgInt64} {
		for _, descending := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind=%d/descending=%t", kind, descending), func(t *testing.T) {
				newWindow := func() *MutableIncrementalRangeWindow {
					window, err := NewMutableIncrementalRangeWindow(IncrementalRangeWindowDefinition{
						Kind:           kind,
						OutputColumn:   "range_value",
						FramePreceding: 2,
						Descending:     descending,
						PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
						OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
						RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
						ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
					})
					if err != nil {
						t.Fatal(err)
					}
					return window
				}
				rows := []Row{
					{"id": "a", "partition": "p", "order": int64(4), "value": nil},
					{"id": "b", "partition": "p", "order": int64(4), "value": int64(1)},
					{"id": "c", "partition": "p", "order": int64(3), "value": int64(1)},
					{"id": "d", "partition": "p", "order": int64(2), "value": int64(2)},
					{"id": "e", "partition": "q", "order": int64(4), "value": int64(9)},
				}
				inserts := make([]IncrementalRangeWindowMutation, len(rows))
				for index, row := range rows {
					inserts[index] = IncrementalRangeWindowMutation{Operation: IncrementalRangeWindowInsert, Row: row}
				}
				updates := []IncrementalRangeWindowMutation{
					{Operation: IncrementalRangeWindowUpdate, Key: "a", Row: Row{"id": "a", "partition": "p", "order": int64(4), "value": int64(1)}},
					{Operation: IncrementalRangeWindowUpdate, Key: "d", Row: Row{"id": "d", "partition": "p", "order": int64(2), "value": nil}},
				}

				batched := newWindow()
				if _, err := batched.Apply(inserts); err != nil {
					t.Fatal(err)
				}
				if _, err := batched.Apply(updates); err != nil {
					t.Fatal(err)
				}

				sequential := newWindow()
				if _, err := sequential.Apply(inserts); err != nil {
					t.Fatal(err)
				}
				for _, update := range updates {
					if _, err := sequential.Apply([]IncrementalRangeWindowMutation{update}); err != nil {
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

func TestMutableIncrementalRangeWindowBatchedStableDistinctAndAvgInvalidIsAtomic(t *testing.T) {
	for _, kind := range []IncrementalRangeWindowKind{IncrementalRangeWindowCountDistinctInt64, IncrementalRangeWindowAvgInt64} {
		t.Run(fmt.Sprintf("kind=%d", kind), func(t *testing.T) {
			window, err := NewMutableIncrementalRangeWindow(IncrementalRangeWindowDefinition{
				Kind:           kind,
				OutputColumn:   "range_value",
				FramePreceding: 2,
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
				ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := window.Apply([]IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("a", 1, 1)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("b", 2, 2)},
				{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("c", 3, 3)},
			}); err != nil {
				t.Fatal(err)
			}
			beforeOutputs := make(map[string]Row, len(window.outputs))
			for key, row := range window.outputs {
				beforeOutputs[key] = cloneIncrementalRangeWindowRow(row)
			}
			beforeRows := make(map[string]mutableIncrementalRangeWindowEntry, len(window.rows))
			for key, entry := range window.rows {
				entry.row = cloneIncrementalRangeWindowRow(entry.row)
				beforeRows[key] = entry
			}
			_, err = window.Apply([]IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowUpdate, Key: "a", Row: mutableRangeRow("a", 1, 4)},
				{Operation: IncrementalRangeWindowUpdate, Key: "b", Row: Row{"id": "b", "order": int64(2), "value": "invalid"}},
			})
			wantErr := ErrIncrementalRangeWindowDistinctValueInvalid
			if kind == IncrementalRangeWindowAvgInt64 {
				wantErr = ErrIncrementalRangeWindowAvgValueInvalid
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("invalid stable batch error = %v, want %v", err, wantErr)
			}
			if !reflect.DeepEqual(window.outputs, beforeOutputs) || !reflect.DeepEqual(window.rows, beforeRows) {
				t.Fatalf("invalid stable batch changed state: outputs=%#v rows=%#v", window.outputs, window.rows)
			}
		})
	}
}

func TestMutableIncrementalRangeWindowIsAtomicAndSupportsDescending(t *testing.T) {
	window, err := NewMutableIncrementalRangeWindow(IncrementalRangeWindowDefinition{
		Kind:           IncrementalRangeWindowCount,
		OutputColumn:   "range_count",
		FramePreceding: 1,
		Descending:     true,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]IncrementalRangeWindowMutation{
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("a", 3, 0)},
		{Operation: IncrementalRangeWindowInsert, Row: mutableRangeRow("b", 2, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	invalid, err := window.Apply([]IncrementalRangeWindowMutation{
		{Operation: IncrementalRangeWindowUpdate, Key: "a", Row: mutableRangeRow("a", 3, 0)},
		{Operation: IncrementalRangeWindowDelete, Key: "missing"},
	})
	if !errors.Is(err, ErrMutableIncrementalRangeWindowMissingKey) || invalid != nil {
		t.Fatalf("invalid batch result = %#v, error = %v", invalid, err)
	}
	changes, err := window.Apply([]IncrementalRangeWindowMutation{{
		Operation: IncrementalRangeWindowInsert,
		Row:       mutableRangeRow("c", 1, 0),
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertMutableRangeValue(t, changes, "c", int64(2))
}

func TestMutableIncrementalRangeWindowSupportsAllAggregateKinds(t *testing.T) {
	cases := []struct {
		name string
		kind IncrementalRangeWindowKind
		want interface{}
	}{
		{name: "count", kind: IncrementalRangeWindowCount, want: int64(3)},
		{name: "sum", kind: IncrementalRangeWindowSumInt64, want: int64(8)},
		{name: "min", kind: IncrementalRangeWindowMinInt64, want: int64(2)},
		{name: "max", kind: IncrementalRangeWindowMaxInt64, want: int64(4)},
		{name: "distinct", kind: IncrementalRangeWindowCountDistinctInt64, want: int64(2)},
		{name: "avg", kind: IncrementalRangeWindowAvgInt64, want: float64(8) / 3},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			definition := IncrementalRangeWindowDefinition{
				Kind:           testCase.kind,
				OutputColumn:   "result",
				FramePreceding: 2,
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
			}
			if testCase.kind != IncrementalRangeWindowCount {
				definition.ValueKey = func(row Row) (interface{}, error) { return row["value"], nil }
			}
			window, err := NewMutableIncrementalRangeWindow(definition)
			if err != nil {
				t.Fatal(err)
			}
			rows := []IncrementalRangeWindowMutation{
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "a", "order": int64(1), "value": int64(2)}},
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "b", "order": int64(2), "value": int64(2)}},
				{Operation: IncrementalRangeWindowInsert, Row: Row{"id": "c", "order": int64(3), "value": int64(4)}},
			}
			changes, err := window.Apply(rows)
			if err != nil {
				t.Fatal(err)
			}
			if !hasMutableRangeOutput(changes, "c", 1, "result", testCase.want) {
				t.Fatalf("initial output = %#v, want c=%v", changes, testCase.want)
			}

			updated, err := window.Apply([]IncrementalRangeWindowMutation{{
				Operation: IncrementalRangeWindowUpdate,
				Key:       "c",
				Row:       Row{"id": "c", "order": int64(3), "value": int64(6)},
			}})
			if err != nil {
				t.Fatal(err)
			}
			wantUpdated := testCase.want
			switch testCase.kind {
			case IncrementalRangeWindowSumInt64:
				wantUpdated = int64(10)
			case IncrementalRangeWindowMaxInt64:
				wantUpdated = int64(6)
			case IncrementalRangeWindowCountDistinctInt64:
				wantUpdated = int64(2)
			case IncrementalRangeWindowAvgInt64:
				wantUpdated = float64(10) / 3
			}
			if !hasMutableRangeOutput(updated, "c", 1, "result", wantUpdated) {
				t.Fatalf("updated output = %#v, want c=%v", updated, wantUpdated)
			}
		})
	}
}

func mutableRangeRow(key string, order, value int64) Row {
	return Row{"id": key, "partition": "p", "order": order, "value": value}
}

func assertMutableRangeValue(t *testing.T, changes []DifferentialRow, key string, want interface{}) {
	t.Helper()
	if !hasMutableRangeChange(changes, key, 1, want) {
		t.Fatalf("missing positive range value for %q = %v: %#v", key, want, changes)
	}
}

func assertMutableRangePair(t *testing.T, changes []DifferentialRow, key string, oldValue, newValue interface{}) {
	t.Helper()
	if !hasMutableRangeChange(changes, key, -1, oldValue) || !hasMutableRangeChange(changes, key, 1, newValue) {
		t.Fatalf("range changes for %q = %#v, want %v -> %v", key, changes, oldValue, newValue)
	}
}

func hasMutableRangeChange(changes []DifferentialRow, key string, diff int64, value interface{}) bool {
	for _, change := range changes {
		if change.Key == key && change.Diff == diff && (reflect.DeepEqual(change.Row["range_sum"], value) || reflect.DeepEqual(change.Row["range_count"], value)) {
			return true
		}
	}
	return false
}

func hasMutableRangeOutput(changes []DifferentialRow, key string, diff int64, column string, value interface{}) bool {
	for _, change := range changes {
		if change.Key == key && change.Diff == diff && reflect.DeepEqual(change.Row[column], value) {
			return true
		}
	}
	return false
}
