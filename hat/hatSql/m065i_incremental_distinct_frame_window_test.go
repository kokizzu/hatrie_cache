package hatSql

import (
	"errors"
	"math/rand"
	"strconv"
	"testing"
)

func TestIncrementalFrameWindowCountDistinctMaintainsBoundedNullAwareFrames(t *testing.T) {
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameCountDistinctInt64,
		OutputColumn:   "distinct_count",
		FramePreceding: 2,
		PartitionKey: func(row Row) (string, error) {
			return row["partition"].(string), nil
		},
		OrderKey: func(row Row) (interface{}, error) {
			return row["order"], nil
		},
		RowKey: func(row Row) (string, error) {
			return row["id"].(string), nil
		},
		ValueKey: func(row Row) (interface{}, error) {
			return row["value"], nil
		},
	})
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	rows := []Row{
		{"id": "a", "partition": "one", "order": int64(1), "value": int64(10)},
		{"id": "b", "partition": "one", "order": int64(2), "value": int64(10)},
		{"id": "c", "partition": "one", "order": int64(3), "value": nil},
		{"id": "d", "partition": "one", "order": int64(4), "value": int64(20)},
		{"id": "e", "partition": "one", "order": int64(5), "value": int64(10)},
		{"id": "f", "partition": "two", "order": int64(1), "value": int64(10)},
	}

	updates, err := window.Append(rows)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	want := []int64{1, 1, 1, 2, 2, 1}
	if len(updates) != len(want) {
		t.Fatalf("updates len = %d, want %d", len(updates), len(want))
	}
	for index, update := range updates {
		if got := update.Row["distinct_count"]; got != want[index] {
			t.Fatalf("row %d distinct count = %#v, want %d", index, got, want[index])
		}
		if _, exists := rows[index]["distinct_count"]; exists {
			t.Fatalf("input row %d was mutated: %#v", index, rows[index])
		}
	}

	zero, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameCountDistinctInt64,
		OutputColumn:   "distinct_count",
		FramePreceding: 0,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatalf("new zero-frame window: %v", err)
	}
	zeroUpdates, err := zero.Append([]Row{
		{"id": "z1", "order": int64(1), "value": nil},
		{"id": "z2", "order": int64(2), "value": int64(4)},
		{"id": "z3", "order": int64(3), "value": int64(4)},
	})
	if err != nil {
		t.Fatalf("append zero-frame rows: %v", err)
	}
	for index, wantValue := range []int64{0, 1, 1} {
		if got := zeroUpdates[index].Row["distinct_count"]; got != wantValue {
			t.Fatalf("zero-frame row %d distinct count = %#v, want %d", index, got, wantValue)
		}
	}
}

func TestIncrementalFrameWindowCountDistinctValidatesAtomically(t *testing.T) {
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameCountDistinctInt64,
		OutputColumn:   "distinct_count",
		FramePreceding: 2,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": int64(7)}}); err != nil {
		t.Fatalf("append seed: %v", err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2), "value": int64(9)},
		{"id": "c", "order": int64(3), "value": "invalid"},
	}); !errors.Is(err, ErrIncrementalFrameWindowDistinctValueInvalid) {
		t.Fatalf("invalid append error = %v, want %v", err, ErrIncrementalFrameWindowDistinctValueInvalid)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(9)}})
	if err != nil {
		t.Fatalf("retry append: %v", err)
	}
	if got := updates[0].Row["distinct_count"]; got != int64(2) {
		t.Fatalf("retry distinct count = %#v, want 2", got)
	}
}

func TestIncrementalFrameWindowCountDistinctMatchesReferenceRandomized(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(strconv.FormatBool(descending), func(t *testing.T) {
			window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
				Kind:           IncrementalWindowFrameCountDistinctInt64,
				OutputColumn:   "distinct_count",
				FramePreceding: 4,
				Descending:     descending,
				PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
				ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
			})
			if err != nil {
				t.Fatalf("new window: %v", err)
			}

			random := rand.New(rand.NewSource(65))
			rows := make([]Row, 300)
			for index := range rows {
				order := int64(index)
				if descending {
					order = int64(len(rows) - index)
				}
				var value interface{}
				if random.Intn(5) != 0 {
					value = int64(random.Intn(17) - 8)
				}
				rows[index] = Row{
					"id":        "row-" + strconv.Itoa(index),
					"partition": "partition-" + strconv.Itoa(index%3),
					"order":     order,
					"value":     value,
				}
			}

			updates, err := window.Append(rows)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			history := make(map[string][]interface{})
			for index, row := range rows {
				partition := row["partition"].(string)
				history[partition] = append(history[partition], row["value"])
				frame := history[partition]
				if len(frame) > 5 {
					frame = frame[len(frame)-5:]
				}
				distinct := make(map[int64]struct{})
				for _, value := range frame {
					if intValue, ok := value.(int64); ok {
						distinct[intValue] = struct{}{}
					}
				}
				want := int64(len(distinct))
				if got := updates[index].Row["distinct_count"]; got != want {
					t.Fatalf("row %d distinct count = %#v, want %d", index, got, want)
				}
			}
		})
	}
}
