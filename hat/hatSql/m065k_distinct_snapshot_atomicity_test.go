package hatSql

import (
	"errors"
	"testing"
)

func TestIncrementalFrameWindowDistinctOutOfOrderBatchIsAtomic(t *testing.T) {
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
		{"id": "b", "order": int64(3), "value": int64(9)},
		{"id": "c", "order": int64(2), "value": int64(11)},
	}); !errors.Is(err, ErrIncrementalFrameWindowOutOfOrder) {
		t.Fatalf("out-of-order error = %v, want %v", err, ErrIncrementalFrameWindowOutOfOrder)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(9)}})
	if err != nil {
		t.Fatalf("retry append: %v", err)
	}
	if got := updates[0].Row["distinct_count"]; got != int64(2) {
		t.Fatalf("retry distinct count = %#v, want 2", got)
	}
}
