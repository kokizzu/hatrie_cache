package hatSql

import (
	"errors"
	"math"
	"strconv"
	"testing"
)

func m065rRangeNthValueDefinition(position int, preceding int64, descending bool) IncrementalRangeNthValueWindowDefinition {
	return IncrementalRangeNthValueWindowDefinition{
		Position:       position,
		OutputColumn:   "window_value",
		FramePreceding: preceding,
		Descending:     descending,
		PartitionKey: func(row Row) (string, error) {
			if value, ok := row["partition"].(string); ok {
				return value, nil
			}
			return "", nil
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
	}
}

func TestM065rIncrementalRangeNthValueIsPeerAware(t *testing.T) {
	window, err := NewIncrementalRangeNthValueWindow(m065rRangeNthValueDefinition(2, 1, false))
	if err != nil {
		t.Fatal(err)
	}
	rows := []Row{
		{"id": "a", "order": int64(1), "value": "a"},
		{"id": "b", "order": int64(1), "value": "b"},
		{"id": "c", "order": int64(2), "value": "c"},
		{"id": "d", "order": int64(4), "value": "d"},
	}
	current := make(map[string]Row)
	for _, row := range rows {
		updates, err := window.Append([]Row{row})
		if err != nil {
			t.Fatal(err)
		}
		applyM065mRangeUpdates(t, current, updates)
	}
	assertM065mRangeValues(t, current, map[string]interface{}{
		"a": "b",
		"b": "b",
		"c": "b",
		"d": nil,
	})
}

func TestM065rIncrementalRangeNthValueHandlesNullDescendingAndMissingPosition(t *testing.T) {
	t.Run("null-aware peers", func(t *testing.T) {
		window, err := NewIncrementalRangeNthValueWindow(m065rRangeNthValueDefinition(2, 1, false))
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{"id": "a", "order": int64(1), "value": nil},
			{"id": "b", "order": int64(1), "value": "b"},
			{"id": "c", "order": int64(2), "value": "c"},
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatal(err)
		}
		current := make(map[string]Row)
		applyM065mRangeUpdates(t, current, updates)
		assertM065mRangeValues(t, current, map[string]interface{}{"a": "b", "b": "b", "c": "b"})
	})

	t.Run("descending", func(t *testing.T) {
		window, err := NewIncrementalRangeNthValueWindow(m065rRangeNthValueDefinition(2, 1, true))
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{"id": "d", "order": int64(5), "value": "d"},
			{"id": "e", "order": int64(4), "value": "e"},
			{"id": "f", "order": int64(4), "value": "f"},
			{"id": "g", "order": int64(2), "value": "g"},
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatal(err)
		}
		current := make(map[string]Row)
		applyM065mRangeUpdates(t, current, updates)
		assertM065mRangeValues(t, current, map[string]interface{}{"d": nil, "e": "e", "f": "e", "g": nil})
	})
}

func TestM065rIncrementalRangeNthValueValidatesAtomically(t *testing.T) {
	orderError := errors.New("order callback failed")
	window, err := NewIncrementalRangeNthValueWindow(IncrementalRangeNthValueWindowDefinition{
		Position:     1,
		OutputColumn: "window_value",
		OrderKey: func(row Row) (interface{}, error) {
			if row["id"] == "bad" {
				return nil, orderError
			}
			return row["order"], nil
		},
		RowKey:   func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey: func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2), "value": "b"},
		{"id": "bad", "order": int64(3), "value": "bad"},
	}); !errors.Is(err, orderError) {
		t.Fatalf("callback error = %v, want callback error", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": "b"}})
	if err != nil {
		t.Fatal(err)
	}
	current := make(map[string]Row)
	applyM065mRangeUpdates(t, current, updates)
	assertM065mRangeValues(t, current, map[string]interface{}{"b": "b"})
	if _, err := window.Append([]Row{{"id": "c", "order": int64(1), "value": "c"}}); !errors.Is(err, ErrIncrementalRangeNthValueWindowOutOfOrder) {
		t.Fatalf("out-of-order error = %v, want out-of-order error", err)
	}
}

func TestM065rIncrementalRangeNthValueValidatesDefinitionsAndSaturatesBounds(t *testing.T) {
	base := m065rRangeNthValueDefinition(1, 0, false)
	invalid := base
	invalid.Position = 0
	if _, err := NewIncrementalRangeNthValueWindow(invalid); !errors.Is(err, ErrIncrementalRangeNthValueWindowInvalidPosition) {
		t.Fatalf("position error = %v", err)
	}
	invalid = base
	invalid.FramePreceding = -1
	if _, err := NewIncrementalRangeNthValueWindow(invalid); !errors.Is(err, ErrIncrementalRangeNthValueWindowNegativeFrame) {
		t.Fatalf("frame error = %v", err)
	}
	invalid = base
	invalid.OrderKey = func(Row) (interface{}, error) { return int(1), nil }
	window, err := NewIncrementalRangeNthValueWindow(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "bad-order", "order": int64(1), "value": "x"}}); !errors.Is(err, ErrIncrementalRangeNthValueWindowOrderInvalid) {
		t.Fatalf("order type error = %v", err)
	}
	if _, err := (*IncrementalRangeNthValueWindow)(nil).Append(nil); !errors.Is(err, ErrIncrementalRangeNthValueWindowNil) {
		t.Fatalf("nil receiver error = %v", err)
	}

	for _, descending := range []bool{false, true} {
		window, err := NewIncrementalRangeNthValueWindow(m065rRangeNthValueDefinition(2, math.MaxInt64, descending))
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{"id": "first", "order": int64(math.MinInt64), "value": "first"},
			{"id": "second", "order": int64(math.MinInt64), "value": "second"},
		}
		if descending {
			rows[0]["order"] = int64(math.MaxInt64)
			rows[1]["order"] = int64(math.MaxInt64)
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatalf("descending=%t append: %v", descending, err)
		}
		current := make(map[string]Row)
		applyM065mRangeUpdates(t, current, updates)
		assertM065mRangeValues(t, current, map[string]interface{}{"first": "second", "second": "second"})
	}
}

func TestM065rIncrementalRangeNthValueMatchesReference(t *testing.T) {
	for _, position := range []int{1, 2, 3} {
		for _, descending := range []bool{false, true} {
			t.Run(strconv.Itoa(position)+"/"+strconv.FormatBool(descending), func(t *testing.T) {
				window, err := NewIncrementalRangeNthValueWindow(m065rRangeNthValueDefinition(position, 2, descending))
				if err != nil {
					t.Fatal(err)
				}
				rows := make([]Row, 0, 96)
				current := make(map[string]Row)
				for index := 0; index < 96; index++ {
					partition := "p" + strconv.Itoa(index%3)
					order := int64(index / 3)
					if descending {
						order = 1000 - order
					}
					value := interface{}("v-" + strconv.Itoa(index))
					if index%7 == 0 {
						value = nil
					}
					row := Row{
						"id":        "row-" + strconv.Itoa(index),
						"partition": partition,
						"order":     order,
						"value":     value,
					}
					rows = append(rows, row)
					updates, err := window.Append([]Row{row})
					if err != nil {
						t.Fatalf("append %d: %v", index, err)
					}
					applyM065mRangeUpdates(t, current, updates)
					want := make(map[string]interface{}, len(rows))
					for candidate := range rows {
						want[rows[candidate]["id"].(string)] = m065rRangeNthValueReference(rows, candidate, position, 2, descending)
					}
					assertM065mRangeValues(t, current, want)
				}
			})
		}
	}
}

func m065rRangeNthValueReference(rows []Row, index, position int, preceding int64, descending bool) interface{} {
	currentPartition := rows[index]["partition"].(string)
	currentOrder := rows[index]["order"].(int64)
	frame := make([]interface{}, 0, len(rows))
	for candidate := 0; candidate <= index; candidate++ {
		if rows[candidate]["partition"].(string) != currentPartition {
			continue
		}
		candidateOrder := rows[candidate]["order"].(int64)
		included := candidateOrder >= currentOrder-preceding && candidateOrder <= currentOrder
		if descending {
			included = candidateOrder >= currentOrder && candidateOrder <= currentOrder+preceding
		}
		if included {
			frame = append(frame, rows[candidate]["value"])
		}
	}
	if len(frame) < position {
		return nil
	}
	return frame[position-1]
}
