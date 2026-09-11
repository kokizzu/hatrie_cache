package hatSql

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
)

func m065oRangeDistinctDefinition(preceding int64, descending bool) IncrementalRangeWindowDefinition {
	return IncrementalRangeWindowDefinition{
		Kind:           IncrementalRangeWindowCountDistinctInt64,
		OutputColumn:   "distinct_count",
		FramePreceding: preceding,
		Descending:     descending,
		PartitionKey: func(row Row) (string, error) {
			return row["partition"].(string), nil
		},
		OrderKey: func(row Row) (interface{}, error) {
			return row["order"].(int64), nil
		},
		RowKey: func(row Row) (string, error) {
			return row["id"].(string), nil
		},
		ValueKey: func(row Row) (interface{}, error) {
			return row["value"], nil
		},
	}
}

func applyM065oRangeDistinctUpdates(t *testing.T, output map[string]Row, updates []DifferentialRow) {
	t.Helper()
	for _, update := range updates {
		switch update.Diff {
		case -1:
			previous, ok := output[update.Key]
			if !ok {
				t.Fatalf("retraction for missing key %q", update.Key)
			}
			if !reflect.DeepEqual(previous, update.Row) {
				t.Fatalf("retraction for %q = %#v, want %#v", update.Key, update.Row, previous)
			}
			delete(output, update.Key)
		case 1:
			if _, exists := output[update.Key]; exists {
				t.Fatalf("insertion for existing key %q: %#v", update.Key, update.Row)
			}
			output[update.Key] = update.Row
		default:
			t.Fatalf("update %q has diff %d", update.Key, update.Diff)
		}
	}
}

func assertM065oRangeDistinctValues(t *testing.T, output map[string]Row, want map[string]interface{}) {
	t.Helper()
	got := make(map[string]interface{}, len(output))
	for key, row := range output {
		got[key] = row["distinct_count"]
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("distinct outputs = %#v, want %#v", got, want)
	}
}

func TestM065oIncrementalRangeCountDistinctIsPeerAware(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065oRangeDistinctDefinition(1, false))
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	output := make(map[string]Row)
	appendRows := func(rows ...Row) {
		t.Helper()
		updates, appendErr := window.Append(rows)
		if appendErr != nil {
			t.Fatalf("append: %v", appendErr)
		}
		applyM065oRangeDistinctUpdates(t, output, updates)
	}

	appendRows(
		Row{"id": "a", "partition": "p", "order": int64(1), "value": int64(7)},
	)
	assertM065oRangeDistinctValues(t, output, map[string]interface{}{"a": int64(1)})

	appendRows(
		Row{"id": "b", "partition": "p", "order": int64(1), "value": int64(7)},
	)
	assertM065oRangeDistinctValues(t, output, map[string]interface{}{"a": int64(1), "b": int64(1)})

	appendRows(
		Row{"id": "c", "partition": "p", "order": int64(2), "value": int64(9)},
	)
	assertM065oRangeDistinctValues(t, output, map[string]interface{}{"a": int64(1), "b": int64(1), "c": int64(2)})

	appendRows(
		Row{"id": "d", "partition": "p", "order": int64(4), "value": int64(7)},
	)
	assertM065oRangeDistinctValues(t, output, map[string]interface{}{
		"a": int64(1),
		"b": int64(1),
		"c": int64(2),
		"d": int64(1),
	})
}

func TestM065oIncrementalRangeCountDistinctHandlesNullAndDescending(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065oRangeDistinctDefinition(1, false))
	if err != nil {
		t.Fatalf("new ascending window: %v", err)
	}
	updates, err := window.Append([]Row{
		{"id": "a", "partition": "p", "order": int64(1), "value": nil},
		{"id": "b", "partition": "p", "order": int64(2), "value": int64(4)},
		{"id": "c", "partition": "p", "order": int64(3), "value": nil},
	})
	if err != nil {
		t.Fatalf("append ascending: %v", err)
	}
	output := make(map[string]Row)
	applyM065oRangeDistinctUpdates(t, output, updates)
	assertM065oRangeDistinctValues(t, output, map[string]interface{}{
		"a": int64(0),
		"b": int64(1),
		"c": int64(1),
	})

	descending, err := NewIncrementalRangeWindow(m065oRangeDistinctDefinition(1, true))
	if err != nil {
		t.Fatalf("new descending window: %v", err)
	}
	updates, err = descending.Append([]Row{
		{"id": "d", "partition": "p", "order": int64(4), "value": int64(1)},
		{"id": "e", "partition": "p", "order": int64(4), "value": int64(2)},
		{"id": "f", "partition": "p", "order": int64(2), "value": int64(1)},
	})
	if err != nil {
		t.Fatalf("append descending: %v", err)
	}
	output = make(map[string]Row)
	applyM065oRangeDistinctUpdates(t, output, updates)
	assertM065oRangeDistinctValues(t, output, map[string]interface{}{
		"d": int64(2),
		"e": int64(2),
		"f": int64(1),
	})
}

func TestM065oIncrementalRangeCountDistinctValidatesAtomically(t *testing.T) {
	definition := m065oRangeDistinctDefinition(2, false)
	window, err := NewIncrementalRangeWindow(definition)
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	if _, err := window.Append([]Row{{"id": "a", "partition": "p", "order": int64(1), "value": int64(7)}}); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "partition": "p", "order": int64(2), "value": "wrong"},
	}); !errors.Is(err, ErrIncrementalRangeWindowDistinctValueInvalid) {
		t.Fatalf("invalid value error = %v, want %v", err, ErrIncrementalRangeWindowDistinctValueInvalid)
	}
	updates, err := window.Append([]Row{{"id": "b", "partition": "p", "order": int64(2), "value": int64(9)}})
	if err != nil {
		t.Fatalf("retry append: %v", err)
	}
	if got := updates[0].Row["distinct_count"]; got != int64(2) {
		t.Fatalf("retry distinct count = %#v, want 2", got)
	}

	if _, err := window.Append([]Row{{"id": "c", "partition": "p", "order": int64(1), "value": int64(11)}}); !errors.Is(err, ErrIncrementalRangeWindowOutOfOrder) {
		t.Fatalf("out-of-order error = %v, want %v", err, ErrIncrementalRangeWindowOutOfOrder)
	}
	updates, err = window.Append([]Row{{"id": "c", "partition": "p", "order": int64(3), "value": int64(11)}})
	if err != nil {
		t.Fatalf("retry after order error: %v", err)
	}
	if got := updates[0].Row["distinct_count"]; got != int64(3) {
		t.Fatalf("post-order-error distinct count = %#v, want 3", got)
	}
}

func TestM065oIncrementalRangeCountDistinctMatchesReference(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		descending bool
	}{
		{name: "ascending"},
		{name: "descending", descending: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			const preceding int64 = 2
			window, err := NewIncrementalRangeWindow(m065oRangeDistinctDefinition(preceding, testCase.descending))
			if err != nil {
				t.Fatalf("new window: %v", err)
			}
			rows := make([]Row, 96)
			current := make(map[string]Row)
			for index := range rows {
				order := int64(index / 3)
				if testCase.descending {
					order = 100 - order
				}
				var value interface{}
				if index%7 != 0 {
					value = int64(index%5 - 2)
				}
				rows[index] = Row{
					"id":        "row-" + strconv.Itoa(index),
					"partition": "p-" + strconv.Itoa(index%3),
					"order":     order,
					"value":     value,
				}
				updates, appendErr := window.Append([]Row{rows[index]})
				if appendErr != nil {
					t.Fatalf("append %d: %v", index, appendErr)
				}
				applyM065oRangeDistinctUpdates(t, current, updates)

				want := make(map[string]interface{}, index+1)
				for candidate := 0; candidate <= index; candidate++ {
					candidatePartition := rows[candidate]["partition"].(string)
					candidateOrder := rows[candidate]["order"].(int64)
					values := make(map[int64]struct{})
					for prior := 0; prior <= index; prior++ {
						if rows[prior]["partition"].(string) != candidatePartition {
							continue
						}
						priorOrder := rows[prior]["order"].(int64)
						inFrame := priorOrder >= candidateOrder-preceding && priorOrder <= candidateOrder
						if testCase.descending {
							inFrame = priorOrder >= candidateOrder && priorOrder <= candidateOrder+preceding
						}
						if !inFrame {
							continue
						}
						if value, ok := rows[prior]["value"].(int64); ok {
							values[value] = struct{}{}
						}
					}
					want[rows[candidate]["id"].(string)] = int64(len(values))
				}
				assertM065oRangeDistinctValues(t, current, want)
			}
		})
	}
}
