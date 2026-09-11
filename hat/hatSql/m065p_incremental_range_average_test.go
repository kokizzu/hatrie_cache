package hatSql

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func assertM065pRangeValues(t *testing.T, current map[string]Row, want map[string]interface{}) {
	t.Helper()
	if len(current) != len(want) {
		t.Fatalf("current output keys = %d, want %d: %#v", len(current), len(want), current)
	}
	for key, value := range want {
		row, ok := current[key]
		if !ok {
			t.Fatalf("missing output key %q", key)
		}
		if !reflect.DeepEqual(row["window_value"], value) {
			t.Fatalf("output %q = %#v, want %#v", key, row["window_value"], value)
		}
	}
}

func TestM065pIncrementalRangeAverageIsPeerAware(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowAvgInt64, 1, false))
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	rows := []Row{
		{"id": "a", "order": int64(1), "value": int64(10)},
		{"id": "b", "order": int64(1), "value": nil},
		{"id": "c", "order": int64(2), "value": int64(5)},
		{"id": "d", "order": int64(4), "value": int64(7)},
	}
	current := make(map[string]Row)
	updates, err := window.Append(rows[:2])
	if err != nil {
		t.Fatalf("append peers: %v", err)
	}
	applyM065mRangeUpdates(t, current, updates)
	assertM065pRangeValues(t, current, map[string]interface{}{"a": float64(10), "b": float64(10)})

	updates, err = window.Append(rows[2:])
	if err != nil {
		t.Fatalf("append remaining rows: %v", err)
	}
	applyM065mRangeUpdates(t, current, updates)
	assertM065pRangeValues(t, current, map[string]interface{}{
		"a": float64(10),
		"b": float64(10),
		"c": float64(7.5),
		"d": float64(7),
	})
}

func TestM065pIncrementalRangeAverageHandlesDescendingAndAllNull(t *testing.T) {
	allNull, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowAvgInt64, 0, false))
	if err != nil {
		t.Fatalf("new all-null window: %v", err)
	}
	updates, err := allNull.Append([]Row{{"id": "a", "order": int64(1), "value": nil}})
	if err != nil {
		t.Fatalf("append all-null row: %v", err)
	}
	if got := updates[0].Row["window_value"]; got != nil {
		t.Fatalf("all-null average = %#v, want nil", got)
	}

	descending, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowAvgInt64, 1, true))
	if err != nil {
		t.Fatalf("new descending window: %v", err)
	}
	updates, err = descending.Append([]Row{
		{"id": "b", "order": int64(5), "value": int64(10)},
		{"id": "c", "order": int64(4), "value": nil},
		{"id": "d", "order": int64(4), "value": int64(2)},
		{"id": "e", "order": int64(2), "value": int64(7)},
	})
	if err != nil {
		t.Fatalf("append descending: %v", err)
	}
	current := make(map[string]Row)
	applyM065mRangeUpdates(t, current, updates)
	assertM065pRangeValues(t, current, map[string]interface{}{
		"b": float64(10),
		"c": float64(6),
		"d": float64(6),
		"e": float64(7),
	})
}

func TestM065pIncrementalRangeAverageValidatesOverflowAtomically(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowAvgInt64, 2, false))
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": int64(math.MaxInt64)}}); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(1)}}); !errors.Is(err, ErrIncrementalRangeWindowSumOverflow) {
		t.Fatalf("overflow error = %v, want %v", err, ErrIncrementalRangeWindowSumOverflow)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(-1)}})
	if err != nil {
		t.Fatalf("retry append: %v", err)
	}
	if got := updates[0].Row["window_value"]; got != float64(math.MaxInt64-1)/2 {
		t.Fatalf("retry average = %#v, want %#v", got, float64(math.MaxInt64-1)/2)
	}
}

func TestM065pIncrementalRangeAverageMatchesReference(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		descending bool
	}{
		{name: "ascending"},
		{name: "descending", descending: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			const preceding int64 = 2
			definition := m065mRangeDefinition(IncrementalRangeWindowAvgInt64, preceding, testCase.descending)
			definition.PartitionKey = func(row Row) (string, error) {
				return row["partition"].(string), nil
			}
			window, err := NewIncrementalRangeWindow(definition)
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
				applyM065mRangeUpdates(t, current, updates)

				want := make(map[string]interface{}, index+1)
				for candidate := 0; candidate <= index; candidate++ {
					candidatePartition := rows[candidate]["partition"].(string)
					candidateOrder := rows[candidate]["order"].(int64)
					var sum int64
					var count int64
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
						value, ok := rows[prior]["value"].(int64)
						if !ok {
							continue
						}
						sum += value
						count++
					}
					var expected interface{}
					if count > 0 {
						expected = float64(sum) / float64(count)
					}
					want[rows[candidate]["id"].(string)] = expected
				}
				assertM065pRangeValues(t, current, want)
			}
		})
	}
}
