package hatSql

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
)

func m065qRangeBoundaryDefinition(kind IncrementalRangeBoundaryWindowKind, preceding int64, descending bool) IncrementalRangeBoundaryWindowDefinition {
	return IncrementalRangeBoundaryWindowDefinition{
		Kind:           kind,
		OutputColumn:   "window_value",
		FramePreceding: preceding,
		Descending:     descending,
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

func assertM065qRangeBoundaryValues(t *testing.T, current map[string]Row, want map[string]interface{}) {
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

func TestM065qIncrementalRangeBoundaryIsPeerAware(t *testing.T) {
	rows := []Row{
		{"id": "a", "order": int64(1), "value": "a"},
		{"id": "b", "order": int64(1), "value": "b"},
		{"id": "c", "order": int64(2), "value": "c"},
		{"id": "d", "order": int64(4), "value": "d"},
	}
	for _, testCase := range []struct {
		name string
		kind IncrementalRangeBoundaryWindowKind
		want map[string]interface{}
	}{
		{name: "first", kind: IncrementalRangeFirstValue, want: map[string]interface{}{"a": "a", "b": "a", "c": "a", "d": "d"}},
		{name: "last", kind: IncrementalRangeLastValue, want: map[string]interface{}{"a": "b", "b": "b", "c": "c", "d": "d"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			window, err := NewIncrementalRangeBoundaryWindow(m065qRangeBoundaryDefinition(testCase.kind, 1, false))
			if err != nil {
				t.Fatalf("new window: %v", err)
			}
			updates, err := window.Append(rows)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			current := make(map[string]Row)
			applyM065mRangeUpdates(t, current, updates)
			assertM065qRangeBoundaryValues(t, current, testCase.want)
		})
	}
}

func TestM065qIncrementalRangeBoundaryHandlesNullAndDescending(t *testing.T) {
	first, err := NewIncrementalRangeBoundaryWindow(m065qRangeBoundaryDefinition(IncrementalRangeFirstValue, 1, false))
	if err != nil {
		t.Fatalf("new first window: %v", err)
	}
	last, err := NewIncrementalRangeBoundaryWindow(m065qRangeBoundaryDefinition(IncrementalRangeLastValue, 1, false))
	if err != nil {
		t.Fatalf("new last window: %v", err)
	}
	rows := []Row{
		{"id": "a", "order": int64(1), "value": nil},
		{"id": "b", "order": int64(1), "value": "b"},
		{"id": "c", "order": int64(2), "value": nil},
	}
	firstUpdates, err := first.Append(rows)
	if err != nil {
		t.Fatalf("append first NULL rows: %v", err)
	}
	lastUpdates, err := last.Append(rows)
	if err != nil {
		t.Fatalf("append last NULL rows: %v", err)
	}
	firstCurrent := make(map[string]Row)
	lastCurrent := make(map[string]Row)
	applyM065mRangeUpdates(t, firstCurrent, firstUpdates)
	applyM065mRangeUpdates(t, lastCurrent, lastUpdates)
	assertM065qRangeBoundaryValues(t, firstCurrent, map[string]interface{}{"a": nil, "b": nil, "c": nil})
	assertM065qRangeBoundaryValues(t, lastCurrent, map[string]interface{}{"a": "b", "b": "b", "c": nil})

	descendingFirst, err := NewIncrementalRangeBoundaryWindow(m065qRangeBoundaryDefinition(IncrementalRangeFirstValue, 1, true))
	if err != nil {
		t.Fatalf("new descending first window: %v", err)
	}
	descendingLast, err := NewIncrementalRangeBoundaryWindow(m065qRangeBoundaryDefinition(IncrementalRangeLastValue, 1, true))
	if err != nil {
		t.Fatalf("new descending last window: %v", err)
	}
	rows = []Row{
		{"id": "d", "order": int64(5), "value": "d"},
		{"id": "e", "order": int64(4), "value": "e"},
		{"id": "f", "order": int64(4), "value": "f"},
		{"id": "g", "order": int64(2), "value": "g"},
	}
	firstUpdates, err = descendingFirst.Append(rows)
	if err != nil {
		t.Fatalf("append descending first rows: %v", err)
	}
	lastUpdates, err = descendingLast.Append(rows)
	if err != nil {
		t.Fatalf("append descending last rows: %v", err)
	}
	firstCurrent = make(map[string]Row)
	lastCurrent = make(map[string]Row)
	applyM065mRangeUpdates(t, firstCurrent, firstUpdates)
	applyM065mRangeUpdates(t, lastCurrent, lastUpdates)
	assertM065qRangeBoundaryValues(t, firstCurrent, map[string]interface{}{"d": "d", "e": "d", "f": "d", "g": "g"})
	assertM065qRangeBoundaryValues(t, lastCurrent, map[string]interface{}{"d": "d", "e": "f", "f": "f", "g": "g"})
}

func TestM065qIncrementalRangeBoundaryValidatesAtomically(t *testing.T) {
	definition := m065qRangeBoundaryDefinition(IncrementalRangeFirstValue, 2, false)
	definition.ValueKey = func(row Row) (interface{}, error) {
		if row["id"] == "bad" {
			return nil, errors.New("bad value")
		}
		return row["value"], nil
	}
	window, err := NewIncrementalRangeBoundaryWindow(definition)
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": "a"}}); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	if _, err := window.Append([]Row{{"id": "bad", "order": int64(2), "value": "bad"}}); err == nil {
		t.Fatal("bad callback unexpectedly succeeded")
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": "b"}})
	if err != nil {
		t.Fatalf("retry callback append: %v", err)
	}
	if got := updates[0].Row["window_value"]; got != "a" {
		t.Fatalf("retry first value = %#v, want a", got)
	}
	if _, err := window.Append([]Row{{"id": "c", "order": int64(1), "value": "c"}}); !errors.Is(err, ErrIncrementalRangeBoundaryWindowOutOfOrder) {
		t.Fatalf("out-of-order error = %v, want %v", err, ErrIncrementalRangeBoundaryWindowOutOfOrder)
	}
}

func TestM065qIncrementalRangeBoundaryMatchesReference(t *testing.T) {
	for _, testCase := range []struct {
		name string
		kind IncrementalRangeBoundaryWindowKind
	}{
		{name: "first", kind: IncrementalRangeFirstValue},
		{name: "last", kind: IncrementalRangeLastValue},
	} {
		for _, descending := range []bool{false, true} {
			t.Run(testCase.name+"/descending="+strconv.FormatBool(descending), func(t *testing.T) {
				const preceding int64 = 2
				definition := m065qRangeBoundaryDefinition(testCase.kind, preceding, descending)
				definition.PartitionKey = func(row Row) (string, error) {
					return row["partition"].(string), nil
				}
				window, err := NewIncrementalRangeBoundaryWindow(definition)
				if err != nil {
					t.Fatalf("new window: %v", err)
				}
				rows := make([]Row, 96)
				current := make(map[string]Row)
				for index := range rows {
					order := int64(index / 3)
					if descending {
						order = 200 - order
					}
					var value interface{}
					if index%11 != 0 {
						value = "value-" + strconv.Itoa(index%7)
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
						var expected interface{}
						for prior := 0; prior <= index; prior++ {
							if rows[prior]["partition"].(string) != candidatePartition {
								continue
							}
							priorOrder := rows[prior]["order"].(int64)
							inFrame := priorOrder >= candidateOrder-preceding && priorOrder <= candidateOrder
							if descending {
								inFrame = priorOrder >= candidateOrder && priorOrder <= candidateOrder+preceding
							}
							if !inFrame {
								continue
							}
							expected = rows[prior]["value"]
							if testCase.kind == IncrementalRangeFirstValue {
								break
							}
						}
						want[rows[candidate]["id"].(string)] = expected
					}
					assertM065qRangeBoundaryValues(t, current, want)
				}
			})
		}
	}
}
