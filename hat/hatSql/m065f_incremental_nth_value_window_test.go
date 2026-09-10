package hatSql

import (
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func TestIncrementalNthValueWindowMaintainsFixedPositionPerPartition(t *testing.T) {
	window, err := NewIncrementalNthValueWindow(IncrementalNthValueWindowDefinition{
		Position:     2,
		OutputColumn: "second_value",
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := []Row{
		{"id": "a", "partition": "p1", "order": int64(1), "value": "A"},
		{"id": "x", "partition": "p2", "order": int64(1), "value": "X"},
		{"id": "b", "partition": "p1", "order": int64(2), "value": "B"},
		{"id": "c", "partition": "p1", "order": int64(3), "value": "C"},
		{"id": "y", "partition": "p2", "order": int64(2), "value": "Y"},
		{"id": "d", "partition": "p1", "order": int64(4), "value": "D"},
	}
	updates, err := window.Append(rows)
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalNthValueWindowValues(t, updates, "second_value", map[string]interface{}{
		"a": nil,
		"b": "B",
		"c": "B",
		"d": "B",
		"x": nil,
		"y": "Y",
	})
	if _, exists := rows[0]["second_value"]; exists {
		t.Fatal("Append mutated an input row")
	}

	nilValue, err := NewIncrementalNthValueWindow(IncrementalNthValueWindowDefinition{
		Position:     2,
		OutputColumn: "second_value",
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err = nilValue.Append([]Row{
		{"id": "a", "order": int64(1), "value": "A"},
		{"id": "b", "order": int64(2), "value": nil},
		{"id": "c", "order": int64(3), "value": "C"},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalNthValueWindowValues(t, updates, "second_value", map[string]interface{}{"a": nil, "b": nil, "c": nil})
}

func TestIncrementalNthValueWindowSupportsDescendingAndAtomicCallbacks(t *testing.T) {
	orderError := errors.New("order callback failed")
	window, err := NewIncrementalNthValueWindow(IncrementalNthValueWindowDefinition{
		Position:     2,
		OutputColumn: "second_value",
		Descending:   true,
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
	if _, err := window.Append([]Row{{"id": "a", "order": int64(3), "value": "A"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2), "value": "B"},
		{"id": "bad", "order": int64(1), "value": "bad"},
	}); !errors.Is(err, orderError) {
		t.Fatalf("callback error = %v, want callback error", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": "B"}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalNthValueWindowValues(t, updates, "second_value", map[string]interface{}{"b": "B"})
	if _, err := window.Append([]Row{{"id": "c", "order": int64(4), "value": "C"}}); !errors.Is(err, ErrIncrementalNthValueWindowOutOfOrder) {
		t.Fatalf("order error = %v, want out-of-order error", err)
	}
}

func TestIncrementalNthValueWindowRejectsInvalidDefinitionsAndRows(t *testing.T) {
	base := IncrementalNthValueWindowDefinition{
		Position:     1,
		OutputColumn: "first_value",
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	}
	invalid := base
	invalid.Position = 0
	if _, err := NewIncrementalNthValueWindow(invalid); !errors.Is(err, ErrIncrementalNthValueWindowInvalidPosition) {
		t.Fatalf("position error = %v", err)
	}
	invalid = base
	invalid.OutputColumn = " "
	if _, err := NewIncrementalNthValueWindow(invalid); !errors.Is(err, ErrIncrementalNthValueWindowOutputRequired) {
		t.Fatalf("output error = %v", err)
	}
	invalid = base
	invalid.ValueKey = nil
	if _, err := NewIncrementalNthValueWindow(invalid); !errors.Is(err, ErrIncrementalNthValueWindowValueRequired) {
		t.Fatalf("value error = %v", err)
	}
	invalid = base
	invalid.OrderKey = nil
	if _, err := NewIncrementalNthValueWindow(invalid); !errors.Is(err, ErrIncrementalNthValueWindowOrderRequired) {
		t.Fatalf("order error = %v", err)
	}
	invalid = base
	invalid.RowKey = nil
	if _, err := NewIncrementalNthValueWindow(invalid); !errors.Is(err, ErrIncrementalNthValueWindowRowKeyRequired) {
		t.Fatalf("row key error = %v", err)
	}

	window, err := NewIncrementalNthValueWindow(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": "A"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(2), "value": "A2"}}); !errors.Is(err, ErrIncrementalNthValueWindowDuplicateKey) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "first_value": "existing", "value": "B"}}); !errors.Is(err, ErrIncrementalNthValueWindowOutputConflict) {
		t.Fatalf("output conflict = %v", err)
	}
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": "B"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := (*IncrementalNthValueWindow)(nil).Append([]Row{{"id": "nil"}}); !errors.Is(err, ErrIncrementalNthValueWindowNil) {
		t.Fatalf("nil receiver error = %v", err)
	}
}

func TestIncrementalNthValueWindowMatchesReferenceRandomized(t *testing.T) {
	for _, position := range []int{1, 2, 5} {
		for _, descending := range []bool{false, true} {
			t.Run(strconv.Itoa(position)+"/"+strconv.FormatBool(descending), func(t *testing.T) {
				window, err := NewIncrementalNthValueWindow(IncrementalNthValueWindowDefinition{
					Position:     position,
					OutputColumn: "nth_value",
					Descending:   descending,
					PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
				})
				if err != nil {
					t.Fatal(err)
				}
				random := rand.New(rand.NewSource(65065))
				nextOrder := map[string]int64{"p0": 0, "p1": 0, "p2": 0}
				values := make(map[string][]interface{})
				for index := 0; index < 240; index++ {
					partition := "p" + strconv.Itoa(random.Intn(3))
					order := nextOrder[partition]
					if descending {
						order = 1000 - order
					}
					nextOrder[partition]++
					value := interface{}("v-" + strconv.Itoa(index))
					if random.Intn(5) == 0 {
						value = nil
					}
					row := Row{
						"id":        "row-" + strconv.Itoa(index),
						"partition": partition,
						"order":     order,
						"value":     value,
					}
					updates, err := window.Append([]Row{row})
					if err != nil {
						t.Fatalf("append %d: %v", index, err)
					}
					values[partition] = append(values[partition], value)
					want := interface{}(nil)
					if len(values[partition]) >= position {
						want = values[partition][position-1]
					}
					assertIncrementalNthValueWindowValues(t, updates, "nth_value", map[string]interface{}{row["id"].(string): want})
				}
			})
		}
	}
}

func assertIncrementalNthValueWindowValues(t *testing.T, updates []DifferentialRow, outputColumn string, want map[string]interface{}) {
	t.Helper()
	if len(updates) != len(want) {
		t.Fatalf("updates = %#v, want one update per key", updates)
	}
	for _, update := range updates {
		if update.Diff != 1 {
			t.Fatalf("update = %#v, want positive diff", update)
		}
		wantValue, exists := want[update.Key]
		if !exists {
			t.Fatalf("unexpected update key %q", update.Key)
		}
		got, exists := update.Row[outputColumn]
		if !exists || !reflect.DeepEqual(got, wantValue) {
			t.Fatalf("update %q value = %#v, want %#v", update.Key, got, wantValue)
		}
	}
}
