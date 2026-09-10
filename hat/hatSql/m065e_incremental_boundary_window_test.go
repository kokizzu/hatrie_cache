package hatSql

import (
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func TestIncrementalBoundaryWindowFirstAndLastValuesMaintainPartitions(t *testing.T) {
	for _, kind := range []IncrementalBoundaryWindowKind{IncrementalWindowFirstValue, IncrementalWindowLastValue} {
		window, err := NewIncrementalBoundaryWindow(IncrementalBoundaryWindowDefinition{
			Kind:         kind,
			OutputColumn: "boundary_value",
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
			{"id": "y", "partition": "p2", "order": int64(2), "value": "Y"},
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]interface{}{
			"a": "A",
			"b": "B",
			"x": "X",
			"y": "Y",
		}
		if kind == IncrementalWindowFirstValue {
			want["b"] = "A"
			want["y"] = "X"
		}
		assertIncrementalBoundaryWindowValues(t, updates, "boundary_value", want)
		if _, exists := updates[0].Row["value"]; !exists {
			t.Fatal("output dropped an input column")
		}
		if _, exists := rows[0]["boundary_value"]; exists {
			t.Fatal("Append mutated an input row")
		}
	}
}

func TestIncrementalBoundaryWindowSupportsNullDescendingAndAtomicCallbacks(t *testing.T) {
	orderError := errors.New("order callback failed")
	window, err := NewIncrementalBoundaryWindow(IncrementalBoundaryWindowDefinition{
		Kind:         IncrementalWindowFirstValue,
		OutputColumn: "first_value",
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
	updates, err := window.Append([]Row{{"id": "a", "order": int64(3), "value": nil}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalBoundaryWindowValues(t, updates, "first_value", map[string]interface{}{"a": nil})
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2), "value": "B"},
		{"id": "bad", "order": int64(1), "value": "bad"},
	}); !errors.Is(err, orderError) {
		t.Fatalf("callback error = %v, want wrapped callback error", err)
	}
	updates, err = window.Append([]Row{{"id": "b", "order": int64(2), "value": "B"}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalBoundaryWindowValues(t, updates, "first_value", map[string]interface{}{"b": nil})
	if _, err := window.Append([]Row{{"id": "c", "order": int64(4), "value": "C"}}); !errors.Is(err, ErrIncrementalBoundaryWindowOutOfOrder) {
		t.Fatalf("order error = %v, want out-of-order error", err)
	}

	last, err := NewIncrementalBoundaryWindow(IncrementalBoundaryWindowDefinition{
		Kind:         IncrementalWindowLastValue,
		OutputColumn: "last_value",
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err = last.Append([]Row{{"id": "n", "order": int64(1), "value": nil}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalBoundaryWindowValues(t, updates, "last_value", map[string]interface{}{"n": nil})
}

func TestIncrementalBoundaryWindowRejectsInvalidDefinitionsAndRows(t *testing.T) {
	base := IncrementalBoundaryWindowDefinition{
		Kind:         IncrementalWindowFirstValue,
		OutputColumn: "boundary_value",
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	}
	invalid := base
	invalid.Kind = 0
	if _, err := NewIncrementalBoundaryWindow(invalid); !errors.Is(err, ErrIncrementalBoundaryWindowInvalidKind) {
		t.Fatalf("invalid kind error = %v", err)
	}
	invalid = base
	invalid.OutputColumn = " "
	if _, err := NewIncrementalBoundaryWindow(invalid); !errors.Is(err, ErrIncrementalBoundaryWindowOutputRequired) {
		t.Fatalf("output error = %v", err)
	}
	invalid = base
	invalid.ValueKey = nil
	if _, err := NewIncrementalBoundaryWindow(invalid); !errors.Is(err, ErrIncrementalBoundaryWindowValueRequired) {
		t.Fatalf("value error = %v", err)
	}
	invalid = base
	invalid.OrderKey = nil
	if _, err := NewIncrementalBoundaryWindow(invalid); !errors.Is(err, ErrIncrementalBoundaryWindowOrderRequired) {
		t.Fatalf("order error = %v", err)
	}
	invalid = base
	invalid.RowKey = nil
	if _, err := NewIncrementalBoundaryWindow(invalid); !errors.Is(err, ErrIncrementalBoundaryWindowRowKeyRequired) {
		t.Fatalf("row key error = %v", err)
	}

	window, err := NewIncrementalBoundaryWindow(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": "A"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(2), "value": "A2"}}); !errors.Is(err, ErrIncrementalBoundaryWindowDuplicateKey) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "boundary_value": "existing", "value": "B"}}); !errors.Is(err, ErrIncrementalBoundaryWindowOutputConflict) {
		t.Fatalf("output conflict = %v", err)
	}
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": "B"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := (*IncrementalBoundaryWindow)(nil).Append([]Row{{"id": "nil"}}); !errors.Is(err, ErrIncrementalBoundaryWindowNil) {
		t.Fatalf("nil receiver error = %v", err)
	}
}

func TestIncrementalBoundaryWindowMatchesReferenceRandomized(t *testing.T) {
	for _, descending := range []bool{false, true} {
		for _, kind := range []IncrementalBoundaryWindowKind{IncrementalWindowFirstValue, IncrementalWindowLastValue} {
			t.Run(strconv.FormatBool(descending)+"/"+strconv.Itoa(int(kind)), func(t *testing.T) {
				window, err := NewIncrementalBoundaryWindow(IncrementalBoundaryWindowDefinition{
					Kind:         kind,
					OutputColumn: "boundary_value",
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
				first := make(map[string]interface{})
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
					if _, exists := first[partition]; !exists {
						first[partition] = value
					}
					want := value
					if kind == IncrementalWindowFirstValue {
						want = first[partition]
					}
					assertIncrementalBoundaryWindowValues(t, updates, "boundary_value", map[string]interface{}{row["id"].(string): want})
				}
			})
		}
	}
}

func assertIncrementalBoundaryWindowValues(t *testing.T, updates []DifferentialRow, outputColumn string, want map[string]interface{}) {
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
