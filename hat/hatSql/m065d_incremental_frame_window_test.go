package hatSql

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func TestIncrementalFrameWindowCountMaintainsBoundedPrecedingFrame(t *testing.T) {
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameCount,
		OutputColumn:   "frame_count",
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
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := []Row{
		{"id": "a", "partition": "p1", "order": int64(1)},
		{"id": "x", "partition": "p2", "order": int64(1)},
		{"id": "b", "partition": "p1", "order": int64(2)},
		{"id": "c", "partition": "p1", "order": int64(3)},
		{"id": "y", "partition": "p2", "order": int64(2)},
		{"id": "d", "partition": "p1", "order": int64(4)},
	}
	updates, err := window.Append(rows)
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_count", map[string]interface{}{
		"a": int64(1),
		"b": int64(2),
		"c": int64(3),
		"d": int64(3),
		"x": int64(1),
		"y": int64(2),
	})
	if _, exists := rows[0]["frame_count"]; exists {
		t.Fatal("Append mutated an input row")
	}

	updates, err = window.Append([]Row{{"id": "e", "partition": "p1", "order": int64(5)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_count", map[string]interface{}{"e": int64(3)})
}

func TestIncrementalFrameWindowSumIgnoresNullAndUsesCheckedInt64(t *testing.T) {
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameSumInt64,
		OutputColumn:   "frame_sum",
		FramePreceding: 2,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err := window.Append([]Row{
		{"id": "a", "order": int64(1), "value": int64(5)},
		{"id": "b", "order": int64(2), "value": nil},
		{"id": "c", "order": int64(3), "value": int64(3)},
		{"id": "d", "order": int64(4), "value": int64(4)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_sum", map[string]interface{}{
		"a": int64(5),
		"b": int64(5),
		"c": int64(8),
		"d": int64(7),
	})

	overflow, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameSumInt64,
		OutputColumn:   "frame_sum",
		FramePreceding: 1,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := overflow.Append([]Row{{"id": "a", "order": int64(1), "value": int64(math.MaxInt64)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := overflow.Append([]Row{{"id": "bad", "order": int64(2), "value": int64(1)}}); !errors.Is(err, ErrIncrementalFrameWindowSumOverflow) {
		t.Fatalf("overflow error = %v, want checked sum overflow", err)
	}
	updates, err = overflow.Append([]Row{{"id": "b", "order": int64(2), "value": int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_sum", map[string]interface{}{"b": int64(math.MaxInt64)})
}

func TestIncrementalFrameWindowSupportsDescendingAndAtomicCallbacks(t *testing.T) {
	orderError := errors.New("order callback failed")
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameCount,
		OutputColumn:   "frame_count",
		FramePreceding: 1,
		Descending:     true,
		OrderKey: func(row Row) (interface{}, error) {
			if row["id"] == "bad" {
				return nil, orderError
			}
			return row["order"], nil
		},
		RowKey: func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(3)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2)},
		{"id": "bad", "order": int64(1)},
	}); !errors.Is(err, orderError) {
		t.Fatalf("callback error = %v, want callback error", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_count", map[string]interface{}{"b": int64(2)})
	if _, err := window.Append([]Row{{"id": "c", "order": int64(4)}}); !errors.Is(err, ErrIncrementalFrameWindowOutOfOrder) {
		t.Fatalf("order error = %v, want out-of-order error", err)
	}
}

func TestIncrementalFrameWindowRejectsInvalidDefinitions(t *testing.T) {
	base := IncrementalFrameWindowDefinition{
		Kind:         IncrementalWindowFrameCount,
		OutputColumn: "frame_count",
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
	}
	invalid := base
	invalid.Kind = 0
	if _, err := NewIncrementalFrameWindow(invalid); err == nil {
		t.Fatal("invalid frame kind was accepted")
	}
	invalid = base
	invalid.FramePreceding = -1
	if _, err := NewIncrementalFrameWindow(invalid); err == nil {
		t.Fatal("negative frame bound was accepted")
	}
	invalid = base
	invalid.Kind = IncrementalWindowFrameSumInt64
	if _, err := NewIncrementalFrameWindow(invalid); err == nil {
		t.Fatal("missing sum value key was accepted")
	}

	window, err := NewIncrementalFrameWindow(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "a", "order": int64(2)},
		{"id": "b", "order": int64(1)},
	}); !errors.Is(err, ErrIncrementalFrameWindowOutOfOrder) {
		t.Fatalf("order error = %v, want out-of-order error", err)
	}
}

func TestIncrementalFrameWindowMatchesReferenceRandomized(t *testing.T) {
	for _, descending := range []bool{false, true} {
		for _, kind := range []IncrementalFrameWindowKind{IncrementalWindowFrameCount, IncrementalWindowFrameSumInt64} {
			t.Run(strconv.FormatBool(descending)+"/"+strconv.Itoa(int(kind)), func(t *testing.T) {
				window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
					Kind:           kind,
					OutputColumn:   "frame_value",
					FramePreceding: 3,
					Descending:     descending,
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
					t.Fatal(err)
				}

				random := rand.New(rand.NewSource(65065))
				nextOrder := map[string]int64{"p0": 0, "p1": 0, "p2": 0}
				history := make(map[string][]incrementalFrameWindowContribution)
				for index := 0; index < 240; index++ {
					partition := "p" + strconv.Itoa(random.Intn(3))
					order := nextOrder[partition]
					if descending {
						order = 1000 - order
					} else {
						order++
					}
					nextOrder[partition]++
					row := Row{
						"id":        "row-" + strconv.Itoa(index),
						"partition": partition,
						"order":     order,
					}
					contribution := incrementalFrameWindowContribution{valid: true}
					if kind == IncrementalWindowFrameSumInt64 {
						if random.Intn(4) == 0 {
							row["value"] = nil
							contribution.valid = false
						} else {
							contribution.value = int64(random.Intn(31) - 15)
							row["value"] = contribution.value
						}
					}

					updates, err := window.Append([]Row{row})
					if err != nil {
						t.Fatalf("append %d: %v", index, err)
					}
					history[partition] = append(history[partition], contribution)
					if len(history[partition]) > 4 {
						history[partition] = history[partition][len(history[partition])-4:]
					}
					want := interface{}(int64(len(history[partition])))
					if kind == IncrementalWindowFrameSumInt64 {
						sum := int64(0)
						valid := false
						for _, item := range history[partition] {
							if item.valid {
								sum += item.value
								valid = true
							}
						}
						if valid {
							want = sum
						} else {
							want = nil
						}
					}
					assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{row["id"].(string): want})
				}
			})
		}
	}
}

func TestIncrementalFrameWindowRejectsDuplicateOutputAndTypedValue(t *testing.T) {
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameCount,
		OutputColumn:   "frame_count",
		FramePreceding: 1,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(2)}}); !errors.Is(err, ErrIncrementalFrameWindowDuplicateKey) {
		t.Fatalf("duplicate error = %v, want duplicate-key error", err)
	}
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "frame_count": int64(99)}}); !errors.Is(err, ErrIncrementalFrameWindowOutputConflict) {
		t.Fatalf("output conflict = %v, want output-conflict error", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_count", map[string]interface{}{"b": int64(2)})

	sum, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameSumInt64,
		OutputColumn:   "frame_sum",
		FramePreceding: 1,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sum.Append([]Row{{"id": "a", "order": int64(1), "value": "wrong"}}); !errors.Is(err, ErrIncrementalFrameWindowSumValueInvalid) {
		t.Fatalf("typed value error = %v, want invalid-value error", err)
	}
	updates, err = sum.Append([]Row{{"id": "a", "order": int64(1), "value": int64(7)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_sum", map[string]interface{}{"a": int64(7)})
}

func assertIncrementalFrameValues(t *testing.T, updates []DifferentialRow, outputColumn string, want map[string]interface{}) {
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
