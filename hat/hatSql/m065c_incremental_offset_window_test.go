package hatSql

import (
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestIncrementalOffsetWindowLagMaintainsBoundedHistory(t *testing.T) {
	window, err := NewIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		Offset:       2,
		DefaultValue: int64(-1),
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
	rows := []Row{
		{"id": "a", "partition": "p", "order": int64(1), "value": int64(10)},
		{"id": "b", "partition": "p", "order": int64(2), "value": int64(20)},
		{"id": "c", "partition": "p", "order": int64(3), "value": int64(30)},
	}
	updates, err := window.Append(rows)
	if err != nil {
		t.Fatal(err)
	}
	assertOffsetWindowValues(t, updates, map[string]interface{}{
		"a": int64(-1),
		"b": int64(-1),
		"c": int64(10),
	})
	if _, exists := rows[0]["lag_value"]; exists {
		t.Fatal("Append mutated an input row")
	}

	updates, err = window.Append([]Row{{"id": "d", "partition": "p", "order": int64(4), "value": int64(40)}})
	if err != nil {
		t.Fatal(err)
	}
	assertOffsetWindowValues(t, updates, map[string]interface{}{"d": int64(20)})
}

func TestIncrementalOffsetWindowLeadRetractionsAndBatchLookahead(t *testing.T) {
	window, err := NewIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLead,
		OutputColumn: "lead_value",
		Offset:       1,
		DefaultValue: nil,
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
	if _, err := window.Append([]Row{{"id": "a", "partition": "p", "order": int64(1), "value": int64(10)}}); err != nil {
		t.Fatal(err)
	}
	updates, err := window.Append([]Row{{"id": "b", "partition": "p", "order": int64(2), "value": int64(20)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 3 {
		t.Fatalf("lead transition updates = %#v, want old/new a and new b", updates)
	}
	assertOffsetWindowDiffs(t, updates, "a", []int64{-1, 1})
	assertOffsetWindowValue(t, updates[1], "a", int64(20))
	assertOffsetWindowValue(t, updates[2], "b", nil)

	updates, err = window.Append([]Row{
		{"id": "c", "partition": "p", "order": int64(3), "value": int64(30)},
		{"id": "d", "partition": "p", "order": int64(4), "value": int64(40)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 4 {
		t.Fatalf("lead batch updates = %#v, want old/new b and final c/d", updates)
	}
	assertOffsetWindowDiffs(t, updates, "b", []int64{-1, 1})
	assertOffsetWindowValue(t, updates[1], "b", int64(30))
	assertOffsetWindowValue(t, updates[2], "c", int64(40))
	assertOffsetWindowValue(t, updates[3], "d", nil)
}

func TestIncrementalOffsetWindowSupportsPartitionsDescendingAndZeroOffset(t *testing.T) {
	window, err := NewIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		Offset:       1,
		DefaultValue: int64(0),
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
		Descending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err := window.Append([]Row{
		{"id": "a", "partition": "p1", "order": int64(3), "value": int64(30)},
		{"id": "x", "partition": "p2", "order": int64(8), "value": int64(80)},
		{"id": "b", "partition": "p1", "order": int64(2), "value": int64(20)},
		{"id": "y", "partition": "p2", "order": int64(7), "value": int64(70)},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOffsetWindowValues(t, updates, map[string]interface{}{
		"a": int64(0),
		"x": int64(0),
		"b": int64(30),
		"y": int64(80),
	})

	zero, err := NewIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLead,
		OutputColumn: "current_value",
		ValueKey: func(row Row) (interface{}, error) {
			return row["value"], nil
		},
		OrderKey: func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:   func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err = zero.Append([]Row{{"id": "z", "order": int64(1), "value": "now"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].Key != "z" || updates[0].Diff != 1 || !reflect.DeepEqual(updates[0].Row["current_value"], "now") {
		t.Fatalf("zero-offset updates = %#v, want current_value=now", updates)
	}
}

func TestIncrementalOffsetWindowAppendIsAtomic(t *testing.T) {
	orderError := errors.New("order callback failed")
	window, err := NewIncrementalOffsetWindow(IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		Offset:       1,
		DefaultValue: int64(-1),
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
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": int64(10)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2), "value": int64(20)},
		{"id": "bad", "order": int64(3), "value": int64(30)},
	}); !errors.Is(err, orderError) {
		t.Fatalf("Append error = %v, want callback error", err)
	}
	updates, err := window.Append([]Row{{"id": "c", "order": int64(2), "value": int64(30)}})
	if err != nil {
		t.Fatal(err)
	}
	assertOffsetWindowValues(t, updates, map[string]interface{}{"c": int64(10)})
}

func TestIncrementalOffsetWindowRejectsInvalidDefinitionsAndOrder(t *testing.T) {
	base := IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	}
	invalid := base
	invalid.ValueKey = nil
	if _, err := NewIncrementalOffsetWindow(invalid); err == nil {
		t.Fatal("missing value key was accepted")
	}
	base.Offset = -1
	if _, err := NewIncrementalOffsetWindow(base); err == nil {
		t.Fatal("negative offset was accepted")
	}

	base.Offset = 1
	window, err := NewIncrementalOffsetWindow(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "a", "order": int64(2), "value": int64(20)},
		{"id": "b", "order": int64(1), "value": int64(10)},
	}); !errors.Is(err, ErrIncrementalOffsetWindowOutOfOrder) {
		t.Fatalf("Append error = %v, want out-of-order error", err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": int64(10)}}); err != nil {
		t.Fatal(err)
	}
}

func TestIncrementalOffsetWindowMatchesNaiveRecomputeAcrossMixedBatches(t *testing.T) {
	for _, direction := range []IncrementalOffsetWindowDirection{IncrementalWindowLag, IncrementalWindowLead} {
		for _, offset := range []int{0, 1, 2} {
			name := "lag-" + strconv.Itoa(offset)
			if direction == IncrementalWindowLead {
				name = "lead-" + strconv.Itoa(offset)
			}
			t.Run(name, func(t *testing.T) {
				definition := IncrementalOffsetWindowDefinition{
					Direction:    direction,
					OutputColumn: "offset_value",
					Offset:       offset,
					DefaultValue: int64(-1),
					PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
				}
				window, err := NewIncrementalOffsetWindow(definition)
				if err != nil {
					t.Fatal(err)
				}
				rows := make(map[string]Row)
				nextOrder := map[string]int64{"p0": 0, "p1": 0, "p2": 0}
				random := rand.New(rand.NewSource(int64(65000+offset) + int64(direction)*100))
				for iteration := 0; iteration < 80; iteration++ {
					before := naiveOffsetWindowOutputs(rows, definition)
					batch := make([]Row, 0, 1+random.Intn(3))
					for batchIndex := 0; batchIndex < cap(batch); batchIndex++ {
						partition := "p" + strconv.Itoa(random.Intn(3))
						order := nextOrder[partition]
						nextOrder[partition] = order + 1
						key := "row-" + strconv.Itoa(iteration) + "-" + strconv.Itoa(batchIndex)
						row := Row{
							"id":        key,
							"partition": partition,
							"order":     order,
							"value":     int64(iteration*10 + batchIndex),
						}
						batch = append(batch, row)
						rows[key] = row
					}
					updates, err := window.Append(batch)
					if err != nil {
						t.Fatalf("iteration %d Append error = %v", iteration, err)
					}
					after := naiveOffsetWindowOutputs(rows, definition)
					want := naiveOffsetWindowChanges(before, after)
					got := make(map[string]DifferentialRow, len(updates))
					for _, update := range updates {
						got[update.Key+"\x00"+strconv.FormatInt(update.Diff, 10)] = update
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("iteration %d changes = %#v, want %#v", iteration, got, want)
					}
				}
			})
		}
	}
}

func naiveOffsetWindowOutputs(rows map[string]Row, definition IncrementalOffsetWindowDefinition) map[string]Row {
	byPartition := make(map[string][]Row)
	for _, row := range rows {
		partition, _ := definition.PartitionKey(row)
		byPartition[partition] = append(byPartition[partition], row)
	}
	outputs := make(map[string]Row, len(rows))
	for _, partitionRows := range byPartition {
		sort.SliceStable(partitionRows, func(left, right int) bool {
			leftOrder, _ := definition.OrderKey(partitionRows[left])
			rightOrder, _ := definition.OrderKey(partitionRows[right])
			comparison := sqlCompare(leftOrder, rightOrder)
			if comparison != 0 {
				return comparison < 0
			}
			leftKey, _ := definition.RowKey(partitionRows[left])
			rightKey, _ := definition.RowKey(partitionRows[right])
			return leftKey < rightKey
		})
		for index, row := range partitionRows {
			value := definition.DefaultValue
			if definition.Direction == IncrementalWindowLag {
				if index >= definition.Offset {
					value, _ = definition.ValueKey(partitionRows[index-definition.Offset])
				}
			} else if index+definition.Offset < len(partitionRows) {
				value, _ = definition.ValueKey(partitionRows[index+definition.Offset])
			}
			key, _ := definition.RowKey(row)
			outputs[key] = incrementalOffsetWindowOutput(row, definition.OutputColumn, value)
		}
	}
	return outputs
}

func naiveOffsetWindowChanges(before, after map[string]Row) map[string]DifferentialRow {
	keys := make(map[string]struct{}, len(before)+len(after))
	for key := range before {
		keys[key] = struct{}{}
	}
	for key := range after {
		keys[key] = struct{}{}
	}
	want := make(map[string]DifferentialRow)
	for key := range keys {
		oldRow, hadOld := before[key]
		newRow, hasNew := after[key]
		if hadOld && hasNew && reflect.DeepEqual(oldRow, newRow) {
			continue
		}
		if hadOld {
			want[key+"\x00-1"] = DifferentialRow{Key: key, Diff: -1, Row: oldRow}
		}
		if hasNew {
			want[key+"\x001"] = DifferentialRow{Key: key, Diff: 1, Row: newRow}
		}
	}
	return want
}

func assertOffsetWindowValues(t *testing.T, updates []DifferentialRow, want map[string]interface{}) {
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
		if got := update.Row["lag_value"]; !reflect.DeepEqual(got, wantValue) {
			if got := update.Row["lead_value"]; !reflect.DeepEqual(got, wantValue) {
				t.Fatalf("update %q value = %#v, want %#v", update.Key, got, wantValue)
			}
		}
	}
}

func assertOffsetWindowDiffs(t *testing.T, updates []DifferentialRow, key string, want []int64) {
	t.Helper()
	got := make([]int64, 0, len(want))
	for _, update := range updates {
		if update.Key == key {
			got = append(got, update.Diff)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diffs for %q = %#v, want %#v", key, got, want)
	}
}

func assertOffsetWindowValue(t *testing.T, update DifferentialRow, key string, want interface{}) {
	t.Helper()
	if update.Key != key || update.Diff != 1 {
		t.Fatalf("update = %#v, want positive %q update", update, key)
	}
	value, exists := update.Row["lead_value"]
	if !exists {
		t.Fatalf("update %q has no lead_value: %#v", key, update.Row)
	}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("update %q value = %#v, want %#v", key, value, want)
	}
}
