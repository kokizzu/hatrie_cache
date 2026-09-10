package hatSql

import (
	"errors"
	"math/rand"
	"strconv"
	"testing"
)

func TestIncrementalFrameWindowMinMaxMaintainsBoundedNullAwareFrames(t *testing.T) {
	for _, kind := range []IncrementalFrameWindowKind{IncrementalWindowFrameMinInt64, IncrementalWindowFrameMaxInt64} {
		window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
			Kind:           kind,
			OutputColumn:   "frame_value",
			FramePreceding: 2,
			PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
			OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
			RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
			ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{"id": "a", "partition": "p1", "order": int64(1), "value": int64(5)},
			{"id": "x", "partition": "p2", "order": int64(1), "value": int64(9)},
			{"id": "b", "partition": "p1", "order": int64(2), "value": nil},
			{"id": "c", "partition": "p1", "order": int64(3), "value": int64(3)},
			{"id": "y", "partition": "p2", "order": int64(2), "value": int64(4)},
			{"id": "d", "partition": "p1", "order": int64(4), "value": int64(7)},
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]interface{}{
			"a": int64(5),
			"b": int64(5),
			"c": int64(3),
			"d": int64(3),
			"x": int64(9),
			"y": int64(4),
		}
		if kind == IncrementalWindowFrameMaxInt64 {
			want["c"] = int64(5)
			want["d"] = int64(7)
			want["y"] = int64(9)
		}
		assertIncrementalFrameValues(t, updates, "frame_value", want)
		if _, exists := rows[0]["frame_value"]; exists {
			t.Fatal("Append mutated an input row")
		}
	}

	allNull, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameMinInt64,
		OutputColumn:   "frame_value",
		FramePreceding: 1,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err := allNull.Append([]Row{
		{"id": "a", "order": int64(1), "value": nil},
		{"id": "b", "order": int64(2), "value": nil},
		{"id": "c", "order": int64(3), "value": int64(4)},
		{"id": "d", "order": int64(4), "value": nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{"a": nil, "b": nil, "c": int64(4), "d": int64(4)})
}

func TestIncrementalFrameWindowExtremaValidatesAtomicallyAndRetainsBoundedState(t *testing.T) {
	orderError := errors.New("order callback failed")
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameMaxInt64,
		OutputColumn:   "frame_value",
		FramePreceding: 3,
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
		t.Fatalf("callback error = %v, want callback error", err)
	}
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": "wrong"}}); !errors.Is(err, ErrIncrementalFrameWindowExtremaValueInvalid) {
		t.Fatalf("typed value error = %v, want extrema-value error", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(20)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{"b": int64(20)})
	if _, err := window.Append([]Row{{"id": "c", "order": int64(4), "frame_value": int64(0), "value": int64(40)}}); !errors.Is(err, ErrIncrementalFrameWindowOutputConflict) {
		t.Fatalf("output conflict = %v", err)
	}

	for index := 0; index < 1000; index++ {
		updates, err = window.Append([]Row{{
			"id":    "row-" + strconv.Itoa(index),
			"order": int64(3 + index),
			"value": int64(index % 17),
		}})
		if err != nil {
			t.Fatalf("bounded append %d: %v", index, err)
		}
		if len(updates) != 1 {
			t.Fatalf("bounded append %d updates = %#v", index, updates)
		}
	}
	state := window.partitions[""]
	activeDeque := state.monotonicCount
	if len(state.contributions) > 5 || activeDeque > 5 {
		t.Fatalf("retained frame state grew: contributions=%d deque=%d", len(state.contributions), activeDeque)
	}

	boundedMin, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameMinInt64,
		OutputColumn:   "frame_value",
		FramePreceding: 4,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1000; index++ {
		if _, err := boundedMin.Append([]Row{{
			"id":    "increasing-" + strconv.Itoa(index),
			"order": int64(index),
			"value": int64(index),
		}}); err != nil {
			t.Fatalf("increasing append %d: %v", index, err)
		}
	}
	state = boundedMin.partitions[""]
	if len(state.contributions) > 5 || len(state.monotonic) > 5 {
		t.Fatalf("monotonic deque backing grew: contributions=%d deque=%d", len(state.contributions), len(state.monotonic))
	}
}

func TestIncrementalFrameWindowExtremaMatchesReferenceRandomized(t *testing.T) {
	for _, kind := range []IncrementalFrameWindowKind{IncrementalWindowFrameMinInt64, IncrementalWindowFrameMaxInt64} {
		for _, descending := range []bool{false, true} {
			t.Run(strconv.Itoa(int(kind))+"/"+strconv.FormatBool(descending), func(t *testing.T) {
				window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
					Kind:           kind,
					OutputColumn:   "frame_value",
					FramePreceding: 4,
					Descending:     descending,
					PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
					OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
					RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
					ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
				})
				if err != nil {
					t.Fatal(err)
				}
				random := rand.New(rand.NewSource(65067))
				nextOrder := map[string]int64{"p0": 0, "p1": 0, "p2": 0}
				history := make(map[string][]interface{})
				for index := 0; index < 300; index++ {
					partition := "p" + strconv.Itoa(random.Intn(3))
					order := nextOrder[partition]
					if descending {
						order = 1000 - order
					}
					nextOrder[partition]++
					value := interface{}(int64(random.Intn(101) - 50))
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
					history[partition] = append(history[partition], value)
					if len(history[partition]) > 5 {
						history[partition] = history[partition][len(history[partition])-5:]
					}
					want := interface{}(nil)
					for _, item := range history[partition] {
						if item == nil {
							continue
						}
						if want == nil {
							want = item
							continue
						}
						itemValue := item.(int64)
						wantValue := want.(int64)
						if kind == IncrementalWindowFrameMinInt64 && itemValue < wantValue {
							want = item
						}
						if kind == IncrementalWindowFrameMaxInt64 && itemValue > wantValue {
							want = item
						}
					}
					assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{row["id"].(string): want})
				}
			})
		}
	}
}
