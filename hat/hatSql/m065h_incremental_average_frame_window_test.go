package hatSql

import (
	"errors"
	"math"
	"math/rand"
	"strconv"
	"testing"
)

func TestIncrementalFrameWindowAverageMaintainsBoundedNullAwareFrames(t *testing.T) {
	for _, descending := range []bool{false, true} {
		window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
			Kind:           IncrementalWindowFrameAvgInt64,
			OutputColumn:   "frame_value",
			FramePreceding: 2,
			Descending:     descending,
			PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
			OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
			RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
			ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{"id": "a", "partition": "p1", "order": int64(4), "value": int64(5)},
			{"id": "x", "partition": "p2", "order": int64(4), "value": int64(9)},
			{"id": "b", "partition": "p1", "order": int64(3), "value": nil},
			{"id": "c", "partition": "p1", "order": int64(2), "value": int64(3)},
			{"id": "y", "partition": "p2", "order": int64(3), "value": int64(4)},
			{"id": "d", "partition": "p1", "order": int64(1), "value": int64(7)},
		}
		if !descending {
			for _, row := range rows {
				row["order"] = 5 - row["order"].(int64)
			}
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]interface{}{
			"a": float64(5),
			"b": float64(5),
			"c": float64(4),
			"d": float64(5),
			"x": float64(9),
			"y": float64(6.5),
		}
		assertIncrementalFrameValues(t, updates, "frame_value", want)
		if _, exists := rows[0]["frame_value"]; exists {
			t.Fatal("Append mutated an input row")
		}
	}

	allNull, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameAvgInt64,
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
	assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{"a": nil, "b": nil, "c": float64(4), "d": float64(4)})
}

func TestIncrementalFrameWindowAverageValidatesAtomicallyAndChecksSumOverflow(t *testing.T) {
	orderError := errors.New("order callback failed")
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameAvgInt64,
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
	if _, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": "wrong"}}); !errors.Is(err, ErrIncrementalFrameWindowAvgValueInvalid) {
		t.Fatalf("typed value error = %v, want average-value error", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(20)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{"b": float64(15)})

	overflow, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameAvgInt64,
		OutputColumn:   "frame_value",
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
	if _, err := overflow.Append([]Row{{"id": "b", "order": int64(2), "value": int64(1)}}); !errors.Is(err, ErrIncrementalFrameWindowSumOverflow) {
		t.Fatalf("overflow error = %v, want sum overflow", err)
	}
	updates, err = overflow.Append([]Row{{"id": "b", "order": int64(2), "value": int64(-1)}})
	if err != nil {
		t.Fatal(err)
	}
	assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{"b": float64(math.MaxInt64-1) / 2})
}

func TestIncrementalFrameWindowAverageMatchesReferenceRandomized(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(strconv.FormatBool(descending), func(t *testing.T) {
			window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
				Kind:           IncrementalWindowFrameAvgInt64,
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
			random := rand.New(rand.NewSource(65068))
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
				var sum int64
				count := 0
				for _, item := range history[partition] {
					if item == nil {
						continue
					}
					sum += item.(int64)
					count++
				}
				want := interface{}(nil)
				if count > 0 {
					want = float64(sum) / float64(count)
				}
				assertIncrementalFrameValues(t, updates, "frame_value", map[string]interface{}{row["id"].(string): want})
			}
		})
	}
}
