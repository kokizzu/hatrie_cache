package hatSql

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func m065mRangeDefinition(kind IncrementalRangeWindowKind, preceding int64, descending bool) IncrementalRangeWindowDefinition {
	return IncrementalRangeWindowDefinition{
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

func applyM065mRangeUpdates(t *testing.T, current map[string]Row, updates []DifferentialRow) {
	t.Helper()
	for _, update := range updates {
		switch update.Diff {
		case -1:
			previous, ok := current[update.Key]
			if !ok {
				t.Fatalf("retraction for missing key %q", update.Key)
			}
			if !reflect.DeepEqual(previous, update.Row) {
				t.Fatalf("retraction for %q = %#v, want %#v", update.Key, update.Row, previous)
			}
			delete(current, update.Key)
		case 1:
			if _, exists := current[update.Key]; exists {
				t.Fatalf("insertion for existing key %q: %#v", update.Key, update.Row)
			}
			current[update.Key] = update.Row
		default:
			t.Fatalf("update %q has diff %d", update.Key, update.Diff)
		}
	}
}

func assertM065mRangeValues(t *testing.T, current map[string]Row, want map[string]interface{}) {
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

func TestM065mIncrementalRangeCountIsPeerAware(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowCount, 1, false))
	if err != nil {
		t.Fatal(err)
	}
	rows := []Row{
		{"id": "a", "order": int64(1), "value": int64(10)},
		{"id": "b", "order": int64(1), "value": int64(20)},
		{"id": "c", "order": int64(2), "value": int64(30)},
		{"id": "d", "order": int64(4), "value": int64(40)},
	}
	current := make(map[string]Row)
	updates, err := window.Append(rows[:2])
	if err != nil {
		t.Fatal(err)
	}
	applyM065mRangeUpdates(t, current, updates)
	assertM065mRangeValues(t, current, map[string]interface{}{"a": int64(2), "b": int64(2)})

	updates, err = window.Append(rows[2:])
	if err != nil {
		t.Fatal(err)
	}
	applyM065mRangeUpdates(t, current, updates)
	assertM065mRangeValues(t, current, map[string]interface{}{
		"a": int64(2),
		"b": int64(2),
		"c": int64(3),
		"d": int64(1),
	})
	for _, row := range rows {
		if _, exists := row["window_value"]; exists {
			t.Fatalf("input row was mutated: %#v", row)
		}
	}
}

func TestM065mIncrementalRangeSumHandlesNullAndDescendingPeers(t *testing.T) {
	t.Run("null-aware peers", func(t *testing.T) {
		window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowSumInt64, 0, false))
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{"id": "a", "order": int64(1), "value": int64(10)},
			{"id": "b", "order": int64(1), "value": nil},
			{"id": "c", "order": int64(2), "value": int64(5)},
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatal(err)
		}
		current := make(map[string]Row)
		applyM065mRangeUpdates(t, current, updates)
		assertM065mRangeValues(t, current, map[string]interface{}{"a": int64(10), "b": int64(10), "c": int64(5)})
	})

	t.Run("descending", func(t *testing.T) {
		window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowSumInt64, 1, true))
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{"id": "a", "order": int64(5), "value": int64(10)},
			{"id": "b", "order": int64(4), "value": int64(2)},
			{"id": "c", "order": int64(4), "value": nil},
			{"id": "d", "order": int64(2), "value": int64(7)},
		}
		updates, err := window.Append(rows)
		if err != nil {
			t.Fatal(err)
		}
		current := make(map[string]Row)
		applyM065mRangeUpdates(t, current, updates)
		assertM065mRangeValues(t, current, map[string]interface{}{"a": int64(10), "b": int64(12), "c": int64(12), "d": int64(7)})
	})
}

func TestM065mIncrementalRangeValidatesAtomically(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowSumInt64, 2, false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": int64(4)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2), "value": int64(5)},
		{"id": "c", "order": int64(3), "value": "invalid"},
	}); !errors.Is(err, ErrIncrementalRangeWindowSumValueInvalid) {
		t.Fatalf("invalid batch error = %v, want sum value error", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(5)}})
	if err != nil {
		t.Fatal(err)
	}
	current := make(map[string]Row)
	applyM065mRangeUpdates(t, current, updates)
	assertM065mRangeValues(t, current, map[string]interface{}{"b": int64(9)})
	if _, err := window.Append([]Row{{"id": "a2", "order": int64(1), "value": int64(2)}}); !errors.Is(err, ErrIncrementalRangeWindowOutOfOrder) {
		t.Fatalf("out-of-order error = %v, want out-of-order error", err)
	}
}

func TestM065mIncrementalRangeRejectsOverflowAfterEvictionAtomically(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowSumInt64, 1, false))
	if err != nil {
		t.Fatal(err)
	}
	rows := []Row{
		{"id": "a", "order": int64(1), "value": int64(math.MaxInt64)},
		{"id": "b", "order": int64(2), "value": int64(-math.MaxInt64)},
		{"id": "c", "order": int64(2), "value": int64(math.MaxInt64)},
		{"id": "d", "order": int64(3), "value": int64(math.MaxInt64)},
	}
	if _, err := window.Append(rows); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "e", "order": int64(4), "value": int64(1)}}); !errors.Is(err, ErrIncrementalRangeWindowSumOverflow) {
		t.Fatalf("eviction overflow = %v, want sum overflow", err)
	}
}

func TestM065mIncrementalRangeMatchesReference(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowCount, 2, false))
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]Row, 180)
	current := make(map[string]Row)
	for index := range rows {
		rows[index] = Row{"id": "row-" + string(rune('a'+index%26)) + string(rune('a'+index/26)), "order": int64(index / 3)}
		updates, err := window.Append([]Row{rows[index]})
		if err != nil {
			t.Fatalf("append %d: %v", index, err)
		}
		applyM065mRangeUpdates(t, current, updates)
		want := make(map[string]interface{}, index+1)
		for candidate := 0; candidate <= index; candidate++ {
			candidateOrder := rows[candidate]["order"].(int64)
			count := int64(0)
			for prior := 0; prior <= index; prior++ {
				priorOrder := rows[prior]["order"].(int64)
				if priorOrder >= candidateOrder-2 && priorOrder <= candidateOrder {
					count++
				}
			}
			want[rows[candidate]["id"].(string)] = count
		}
		assertM065mRangeValues(t, current, want)
	}
}

func BenchmarkM065mRangeIncremental(b *testing.B) {
	benchmarkRows := m065mRangeBenchmarkRows()
	rows := make([]Row, len(benchmarkRows))
	for index, source := range benchmarkRows {
		rows[index] = Row{
			"id":    "row-" + string(rune(index)),
			"order": source.order,
			"value": source.value,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowSumInt64, 64, false))
		if err != nil {
			b.Fatal(err)
		}
		updates, err := window.Append(rows)
		if err != nil {
			b.Fatal(err)
		}
		var checksum int64
		for _, update := range updates {
			checksum += update.Row["window_value"].(int64)
		}
		m065mRangeBenchmarkSink = checksum
	}
}
