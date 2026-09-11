package hatSql

import (
	"errors"
	"math"
	"testing"
)

func TestM065nIncrementalRangeMinAndMaxArePeerAware(t *testing.T) {
	rows := []Row{
		{"id": "a", "order": int64(1), "value": int64(5)},
		{"id": "b", "order": int64(1), "value": int64(3)},
		{"id": "c", "order": int64(2), "value": int64(4)},
		{"id": "d", "order": int64(4), "value": int64(1)},
	}
	for _, test := range []struct {
		kind IncrementalRangeWindowKind
		want map[string]interface{}
	}{
		{IncrementalRangeWindowMinInt64, map[string]interface{}{"a": int64(3), "b": int64(3), "c": int64(3), "d": int64(1)}},
		{IncrementalRangeWindowMaxInt64, map[string]interface{}{"a": int64(5), "b": int64(5), "c": int64(5), "d": int64(1)}},
	} {
		t.Run(string(rune('0'+test.kind)), func(t *testing.T) {
			window, err := NewIncrementalRangeWindow(m065mRangeDefinition(test.kind, 1, false))
			if err != nil {
				t.Fatal(err)
			}
			updates, err := window.Append(rows)
			if err != nil {
				t.Fatal(err)
			}
			current := make(map[string]Row)
			applyM065mRangeUpdates(t, current, updates)
			assertM065mRangeValues(t, current, test.want)
		})
	}
}

func TestM065nIncrementalRangeMinMaxHandleNullAndDescending(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowMinInt64, 1, true))
	if err != nil {
		t.Fatal(err)
	}
	rows := []Row{
		{"id": "a", "order": int64(5), "value": int64(10)},
		{"id": "b", "order": int64(4), "value": nil},
		{"id": "c", "order": int64(4), "value": int64(2)},
		{"id": "d", "order": int64(2), "value": nil},
	}
	updates, err := window.Append(rows)
	if err != nil {
		t.Fatal(err)
	}
	current := make(map[string]Row)
	applyM065mRangeUpdates(t, current, updates)
	assertM065mRangeValues(t, current, map[string]interface{}{"a": int64(10), "b": int64(2), "c": int64(2), "d": nil})
}

func TestM065nIncrementalRangeValidatesExtremaAtomically(t *testing.T) {
	window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowMaxInt64, 2, false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "order": int64(1), "value": int64(4)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "b", "order": int64(2), "value": int64(5)},
		{"id": "c", "order": int64(3), "value": "invalid"},
	}); !errors.Is(err, ErrIncrementalRangeWindowExtremaValueInvalid) {
		t.Fatalf("invalid extrema batch error = %v", err)
	}
	updates, err := window.Append([]Row{{"id": "b", "order": int64(2), "value": int64(5)}})
	if err != nil {
		t.Fatal(err)
	}
	current := make(map[string]Row)
	applyM065mRangeUpdates(t, current, updates)
	assertM065mRangeValues(t, current, map[string]interface{}{"b": int64(5)})
	if _, err := window.Append([]Row{{"id": "a2", "order": int64(1), "value": int64(math.MinInt64)}}); !errors.Is(err, ErrIncrementalRangeWindowOutOfOrder) {
		t.Fatalf("out-of-order error = %v", err)
	}
}

func BenchmarkM065nRangeIncremental(b *testing.B) {
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
		window, err := NewIncrementalRangeWindow(m065mRangeDefinition(IncrementalRangeWindowMinInt64, 64, false))
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
