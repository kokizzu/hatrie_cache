package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM038DifferentialDistinctSmallBatchMatchesDistinctSemantics(t *testing.T) {
	tests := []struct {
		name string
		rows []DifferentialRow
		want []DifferentialRow
	}{
		{
			name: "enter",
			rows: []DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "first"}}},
			want: []DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "first"}}},
		},
		{
			name: "enter-and-leave",
			rows: []DifferentialRow{
				{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "first"}},
				{Key: "a", Time: 2, Diff: -1, Row: Row{"value": "last"}},
			},
			want: []DifferentialRow{
				{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "first"}},
				{Key: "a", Time: 2, Diff: -1, Row: Row{"value": "last"}},
			},
		},
		{
			name: "two-keys",
			rows: []DifferentialRow{
				{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "a"}},
				{Key: "b", Time: 1, Diff: 2, Row: Row{"value": "b"}},
			},
			want: []DifferentialRow{
				{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "a"}},
				{Key: "b", Time: 1, Diff: 1, Row: Row{"value": "b"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, handled, err := smallDifferentialDistinct(test.rows)
			if err != nil {
				t.Fatalf("smallDifferentialDistinct() error = %v", err)
			}
			if !handled {
				t.Fatal("smallDifferentialDistinct() handled = false, want true")
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("smallDifferentialDistinct() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestM038DifferentialDistinctSmallBatchRejectsNegativeMultiplicity(t *testing.T) {
	got, handled, err := smallDifferentialDistinct([]DifferentialRow{{Key: "missing", Diff: -1}})
	if !handled {
		t.Fatal("smallDifferentialDistinct() handled = false, want true")
	}
	if !errors.Is(err, ErrDifferentialDistinctNegativeMultiplicity) {
		t.Fatalf("smallDifferentialDistinct() error = %v, want negative multiplicity", err)
	}
	if got != nil {
		t.Fatalf("smallDifferentialDistinct() result = %#v, want nil", got)
	}
}

func TestM038DifferentialDistinctSmallBatchFallsBackForLargerBatches(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "a", Diff: 1},
		{Key: "b", Diff: 1},
		{Key: "c", Diff: 1},
	}
	got, handled, err := smallDifferentialDistinct(rows)
	if err != nil {
		t.Fatalf("smallDifferentialDistinct() error = %v", err)
	}
	if handled || got != nil {
		t.Fatalf("smallDifferentialDistinct() = %#v, %v, want nil, false", got, handled)
	}
}

var benchmarkM038DistinctSmallBatchSink []DifferentialRow

func BenchmarkM038DifferentialDistinctSmallBatch(b *testing.B) {
	for _, test := range []struct {
		name string
		rows []DifferentialRow
	}{
		{
			name: "one-row",
			rows: []DifferentialRow{{Key: "key", Time: 1, Diff: 1, Row: Row{"value": "payload"}}},
		},
		{
			name: "one-row-nil-payload",
			rows: []DifferentialRow{{Key: "key", Time: 1, Diff: 1}},
		},
		{
			name: "two-keys",
			rows: []DifferentialRow{
				{Key: "key-a", Time: 1, Diff: 1},
				{Key: "key-b", Time: 1, Diff: 1},
			},
		},
	} {
		test := test
		b.Run(test.name+"/map-baseline", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				result, err := distinctDifferentialRowsMapBaseline(test.rows)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkM038DistinctSmallBatchSink = result
			}
		})
		b.Run(test.name+"/slice-path", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				result, err := DistinctDifferentialRows(test.rows)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkM038DistinctSmallBatchSink = result
			}
		})
	}
}

func distinctDifferentialRowsMapBaseline(rows []DifferentialRow) ([]DifferentialRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	multiplicity := make(map[string]uint64, len(rows))
	emitted := make([]DifferentialRow, 0, len(rows))
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		current := multiplicity[update.Key]
		next, err := nextDifferentialDistinctMultiplicity(current, update.Diff)
		if err != nil {
			return nil, err
		}
		if next == 0 {
			delete(multiplicity, update.Key)
		} else {
			multiplicity[update.Key] = next
		}
		switch {
		case current == 0 && next > 0:
			emitted = append(emitted, DifferentialRow{Key: update.Key, Time: update.Time, Diff: 1, Row: cloneDifferentialDistinctRow(update.Row)})
		case current > 0 && next == 0:
			emitted = append(emitted, DifferentialRow{Key: update.Key, Time: update.Time, Diff: -1, Row: cloneDifferentialDistinctRow(update.Row)})
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}
