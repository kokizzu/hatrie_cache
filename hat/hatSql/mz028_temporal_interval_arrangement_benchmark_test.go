package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkMZ028IntervalArrangementUpsert(b *testing.B) {
	const workload = 256
	intervals := make([]SQLTemporalInterval, workload)
	for index := range intervals {
		intervals[index] = SQLTemporalInterval{
			ID:    "id-" + strconv.Itoa(index),
			Key:   "account",
			Start: int64(index),
			End:   int64(index + 1),
			Row:   Row{"id": int64(index)},
		}
	}
	var arrangement *SQLTemporalIntervalArrangement
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index%workload == 0 {
			b.StopTimer()
			var err error
			arrangement, err = NewSQLTemporalIntervalArrangement(SQLTemporalIntervalArrangementOptions{MaxIntervals: workload})
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		position := workload - 1 - index%workload
		if err := arrangement.Upsert(intervals[position]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ028IntervalArrangementAt(b *testing.B) {
	arrangement, err := NewSQLTemporalIntervalArrangement(SQLTemporalIntervalArrangementOptions{MaxIntervals: 256})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if err := arrangement.Upsert(SQLTemporalInterval{
			ID:    "id-" + strconv.Itoa(index),
			Key:   "account",
			Start: int64(index * 2),
			End:   int64(index*2 + 2),
			Row:   Row{"id": int64(index)},
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := arrangement.At("account", int64((index%256)*2))
		if err != nil || len(rows) != 1 {
			b.Fatalf("At() = %#v/%v, want one row", rows, err)
		}
	}
}

func BenchmarkMZ028BaselineLinearIntervalAt(b *testing.B) {
	intervals := benchmarkMZ028Intervals(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		at := int64((index % len(intervals)) * 2)
		matches := make([]SQLTemporalInterval, 0, 1)
		for _, interval := range intervals {
			if interval.Start <= at && at < interval.End {
				interval.Row = cloneSQLTemporalIntervalRow(interval.Row)
				matches = append(matches, interval)
			}
		}
		if len(matches) != 1 {
			b.Fatalf("linear scan matches = %d, want 1", len(matches))
		}
	}
}

func BenchmarkMZ028IntervalArrangementLargeAt(b *testing.B) {
	intervals := benchmarkMZ028Intervals(4096)
	arrangement, err := NewSQLTemporalIntervalArrangement(SQLTemporalIntervalArrangementOptions{MaxIntervals: len(intervals)})
	if err != nil {
		b.Fatal(err)
	}
	for _, interval := range intervals {
		if err := arrangement.Upsert(interval); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := arrangement.At("account", int64((index%len(intervals))*2))
		if err != nil || len(rows) != 1 {
			b.Fatalf("At() = %#v/%v, want one row", rows, err)
		}
	}
}

func benchmarkMZ028Intervals(count int) []SQLTemporalInterval {
	intervals := make([]SQLTemporalInterval, count)
	for index := range intervals {
		intervals[index] = SQLTemporalInterval{
			ID:    "id-" + strconv.Itoa(index),
			Key:   "account",
			Start: int64(index * 2),
			End:   int64(index*2 + 2),
			Row:   Row{"id": int64(index)},
		}
	}
	return intervals
}
