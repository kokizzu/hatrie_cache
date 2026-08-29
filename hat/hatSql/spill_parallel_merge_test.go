package hatSql

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestSQLExternalSortParallelMergeMatchesSequential(t *testing.T) {
	records := sqlParallelMergeRecords(192)
	run := func(workers int) []SQLRow {
		t.Helper()
		control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{Workers: workers})
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		rows, _, _, err := sqlExternalSortRows(records, []sqlOrder{{expr: sqlExpr{kind: "field", name: "id"}}}, t.TempDir(), 1, 1<<20, 0, -1, control)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	sequential := run(1)
	parallel := run(4)
	if !reflect.DeepEqual(parallel, sequential) {
		t.Fatalf("parallel external sort = %#v, want sequential %#v", parallel, sequential)
	}
}

func TestSQLParallelSortMergeFallsBackWithoutExceedingBudget(t *testing.T) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	directory := t.TempDir()
	writeBudget := int64(1 << 20)
	runs := make([]sqlSpillRun, 64)
	for index := range runs {
		runs[index], err = sqlWriteSpillRun(directory, []sqlSpillOutput{{Row: SQLRow{"id": index}, Keys: []interface{}{index}, Ordinal: index}}, &writeBudget, control)
		if err != nil {
			t.Fatal(err)
		}
	}
	available := int64(1)
	next, used, err := sqlMergeSpillSortPassParallel(runs, []sqlOrder{{expr: sqlExpr{kind: "field", name: "id"}}}, directory, &available, control)
	if err != nil || used || next != nil || available != 1 {
		t.Fatalf("parallel merge fallback = %#v, used=%v, available=%d, err=%v", next, used, available, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(runs) {
		t.Fatalf("spill files after fallback = %d, want %d original runs", len(entries), len(runs))
	}
}

func TestSQLParallelGroupMergeProducesRunsAndFallsBackWithinBudget(t *testing.T) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	directory := t.TempDir()
	writeBudget := int64(1 << 20)
	runs := make([]sqlSpillGroupRun, 64)
	for index := range runs {
		runs[index], err = sqlWriteSpillGroupRun(directory, []sqlSpillGroupRecord{{Key: fmt.Sprintf("%03d", index), Value: index, Ordinal: index}}, &writeBudget, control)
		if err != nil {
			t.Fatal(err)
		}
	}
	available := int64(1 << 20)
	next, used, err := sqlMergeSpillGroupPassParallel(runs, sqlOrder{}, directory, &available, control)
	if err != nil || !used || len(next) != 2 {
		t.Fatalf("parallel group merge = %#v, used=%v, err=%v", next, used, err)
	}
	for _, run := range next {
		if err := os.Remove(run.path); err != nil {
			t.Fatal(err)
		}
	}
	available = 1
	next, used, err = sqlMergeSpillGroupPassParallel(runs, sqlOrder{}, directory, &available, control)
	if err != nil || used || next != nil || available != 1 {
		t.Fatalf("parallel group fallback = %#v, used=%v, available=%d, err=%v", next, used, available, err)
	}
}

func BenchmarkSQLExternalSortParallelMerge(b *testing.B) {
	records := sqlParallelMergeRecords(768)
	order := []sqlOrder{{expr: sqlExpr{kind: "field", name: "id"}}}
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("Workers%d", workers), func(b *testing.B) {
			control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{Workers: workers})
			if err != nil {
				b.Fatal(err)
			}
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, _, _, err := sqlExternalSortRows(records, order, b.TempDir(), 1, 8<<20, 0, -1, control); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSQLSpillGroupParallelMerge(b *testing.B) {
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("Workers%d", workers), func(b *testing.B) {
			control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{Workers: workers})
			if err != nil {
				b.Fatal(err)
			}
			defer cancel()
			directory := b.TempDir()
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				available := int64(8 << 20)
				runs := sqlParallelGroupRuns(b, directory, &available, control)
				b.StartTimer()
				var merged []sqlSpillGroupRun
				if workers == 1 {
					merged = make([]sqlSpillGroupRun, 0, 2)
					for start := 0; start < len(runs); start += maxSQLSpillMergeFanIn {
						end := start + maxSQLSpillMergeFanIn
						if end > len(runs) {
							end = len(runs)
						}
						run, err := sqlMergeSpillGroupRunsToRun(runs[start:end], sqlOrder{}, directory, &available, control)
						if err != nil {
							b.Fatal(err)
						}
						merged = append(merged, run)
					}
				} else {
					var used bool
					merged, used, err = sqlMergeSpillGroupPassParallel(runs, sqlOrder{}, directory, &available, control)
					if err != nil || !used {
						b.Fatalf("parallel group merge = %#v, used=%v, err=%v", merged, used, err)
					}
				}
				b.StopTimer()
				for _, run := range runs {
					_ = os.Remove(run.path)
				}
				for _, run := range merged {
					_ = os.Remove(run.path)
				}
				b.StartTimer()
			}
		})
	}
}

func sqlParallelMergeRecords(count int) []sqlSpillOutput {
	records := make([]sqlSpillOutput, count)
	for index := range records {
		id := count - index
		records[index] = sqlSpillOutput{Row: SQLRow{"id": id}, Keys: []interface{}{id}, Ordinal: index}
	}
	return records
}

func sqlParallelGroupRuns(b *testing.B, directory string, available *int64, control *sqlExecutionControl) []sqlSpillGroupRun {
	b.Helper()
	runs := make([]sqlSpillGroupRun, 64)
	for index := range runs {
		var err error
		runs[index], err = sqlWriteSpillGroupRun(directory, []sqlSpillGroupRecord{{Key: fmt.Sprintf("%03d", index), Value: index, Ordinal: index}}, available, control)
		if err != nil {
			b.Fatal(err)
		}
	}
	return runs
}
