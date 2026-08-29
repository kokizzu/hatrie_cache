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

func sqlParallelMergeRecords(count int) []sqlSpillOutput {
	records := make([]sqlSpillOutput, count)
	for index := range records {
		id := count - index
		records[index] = sqlSpillOutput{Row: SQLRow{"id": id}, Keys: []interface{}{id}, Ordinal: index}
	}
	return records
}
