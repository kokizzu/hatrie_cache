package hatSql_test

import (
	"fmt"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkMZ038SortedArrangementOffsetPageBaseline(b *testing.B) {
	arrangement := mz038RangeBenchmarkArrangement(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := arrangement.RowsPage(9000, 32)
		if len(rows) != 32 {
			b.Fatalf("offset page rows = %d, want 32", len(rows))
		}
	}
}

func BenchmarkMZ038SortedArrangementFullScanBaseline(b *testing.B) {
	arrangement := mz038RangeBenchmarkArrangement(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := arrangement.Rows()
		matched := 0
		for _, row := range rows {
			value := row.Values[1].Int64
			if value >= 9000 && value < 9999 {
				matched++
			}
		}
		if matched != 999 {
			b.Fatalf("full scan matched %d rows, want 999", matched)
		}
	}
}

func BenchmarkMZ038SortedArrangementRowsRange(b *testing.B) {
	arrangement := mz038RangeBenchmarkArrangement(b)
	lower := &hatSql.TypedTableSortedArrangementBound{
		Values:    []hatSql.TypedTableValue{hatSql.TypedInt64(9000)},
		Inclusive: true,
	}
	upper := &hatSql.TypedTableSortedArrangementBound{
		Values:    []hatSql.TypedTableValue{hatSql.TypedInt64(9999)},
		Inclusive: false,
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := arrangement.RowsRange(lower, upper, 32)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != 32 {
			b.Fatalf("range rows = %d, want 32", len(rows))
		}
	}
}

func BenchmarkMZ038SortedArrangementRangePaginationBaseline(b *testing.B) {
	arrangement := mz038RangeBenchmarkArrangement(b)
	pages := mz038RangePaginationPages()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		total := 0
		for _, page := range pages {
			rows, err := arrangement.RowsRange(page.lower, page.upper, 32)
			if err != nil {
				b.Fatal(err)
			}
			total += len(rows)
		}
		if total != 999 {
			b.Fatalf("range pagination rows = %d, want 999", total)
		}
	}
}

func BenchmarkMZ038SortedArrangementRangePaginationCursor(b *testing.B) {
	arrangement := mz038RangeBenchmarkArrangement(b)
	lower := &hatSql.TypedTableSortedArrangementBound{
		Values:    []hatSql.TypedTableValue{hatSql.TypedInt64(9000)},
		Inclusive: true,
	}
	upper := &hatSql.TypedTableSortedArrangementBound{
		Values:    []hatSql.TypedTableValue{hatSql.TypedInt64(9999)},
		Inclusive: false,
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		cursor, err := arrangement.NewRowsRangeCursor(lower, upper)
		if err != nil {
			b.Fatal(err)
		}
		total := 0
		for {
			rows, done, err := cursor.NextPage(32)
			if err != nil {
				b.Fatal(err)
			}
			total += len(rows)
			if done {
				break
			}
		}
		if total != 999 {
			b.Fatalf("cursor pagination rows = %d, want 999", total)
		}
	}
}

type mz038RangePaginationPage struct {
	lower *hatSql.TypedTableSortedArrangementBound
	upper *hatSql.TypedTableSortedArrangementBound
}

func mz038RangePaginationPages() []mz038RangePaginationPage {
	pages := make([]mz038RangePaginationPage, 0, 32)
	for start := int64(9000); start < 9999; start += 32 {
		end := start + 32
		if end > 9999 {
			end = 9999
		}
		pages = append(pages, mz038RangePaginationPage{
			lower: &hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(start)}, Inclusive: true},
			upper: &hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(end)}, Inclusive: false},
		})
	}
	return pages
}

func mz038RangeBenchmarkArrangement(b *testing.B) *hatSql.TypedTableSortedArrangement {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "mz038_benchmark",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 10000; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%05d", index), []hatSql.TypedTableValue{
			hatSql.TypedString(fmt.Sprintf("team-%05d", index)),
			hatSql.TypedInt64(int64(index)),
		}); err != nil {
			b.Fatal(err)
		}
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "score"})
	if err != nil {
		b.Fatal(err)
	}
	return arrangement
}
