package hatSql_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableChangefeedAndExactAggregate(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("b", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(5)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("b"); err != nil {
		t.Fatal(err)
	}

	changes, last, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if last != 4 || len(changes) != 4 || changes[0].Sequence != 1 || changes[2].Before[0].String != "red" || changes[2].After[0].String != "blue" {
		t.Fatalf("changes = %#v, last = %d", changes, last)
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	rows := aggregate.Rows()
	if len(rows) != 1 || rows[0]["team"] != "blue" || rows[0]["count"] != int64(1) || rows[0]["sum"] != float64(5) {
		t.Fatalf("aggregate rows = %#v", rows)
	}
	if err := table.CompactChangesThrough(2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := table.ChangesAfter(0, 16); !errors.Is(err, hatSql.ErrTypedTableChangesCompacted) {
		t.Fatalf("ChangesAfter(0) error = %v, want compaction gap", err)
	}
	changes, last, err = table.ChangesAfter(2, 16)
	if err != nil || last != 4 || len(changes) != 2 || changes[0].Sequence != 3 {
		t.Fatalf("ChangesAfter(2) = %#v, %d, %v", changes, last, err)
	}
	rows, err = table.ResolveSQLSource("CACHE", "events")
	if err != nil || len(rows) != 1 || rows[0]["team"] != "blue" || rows[0]["points"] != int64(5) {
		t.Fatalf("ResolveSQLSource() = %#v, %v", rows, err)
	}
	batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", []string{"team", "points"})
	team, teamOK := batch.Value("team", 0)
	points, pointsOK := batch.Value("points", 0)
	if err != nil || !available || batch.Rows != 1 || !teamOK || team != "blue" || !pointsOK || points != int64(5) {
		t.Fatalf("ResolveSQLColumnarSource() = %#v, %t, %v", batch, available, err)
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team WHERE points = 5", table, nil, hatSql.QueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["team"] != "blue" {
		t.Fatalf("typed table SQL query = %#v, %v", result, err)
	}
}

func TestTypedTableRejectsSchemaAndValueMismatches(t *testing.T) {
	if _, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "events", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "team", Kind: hatSql.TypedTableInt64}}}); err == nil {
		t.Fatal("NewTypedTable() error = nil, want duplicate-column failure")
	}
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "events", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedInt64(1)}); err == nil {
		t.Fatal("Upsert() error = nil, want value-kind mismatch")
	}
}

func TestTypedTableColumnarLayoutCacheInvalidatesBeforeMutation(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:        true,
			MaxBytes:       1 << 20,
			MinReads:       2,
			RowsPerSegment: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key    string
		points int64
	}{{key: "a", points: 3}, {key: "b", points: 9}, {key: "c", points: 15}} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(row.points)}); err != nil {
			t.Fatal(err)
		}
	}
	fields := []string{"team", "points"}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields); err != nil || available {
		t.Fatalf("cold BorrowSQLColumnarSourceSegments() available = %t, error = %v", available, err)
	}
	if table.PreferSQLColumnarSource("CACHE", "events", fields) {
		t.Fatal("cold layout unexpectedly preferred")
	}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		t.Fatalf("first ResolveSQLColumnarSource() available = %t, error = %v", available, err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields); err != nil || available {
		t.Fatalf("single-read BorrowSQLColumnarSourceSegments() available = %t, error = %v", available, err)
	}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		t.Fatalf("second ResolveSQLColumnarSource() available = %t, error = %v", available, err)
	}
	batch, segments, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !available || segments == nil || segments.RowsPerSegment != 2 || len(segments.Columns["points"]) != 2 {
		t.Fatalf("warm BorrowSQLColumnarSourceSegments() = %#v, %#v, %t, %v", batch, segments, available, err)
	}
	bounds := segments.Columns["points"]
	if !bounds[0].Valid || bounds[0].Minimum != 3 || bounds[0].Maximum != 9 || !bounds[1].Valid || bounds[1].Minimum != 15 || bounds[1].Maximum != 15 {
		t.Fatalf("points segment bounds = %#v", bounds)
	}
	if !table.PreferSQLColumnarSource("CACHE", "events", fields) {
		t.Fatal("warm layout was not preferred")
	}
	beforeVersion, versioned, err := table.SQLSourceVersion("CACHE", "events")
	if err != nil || !versioned || beforeVersion != "3" {
		t.Fatalf("SQLSourceVersion() = %q, %t, %v", beforeVersion, versioned, err)
	}

	if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(21)}); err != nil {
		t.Fatal(err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields); err != nil || available {
		t.Fatalf("post-update BorrowSQLColumnarSourceSegments() available = %t, error = %v", available, err)
	}
	afterVersion, versioned, err := table.SQLSourceVersion("CACHE", "events")
	if err != nil || !versioned || afterVersion != "4" || afterVersion == beforeVersion {
		t.Fatalf("post-update SQLSourceVersion() = %q, %t, %v", afterVersion, versioned, err)
	}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		t.Fatalf("post-update ResolveSQLColumnarSource() available = %t, error = %v", available, err)
	}
	batch, available, err = table.ResolveSQLColumnarSource("CACHE", "events", fields)
	points, pointsOK := batch.Value("points", 0)
	if err != nil || !available || !pointsOK || points != int64(21) {
		t.Fatalf("refreshed ResolveSQLColumnarSource() = %#v, %t, %v", batch, available, err)
	}
}

func TestTypedTableAdaptiveSegmentsUseBoundedSmallerLayout(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "adaptive_events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:          true,
			MaxBytes:         1 << 20,
			MinReads:         2,
			RowsPerSegment:   256,
			AdaptiveSegments: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := table.Upsert(fmt.Sprintf("event-%d", index), []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(int64(index))}); err != nil {
			t.Fatal(err)
		}
	}
	fields := []string{"team", "points"}
	for range 2 {
		if _, available, err := table.ResolveSQLColumnarSource("CACHE", "adaptive_events", fields); err != nil || !available {
			t.Fatalf("ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	_, segments, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "adaptive_events", fields)
	if err != nil || !available || segments == nil || segments.RowsPerSegment != 32 {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = %#v, %t, %v", segments, available, err)
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('adaptive_events') SELECT points WHERE points >= 1000", table, nil, hatSql.QueryOptions{})
	if err != nil || len(result.Rows) != 24 || result.Rows[0]["points"] != int64(1000) {
		t.Fatalf("adaptive typed-table query = %#v, %v", result, err)
	}
}

func TestTypedTableColumnarLayoutCacheDefaultsOffAndHonorsByteBudget(t *testing.T) {
	newTable := func(options hatSql.TypedTableColumnarCacheOptions) *hatSql.TypedTable {
		table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
			Name: "events",
			Columns: []hatSql.TypedTableColumn{
				{Name: "team", Kind: hatSql.TypedTableString},
				{Name: "points", Kind: hatSql.TypedTableInt64},
			},
			ColumnarCache: options,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)}); err != nil {
			t.Fatal(err)
		}
		return table
	}
	fields := []string{"team", "points"}
	for _, test := range []struct {
		name    string
		options hatSql.TypedTableColumnarCacheOptions
	}{
		{name: "default off"},
		{name: "one byte cap", options: hatSql.TypedTableColumnarCacheOptions{Enabled: true, MaxBytes: 1, MinReads: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			table := newTable(test.options)
			for range 2 {
				if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
					t.Fatalf("ResolveSQLColumnarSource() available = %t, error = %v", available, err)
				}
			}
			if _, available, err := table.BorrowSQLColumnarSource("CACHE", "events", fields); err != nil || available {
				t.Fatalf("BorrowSQLColumnarSource() available = %t, error = %v", available, err)
			}
			if table.PreferSQLColumnarSource("CACHE", "events", fields) {
				t.Fatal("uncached layout unexpectedly preferred")
			}
			version, versioned, err := table.SQLSourceVersion("CACHE", "events")
			if err != nil || versioned != test.options.Enabled || versioned && version != "1" || !versioned && version != "" {
				t.Fatalf("SQLSourceVersion() = %q, %t, %v", version, versioned, err)
			}
		})
	}
}
