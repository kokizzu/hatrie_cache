package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableCompressedColumnarCacheUsesPackedLayouts(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:           true,
			CompressedBatches: true,
			MaxBytes:          1 << 20,
			MinReads:          1,
			RowsPerSegment:    4,
			SparsePrimaryIndex: true,
			SparsePrimaryField: "points",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key    string
		team   string
		points int64
	}{
		{key: "a", team: "red", points: 10},
		{key: "b", team: "blue", points: 20},
		{key: "c", team: "red", points: 30},
		{key: "d", team: "blue", points: 40},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{
			hatSql.TypedString(row.team),
			hatSql.TypedInt64(row.points),
		}); err != nil {
			t.Fatal(err)
		}
	}

	fields := []string{"team", "points"}
	batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields)
	if err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() available = %t, error = %v", available, err)
	}
	if len(batch.Columns) != 0 {
		t.Fatalf("plain columns = %#v, want all scalar columns packed", batch.Columns)
	}
	if _, ok := batch.NumericColumns["points"]; !ok {
		t.Fatalf("numeric columns = %#v, want points", batch.NumericColumns)
	}
	dictionary, ok := batch.Dictionaries["team"]
	if !ok || dictionary.CodeWidth != 1 || len(dictionary.PackedCodes) != 4 || dictionary.Values != nil {
		t.Fatalf("team dictionary = %#v, want packed codes and values", dictionary)
	}
	_, segments, segmentsAvailable, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !segmentsAvailable || segments == nil {
		t.Fatalf("BorrowSQLColumnarSourceSegments() available = %t, segments = %#v, error = %v", segmentsAvailable, segments, err)
	}
	pointsBounds := segments.Columns["points"]
	if len(pointsBounds) != 1 || !pointsBounds[0].Valid || pointsBounds[0].Minimum != 10 || pointsBounds[0].Maximum != 40 || segments.SparsePrimaryField != "points" {
		t.Fatalf("compressed points segment bounds = %#v, want one 10..40 segment", pointsBounds)
	}
	for index, want := range []struct {
		team   string
		points int64
	}{{"red", 10}, {"blue", 20}, {"red", 30}, {"blue", 40}} {
		team, teamOK := batch.Value("team", index)
		points, pointsOK := batch.Value("points", index)
		if !teamOK || team != want.team || !pointsOK || points != want.points {
			t.Fatalf("batch row %d = %#v/%t, %#v/%t; want %#v, %d", index, team, teamOK, points, pointsOK, want.team, want.points)
		}
	}

	result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team, points WHERE points >= 20", table, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 || result.Rows[0]["team"] != "blue" || result.Rows[0]["points"] != int64(20) {
		t.Fatalf("compressed SQL result = %#v, want three matching rows", result.Rows)
	}
}

func TestTypedTableColumnarCacheCompressionDefaultsOff(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled: true,
			MaxBytes: 1 << 20,
			MinReads: 1,
			RowsPerSegment: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := range 4 {
		if _, err := table.Upsert(string(rune('a'+index)), []hatSql.TypedTableValue{hatSql.TypedInt64(int64(index))}); err != nil {
			t.Fatal(err)
		}
	}
	batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", []string{"points"})
	if err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() available = %t, error = %v", available, err)
	}
	if len(batch.Columns) != 1 || len(batch.NumericColumns) != 0 {
		t.Fatalf("default columnar layout = %#v, want legacy plain columns", batch)
	}
}
