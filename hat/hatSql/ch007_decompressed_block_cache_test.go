package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableDecompressedBlockCachePreservesPackedValues(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "active", Kind: hatSql.TypedTableBool},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:                   true,
			CompressedBatches:         true,
			MaxBytes:                  1 << 20,
			MinReads:                  1,
			DecompressedBlockCache:    true,
			DecompressedBlockMaxBytes: 1 << 20,
			DecompressedBlockRows:     2,
			DecompressedBlockMinReads: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		key    string
		team   string
		points int64
		active bool
	}{
		{key: "a", team: "red", points: 10, active: true},
		{key: "b", team: "blue", points: 20, active: false},
		{key: "c", team: "red", points: 30, active: true},
		{key: "d", team: "blue", points: 40, active: false},
	}
	for _, row := range rows {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{
			hatSql.TypedString(row.team),
			hatSql.TypedInt64(row.points),
			hatSql.TypedBool(row.active),
		}); err != nil {
			t.Fatal(err)
		}
	}

	fields := []string{"team", "points", "active"}
	for read := 0; read < 3; read++ {
		batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields)
		if err != nil || !available {
			t.Fatalf("ResolveSQLColumnarSource() = available %t, error %v", available, err)
		}
		for index, want := range rows {
			team, teamOK := batch.Value("team", index)
			points, pointsOK := batch.Value("points", index)
			active, activeOK := batch.Value("active", index)
			if !teamOK || team != want.team || !pointsOK || points != want.points || !activeOK || active != want.active {
				t.Fatalf("row %d = %#v/%t, %#v/%t, %#v/%t; want %q, %d, %t", index, team, teamOK, points, pointsOK, active, activeOK, want.team, want.points, want.active)
			}
		}
	}
}

func TestTypedTableDecompressedBlockCacheIsOptIn(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "events-disabled",
		Columns: []hatSql.TypedTableColumn{{Name: "points", Kind: hatSql.TypedTableInt64}},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:           true,
			CompressedBatches: true,
			MinReads:          1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if options := table.Schema().ColumnarCache; options.DecompressedBlockCache || options.DecompressedBlockMaxBytes != 0 || options.DecompressedBlockRows != 0 || options.DecompressedBlockMinReads != 0 {
		t.Fatalf("default decompressed block options = %+v, want disabled", options)
	}
}

func TestTypedTableDecompressedBlockCacheUsesSaneEnabledDefaults(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "events-defaults",
		Columns: []hatSql.TypedTableColumn{{Name: "points", Kind: hatSql.TypedTableInt64}},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:                true,
			CompressedBatches:      true,
			DecompressedBlockCache: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	options := table.Schema().ColumnarCache
	if options.DecompressedBlockMaxBytes != 1<<20 || options.DecompressedBlockRows != 256 || options.DecompressedBlockMinReads != 2 {
		t.Fatalf("enabled decompressed block defaults = %+v; want 1 MiB, 256 rows, 2 reads", options)
	}

	table, err = hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "events-uncompressed",
		Columns: []hatSql.TypedTableColumn{{Name: "points", Kind: hatSql.TypedTableInt64}},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:                   true,
			DecompressedBlockCache:    true,
			DecompressedBlockMaxBytes: 123,
			DecompressedBlockRows:     7,
			DecompressedBlockMinReads: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if options := table.Schema().ColumnarCache; options.DecompressedBlockCache || options.DecompressedBlockMaxBytes != 0 || options.DecompressedBlockRows != 0 || options.DecompressedBlockMinReads != 0 {
		t.Fatalf("uncompressed decompressed block options = %+v; want disabled", options)
	}
}
