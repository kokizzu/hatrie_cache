package hatSql

import "testing"

func TestC212TypedTableColumnarOrderIsAdmittedAndInvalidated(t *testing.T) {
	table := newC212TypedTableOrderTestTable(t)
	fields := []string{"id", "score"}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() available = %t, error = %v", available, err)
	}
	layoutKey := typedTableColumnarLayoutKey(fields)
	table.columnar.mu.Lock()
	baseBytes := table.columnar.layouts[layoutKey].bytes
	table.columnar.mu.Unlock()
	for attempt := 0; attempt < 7; attempt++ {
		if _, available, err := table.BorrowSQLColumnarSourceOrder("CACHE", "events", fields, "score"); err != nil || available {
			t.Fatalf("cold order attempt %d available = %t, error = %v", attempt, available, err)
		}
	}
	order, available, err := table.BorrowSQLColumnarSourceOrder("CACHE", "events", fields, "score")
	if err != nil || !available {
		t.Fatalf("admitted order available = %t, error = %v", available, err)
	}
	batch, available, err := table.BorrowSQLColumnarSource("CACHE", "events", fields)
	if err != nil || !available {
		t.Fatalf("BorrowSQLColumnarSource() available = %t, error = %v", available, err)
	}
	for index := 1; index < len(order); index++ {
		left, leftAvailable := batch.Value("score", int(order[index-1]))
		right, rightAvailable := batch.Value("score", int(order[index]))
		if !leftAvailable || !rightAvailable || left.(int64) > right.(int64) {
			t.Fatalf("order[%d:%d] = %v/%v, want ascending scores", index-1, index, left, right)
		}
	}
	table.columnar.mu.Lock()
	layout := table.columnar.layouts[layoutKey]
	retainedOrderBytes := layout.bytes - baseBytes
	cacheBytes := table.columnar.bytes
	maxBytes := table.columnar.options.MaxBytes
	table.columnar.mu.Unlock()
	if retainedOrderBytes != len(order)*4 {
		t.Fatalf("retained order bytes = %d, want %d", retainedOrderBytes, len(order)*4)
	}
	if cacheBytes > maxBytes {
		t.Fatalf("columnar cache bytes = %d, max %d", cacheBytes, maxBytes)
	}
	if _, err := table.Upsert("row-1", []TypedTableValue{TypedInt64(1), TypedInt64(-1)}); err != nil {
		t.Fatal(err)
	}
	if _, available, err := table.BorrowSQLColumnarSourceOrder("CACHE", "events", fields, "score"); err != nil || available {
		t.Fatalf("invalidated order available = %t, error = %v", available, err)
	}
}

func TestC212TypedTableColumnarOrderDisabledByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "score", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("row-1", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, available, err := table.BorrowSQLColumnarSourceOrder("CACHE", "events", []string{"score"}, "score"); err != nil || available {
		t.Fatalf("default order available = %t, error = %v", available, err)
	}
}

func newC212TypedTableOrderTestTable(t testing.TB) *TypedTable {
	testingTable, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "id", Kind: TypedTableInt64},
			{Name: "score", Kind: TypedTableInt64},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:          true,
			SortedOrderCache: true,
			MaxBytes:         1 << 20,
			MinReads:         1,
			RowsPerSegment:   256,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		id    int64
		score int64
	}{
		{key: "row-1", id: 1, score: 30},
		{key: "row-2", id: 2, score: 10},
		{key: "row-3", id: 3, score: 20},
	} {
		if _, err := testingTable.Upsert(row.key, []TypedTableValue{TypedInt64(row.id), TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	return testingTable
}

func TestC212TypedTableColumnarOrderServesSQLRows(t *testing.T) {
	table := newC212TypedTableOrderTestTable(t)
	query := "FROM CACHE('events') AS item SELECT item.id, item.score ORDER BY item.score ASC LIMIT 2"
	for attempt := 0; attempt < 8; attempt++ {
		result, err := ExecuteSQLQuery(query, table)
		if err != nil || len(result.Rows) != 2 {
			t.Fatalf("warm-up query %d = %#v, %v", attempt, result, err)
		}
	}
	result, err := ExecuteSQLQuery(query, table)
	if err != nil {
		t.Fatal(err)
	}
	if got := []interface{}{result.Rows[0]["id"], result.Rows[1]["id"]}; got[0] != int64(2) || got[1] != int64(3) {
		t.Fatalf("SQL ordered rows = %#v, want [2 3]", got)
	}
}

func TestC212TypedTableColumnarOrderSurvivesResolverWrappers(t *testing.T) {
	query := "FROM CACHE('events') AS item SELECT item.id, item.score ORDER BY item.score ASC LIMIT 2"
	resolvers := []struct {
		name     string
		resolver SourceResolver
	}{
		{name: "session", resolver: NewSQLSession(newC212TypedTableOrderTestTable(t))},
		{name: "catalog", resolver: CatalogResolver{Source: newC212TypedTableOrderTestTable(t)}},
	}
	for _, test := range resolvers {
		t.Run(test.name, func(t *testing.T) {
			columnar, ok := test.resolver.(ColumnarSourceResolver)
			if !ok {
				t.Fatalf("resolver does not forward ColumnarSourceResolver")
			}
			if _, available, err := columnar.ResolveSQLColumnarSource("CACHE", "events", []string{"id", "score"}); err != nil || !available {
				t.Fatalf("columnar source available = %t, error = %v", available, err)
			}
			sorted, ok := test.resolver.(SortedColumnarSourceResolver)
			if !ok {
				t.Fatalf("resolver does not forward SortedColumnarSourceResolver")
			}
			for attempt := 0; attempt < 7; attempt++ {
				if _, available, err := sorted.BorrowSQLColumnarSourceOrder("CACHE", "events", []string{"id", "score"}, "score"); err != nil || available {
					t.Fatalf("warm-up %d available = %t, error = %v", attempt, available, err)
				}
			}
			order, available, err := sorted.BorrowSQLColumnarSourceOrder("CACHE", "events", []string{"id", "score"}, "score")
			if err != nil || !available || len(order) != 3 {
				t.Fatalf("admitted order = %#v, available = %t, error = %v", order, available, err)
			}
			result, err := ExecuteSQLQuery(query, test.resolver)
			if err != nil {
				t.Fatal(err)
			}
			if got := []interface{}{result.Rows[0]["id"], result.Rows[1]["id"]}; got[0] != int64(2) || got[1] != int64(3) {
				t.Fatalf("SQL ordered rows = %#v, want [2 3]", got)
			}
		})
	}
}

func TestC212TypedTableColumnarOrderObservationEviction(t *testing.T) {
	table := newC212TypedTableOrderTestTable(t)
	fieldsA := []string{"id", "score"}
	fieldsB := []string{"score", "id"}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fieldsA); err != nil || !available {
		t.Fatalf("layout A available = %t, error = %v", available, err)
	}
	for attempt := 0; attempt < typedTableColumnarOrderCacheMinReads-1; attempt++ {
		if _, available, err := table.BorrowSQLColumnarSourceOrder("CACHE", "events", fieldsA, "score"); err != nil || available {
			t.Fatalf("layout A warm-up %d available = %t, error = %v", attempt, available, err)
		}
	}
	table.columnar.mu.Lock()
	layoutBytes := table.columnar.layouts[typedTableColumnarLayoutKey(fieldsA)].bytes
	table.columnar.options.MaxBytes = layoutBytes + 3*4
	table.columnar.mu.Unlock()
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fieldsB); err != nil || !available {
		t.Fatalf("layout B available = %t, error = %v", available, err)
	}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fieldsA); err != nil || !available {
		t.Fatalf("recreated layout A available = %t, error = %v", available, err)
	}
	if _, available, err := table.BorrowSQLColumnarSourceOrder("CACHE", "events", fieldsA, "score"); err != nil || available {
		t.Fatalf("recreated layout A first order available = %t, error = %v; want observation reset", available, err)
	}
}
