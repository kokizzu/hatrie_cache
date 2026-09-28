package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func m038SQLDistinctRow(bucket int64) Row {
	row := make(Row, 1)
	row["bucket"] = bucket
	return row
}

func m038SQLDistinctActiveRow(active bool, bucket int64) Row {
	row := make(Row, 2)
	row["active"] = active
	row["bucket"] = bucket
	return row
}

func m038SQLDistinctValueRow(value interface{}) Row {
	row := make(Row, 1)
	row["value"] = value
	return row
}

func TestM038SQLIncrementalDistinctMaintainsProjectedSet(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('events') SELECT DISTINCT bucket")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	operator, err := compiled.CompileIncrementalDistinct()
	if err != nil {
		t.Fatalf("CompileIncrementalDistinct() error = %v", err)
	}

	changes, err := operator.Apply([]DifferentialRow{
		{Key: "row-1", Time: 1, Diff: 1, Row: m038SQLDistinctRow(7)},
		{Key: "row-2", Time: 2, Diff: 1, Row: m038SQLDistinctRow(7)},
		{Key: "row-3", Time: 3, Diff: 1, Row: m038SQLDistinctRow(8)},
	})
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("initial changes = %#v, want one row per projected value", changes)
	}
	if changes[0].Diff != 1 || changes[0].Time != 1 || !reflect.DeepEqual(changes[0].Row, m038SQLDistinctRow(7)) {
		t.Fatalf("first distinct change = %#v, want bucket 7 at time 1", changes[0])
	}
	if changes[1].Diff != 1 || changes[1].Time != 3 || !reflect.DeepEqual(changes[1].Row, m038SQLDistinctRow(8)) {
		t.Fatalf("second distinct change = %#v, want bucket 8 at time 3", changes[1])
	}
	if changes[0].Key == "" || changes[1].Key == "" || changes[0].Key == changes[1].Key {
		t.Fatalf("distinct output keys = %#v, want stable unique keys", changes)
	}

	if changes, err = operator.Apply([]DifferentialRow{{Key: "row-1", Time: 4, Diff: -1, Row: m038SQLDistinctRow(7)}}); err != nil || changes != nil {
		t.Fatalf("retract duplicate row = %#v, %v; want no visible change", changes, err)
	}
	changes, err = operator.Apply([]DifferentialRow{{Key: "row-2", Time: 5, Diff: -1, Row: m038SQLDistinctRow(7)}})
	if err != nil {
		t.Fatalf("retract final row error = %v", err)
	}
	if len(changes) != 1 || changes[0].Diff != -1 || changes[0].Time != 5 || !reflect.DeepEqual(changes[0].Row, m038SQLDistinctRow(7)) {
		t.Fatalf("retract final row = %#v, want one negative bucket-7 change", changes)
	}

	filtered, err := CompileSQLQuery("FROM CACHE('events') WHERE active = true SELECT DISTINCT bucket")
	if err != nil {
		t.Fatalf("filtered CompileSQLQuery() error = %v", err)
	}
	filteredOperator, err := filtered.CompileIncrementalDistinct()
	if err != nil {
		t.Fatalf("filtered CompileIncrementalDistinct() error = %v", err)
	}
	changes, err = filteredOperator.Apply([]DifferentialRow{
		{Key: "filtered-out", Diff: 1, Row: m038SQLDistinctActiveRow(false, 9)},
		{Key: "kept", Diff: 1, Row: m038SQLDistinctActiveRow(true, 9)},
	})
	if err != nil {
		t.Fatalf("filtered Apply() error = %v", err)
	}
	if len(changes) != 1 || !reflect.DeepEqual(changes[0].Row, m038SQLDistinctRow(9)) {
		t.Fatalf("filtered changes = %#v, want one bucket-9 row", changes)
	}
}

func TestM038SQLIncrementalDistinctRejectsUnsupportedShapes(t *testing.T) {
	for _, query := range []string{
		"FROM CACHE('events') SELECT bucket",
		"FROM CACHE('events') SELECT DISTINCT bucket ORDER BY bucket",
		"FROM CACHE('events') SELECT DISTINCT bucket LIMIT 1",
		"FROM EXTERNAL('events') SELECT DISTINCT bucket",
	} {
		compiled, err := CompileSQLQuery(query)
		if err != nil {
			t.Fatalf("CompileSQLQuery(%q) error = %v", query, err)
		}
		if _, err := compiled.CompileIncrementalDistinct(); !errors.Is(err, ErrSQLIncrementalDistinctUnsupported) {
			t.Errorf("CompileIncrementalDistinct(%q) error = %v, want %v", query, err, ErrSQLIncrementalDistinctUnsupported)
		}
	}
}

func TestM038SQLIncrementalDistinctIsAtomicAndPreservesValueTypes(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('events') SELECT DISTINCT value")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	operator, err := compiled.CompileIncrementalDistinct()
	if err != nil {
		t.Fatalf("CompileIncrementalDistinct() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{{Key: "seed", Diff: 1, Row: m038SQLDistinctValueRow("seed")}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before := operator.Snapshot()
	_, err = operator.Apply([]DifferentialRow{
		{Key: "valid", Diff: 1, Row: m038SQLDistinctValueRow("new")},
		{Key: "unsupported", Diff: 1, Row: m038SQLDistinctValueRow(func() {})},
	})
	if err == nil {
		t.Fatal("unsupported value Apply() error = nil, want canonicalization error")
	}
	if after := operator.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("failed batch changed state: before=%#v after=%#v", before, after)
	}

	changes, err := operator.Apply([]DifferentialRow{
		{Key: "bytes", Diff: 1, Row: m038SQLDistinctValueRow([]byte("a"))},
		{Key: "string", Diff: 1, Row: m038SQLDistinctValueRow("YQ==")},
	})
	if err != nil {
		t.Fatalf("byte/string Apply() error = %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("byte/string changes = %#v, want two distinct rows", changes)
	}
}
