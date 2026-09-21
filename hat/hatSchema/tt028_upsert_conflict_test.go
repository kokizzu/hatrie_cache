package hatSchema

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMaterializedSourceUpsertConflictHandlerMergesAndMaintainsIndexes(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Indexed: true},
		{Name: "name", Indexed: true},
		{Name: "count"},
		{
			Name:    "total",
			Indexed: true,
			Generated: func(row Row) (interface{}, error) {
				count, ok := row["count"].(int64)
				if !ok {
					return nil, errors.New("count is not int64")
				}
				return count * 2, nil
			},
		},
	})
	if _, err := source.Insert(Row{"id": "item-1", "name": "old", "count": int64(2)}); err != nil {
		t.Fatalf("initial Insert() error = %v", err)
	}
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		t.Fatalf("BuildUniqueIndex() error = %v", err)
	}

	result, err := source.Upsert(Row{"id": "item-1", "name": "new", "count": int64(3)}, MaterializedUpsertOptions{
		ConflictField: "id",
		OnConflict: func(existing, incoming Row) (Row, error) {
			return Row{
				"id":    existing["id"],
				"name":  incoming["name"],
				"count": existing["count"].(int64) + incoming["count"].(int64),
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if !result.Updated || result.Inserted || !reflect.DeepEqual(result.Row, Row{"id": "item-1", "name": "new", "count": int64(5), "total": int64(10)}) {
		t.Fatalf("Upsert() result = %#v, want updated merged row", result)
	}
	if rows := source.Rows(); !reflect.DeepEqual(rows, []Row{{"id": "item-1", "name": "new", "count": int64(5), "total": int64(10)}}) {
		t.Fatalf("Rows() = %#v, want one merged row", rows)
	}
	if rows := source.Lookup("name", "old"); len(rows) != 0 {
		t.Fatalf("old name index = %#v, want empty", rows)
	}
	if rows := source.Lookup("name", "new"); len(rows) != 1 || rows[0]["total"] != int64(10) {
		t.Fatalf("new name index = %#v, want merged row", rows)
	}
	if rows := source.Lookup("total", int64(10)); len(rows) != 1 {
		t.Fatalf("total index = %#v, want one row", rows)
	}
}

func TestMaterializedSourceUpsertConflictHandlerErrorIsAtomic(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Indexed: true}, {Name: "value"}})
	if _, err := source.Insert(Row{"id": "item-1", "value": int64(1)}); err != nil {
		t.Fatalf("initial Insert() error = %v", err)
	}
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		t.Fatalf("BuildUniqueIndex() error = %v", err)
	}
	want := source.Rows()
	mergeErr := errors.New("reject merge")
	if _, err := source.Upsert(Row{"id": "item-1", "value": int64(2)}, MaterializedUpsertOptions{
		ConflictField: "id",
		OnConflict:    func(Row, Row) (Row, error) { return nil, mergeErr },
	}); !errors.Is(err, mergeErr) {
		t.Fatalf("Upsert() error = %v, want %v", err, mergeErr)
	}
	if got := source.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() after rejected merge = %#v, want %#v", got, want)
	}
}

func TestMaterializedSourceUpsertConflictMaintainsCoveringAndFunctionalIndexes(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Indexed: true},
		{Name: "name"},
		{Name: "score"},
	})
	if _, err := source.Insert(Row{"id": "item-1", "name": "old", "score": int64(1)}); err != nil {
		t.Fatalf("initial Insert() error = %v", err)
	}
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		t.Fatalf("BuildUniqueIndex() error = %v", err)
	}
	if _, err := source.BuildCoveringIndex("id", []string{"name", "score"}); err != nil {
		t.Fatalf("BuildCoveringIndex() error = %v", err)
	}
	if _, err := source.BuildFunctionalIndex("name_upper", []string{"name"}, func(row Row) (interface{}, error) {
		return strings.ToUpper(row["name"].(string)), nil
	}); err != nil {
		t.Fatalf("BuildFunctionalIndex() error = %v", err)
	}

	if _, err := source.Upsert(Row{"id": "item-1", "name": "new", "score": int64(9)}, MaterializedUpsertOptions{
		ConflictField: "id",
		OnConflict:    func(_, incoming Row) (Row, error) { return incoming, nil },
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if rows, available := source.LookupCovering("id", "item-1", []string{"name", "score"}); !available || !reflect.DeepEqual(rows, []Row{{"id": "item-1", "name": "new", "score": int64(9)}}) {
		t.Fatalf("covering lookup = %#v/%t, want updated projected row", rows, available)
	}
	if rows := source.Lookup("name_upper", "NEW"); len(rows) != 1 || rows[0]["name"] != "new" {
		t.Fatalf("functional lookup = %#v, want updated row", rows)
	}
}

func TestMaterializedSourceUpsertRejectsConflictKeyChangeAtomically(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Indexed: true}, {Name: "value"}})
	if _, err := source.Insert(Row{"id": "item-1", "value": int64(1)}); err != nil {
		t.Fatalf("initial Insert() error = %v", err)
	}
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		t.Fatalf("BuildUniqueIndex() error = %v", err)
	}
	if _, err := source.Upsert(Row{"id": "item-1", "value": int64(2)}, MaterializedUpsertOptions{
		ConflictField: "id",
		OnConflict: func(_, _ Row) (Row, error) {
			return Row{"id": "item-2", "value": int64(2)}, nil
		},
	}); !errors.Is(err, ErrMaterializedSourceConflictKeyChanged) {
		t.Fatalf("Upsert() error = %v, want conflict-key error", err)
	}
	if rows := source.Lookup("id", "item-1"); len(rows) != 1 || rows[0]["value"] != int64(1) {
		t.Fatalf("original unique index = %#v, want unchanged row", rows)
	}
	if rows := source.Lookup("id", "item-2"); len(rows) != 0 {
		t.Fatalf("changed unique index = %#v, want empty", rows)
	}
}
