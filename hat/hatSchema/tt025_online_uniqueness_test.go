package hatSchema

import (
	"errors"
	"testing"
)

func TestTT025BuildUniqueIndexRejectsExistingDuplicatesWithoutPublication(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "email"}})
	for _, row := range []Row{
		{"id": int64(1), "email": "same@example.test"},
		{"id": int64(2), "email": "same@example.test"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := source.BuildUniqueIndex("email"); !errors.Is(err, ErrMaterializedSourceUniqueIndexViolation) {
		t.Fatalf("BuildUniqueIndex() error = %v, want unique-index violation", err)
	}
	if source.HasIndex("email") {
		t.Fatal("failed unique-index validation published an index")
	}
	if got := len(source.Rows()); got != 2 {
		t.Fatalf("failed unique-index validation changed row count to %d", got)
	}
}

func TestTT025UniqueIndexRejectsFutureDuplicatesAtomically(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "email"}})
	for _, row := range []Row{
		{"id": int64(1), "email": "one@example.test"},
		{"id": int64(2), "email": "two@example.test"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}

	report, err := source.BuildUniqueIndex("email")
	if err != nil {
		t.Fatalf("BuildUniqueIndex() error = %v", err)
	}
	if report.Field != "email" || report.Rows != 2 || report.Attempts < 1 || !source.HasIndex("email") {
		t.Fatalf("BuildUniqueIndex() report/state = %#v/%t", report, source.HasIndex("email"))
	}

	if _, err := source.Insert(Row{"id": int64(3), "email": "one@example.test"}); !errors.Is(err, ErrMaterializedSourceUniqueIndexViolation) {
		t.Fatalf("duplicate Insert() error = %v, want unique-index violation", err)
	}
	if got := len(source.Rows()); got != 2 {
		t.Fatalf("duplicate Insert() changed row count to %d", got)
	}
	if _, err := source.Insert(Row{"id": int64(3), "email": "three@example.test"}); err != nil {
		t.Fatalf("distinct Insert() error = %v", err)
	}
}

func TestTT025UniqueIndexAllowsMultipleNullValues(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "email"}})
	if _, err := source.Insert(Row{"id": int64(1), "email": nil}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Insert(Row{"id": int64(2), "email": nil}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.BuildUniqueIndex("email"); err != nil {
		t.Fatalf("BuildUniqueIndex() with NULL values error = %v", err)
	}
	if _, err := source.Insert(Row{"id": int64(3), "email": nil}); err != nil {
		t.Fatalf("NULL Insert() after unique build error = %v", err)
	}
}
