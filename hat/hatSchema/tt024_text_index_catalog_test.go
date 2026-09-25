package hatSchema

import (
	"context"
	"errors"
	"testing"
)

type tt024TextIndexCatalogMemory struct {
	frames map[string][]byte
	puts   int
	gets   int
}

func (catalog *tt024TextIndexCatalogMemory) PutTextIndex(_ context.Context, sourceKey, field string, frame []byte) error {
	if catalog.frames == nil {
		catalog.frames = make(map[string][]byte)
	}
	catalog.frames[sourceKey+"\x00"+field] = append([]byte(nil), frame...)
	catalog.puts++
	return nil
}

func (catalog *tt024TextIndexCatalogMemory) GetTextIndex(_ context.Context, sourceKey, field string) ([]byte, error) {
	catalog.gets++
	frame, ok := catalog.frames[sourceKey+"\x00"+field]
	if !ok {
		return nil, errors.New("missing text index")
	}
	return append([]byte(nil), frame...), nil
}

func TestTT024TextIndexCatalogRoundTripAndContext(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "body": "alpha beta gamma"},
		{"id": int64(2), "body": "unrelated document"},
	}
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for _, row := range rows {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		t.Fatal(err)
	}
	catalog := &tt024TextIndexCatalogMemory{}
	if err := source.PersistTextIndex(context.Background(), catalog, "docs", "body"); err != nil {
		t.Fatalf("PersistTextIndex() error = %v", err)
	}
	if catalog.puts != 1 {
		t.Fatalf("catalog puts = %d, want 1", catalog.puts)
	}

	restored := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for _, row := range rows {
		if _, err := restored.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.RestoreTextIndexFromCatalog(context.Background(), catalog, "docs", "body"); err != nil {
		t.Fatalf("RestoreTextIndexFromCatalog() error = %v", err)
	}
	if catalog.gets != 1 || !restored.HasTextIndex("body") {
		t.Fatalf("catalog gets/index state = %d/%t, want 1/true", catalog.gets, restored.HasTextIndex("body"))
	}
	if got := restored.LookupText("body", "alpha beta", 0); len(got) != 1 || got[0]["id"] != int64(1) {
		t.Fatalf("restored lookup = %#v, want row 1", got)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := source.PersistTextIndex(canceled, catalog, "docs", "body"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled PersistTextIndex() error = %v, want context.Canceled", err)
	}
	if err := restored.RestoreTextIndexFromCatalog(canceled, catalog, "docs", "body"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled RestoreTextIndexFromCatalog() error = %v, want context.Canceled", err)
	}
}

func TestTT024TextIndexCatalogRejectsInvalidInputs(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "body"}})
	catalog := &tt024TextIndexCatalogMemory{}
	for name, run := range map[string]func() error{
		"nil catalog persist": func() error {
			return source.PersistTextIndex(context.Background(), nil, "docs", "body")
		},
		"empty source key": func() error {
			return source.PersistTextIndex(context.Background(), catalog, " ", "body")
		},
		"empty field": func() error {
			return source.RestoreTextIndexFromCatalog(context.Background(), catalog, "docs", " ")
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil {
				t.Fatal("invalid catalog input returned nil error")
			}
		})
	}
}
