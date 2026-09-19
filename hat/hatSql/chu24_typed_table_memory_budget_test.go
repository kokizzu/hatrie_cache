package hatSql

import (
	"errors"
	"testing"
)

func TestCHU24TypedTableMemoryBudgetRejectsOversizedUpsert(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableString},
		},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: 64},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("small", []TypedTableValue{TypedString("ok")}); err != nil {
		t.Fatalf("small Upsert() error = %v", err)
	}
	before := table.Rows()
	if _, err := table.Upsert("large", []TypedTableValue{TypedString("this value is larger than the remaining budget")}); !errors.Is(err, ErrTypedTableMemoryBudgetExceeded) {
		t.Fatalf("oversized Upsert() error = %v, want ErrTypedTableMemoryBudgetExceeded", err)
	}
	if got := table.Rows(); len(got) != len(before) || got[0]["value"] != before[0]["value"] {
		t.Fatalf("failed Upsert() changed rows: got %#v, before %#v", got, before)
	}
	usage := table.MemoryUsage()
	if usage.MaxBytes != 64 || usage.UsedBytes <= 0 || usage.AvailableBytes != usage.MaxBytes-usage.UsedBytes {
		t.Fatalf("MemoryUsage() = %#v, want bounded usage", usage)
	}
}

func TestCHU24TypedTableMemoryBudgetTracksPatchCompaction(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:         "events",
		Columns:      []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: 256},
		PatchParts:   TypedTablePatchOptions{Enabled: true, MergeThreshold: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("one", []TypedTableValue{TypedString("a")}); err != nil {
		t.Fatal(err)
	}
	initial := table.MemoryUsage()
	if _, err := table.Upsert("one", []TypedTableValue{TypedString("a much longer value")}); err != nil {
		t.Fatalf("replacement Upsert() error = %v", err)
	}
	updated := table.MemoryUsage()
	if updated.UsedBytes <= initial.UsedBytes {
		t.Fatalf("updated usage = %#v, want more than initial %#v", updated, initial)
	}
	if _, err := table.Delete("one"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	deleted := table.MemoryUsage()
	if deleted.UsedBytes != updated.UsedBytes {
		t.Fatalf("deleted usage = %#v, want physical row retained at %#v", deleted, updated)
	}
	if err := table.CompactPatchParts(); err != nil {
		t.Fatalf("CompactPatchParts() error = %v", err)
	}
	compacted := table.MemoryUsage()
	if compacted.UsedBytes != 0 || len(table.Rows()) != 0 {
		t.Fatalf("compacted usage/rows = %#v/%#v, want zero", compacted, table.Rows())
	}
}

func TestCHU24TypedTableMemoryBudgetAppendColumnarIsAtomic(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:         "events",
		Columns:      []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: 78},
	})
	if err != nil {
		t.Fatal(err)
	}
	batch := ColumnarBatch{Rows: 2, Columns: map[string][]interface{}{"value": {"first", "second"}}}
	if _, err := table.AppendColumnar([]string{"a", "b"}, batch); !errors.Is(err, ErrTypedTableMemoryBudgetExceeded) {
		t.Fatalf("AppendColumnar() error = %v, want ErrTypedTableMemoryBudgetExceeded", err)
	}
	if got := table.Rows(); len(got) != 0 || table.MemoryUsage().UsedBytes != 0 {
		t.Fatalf("failed AppendColumnar() changed table: rows=%#v usage=%#v", got, table.MemoryUsage())
	}
}

func TestCHU24TypedTableMemoryBudgetValidationAndDisabledPath(t *testing.T) {
	if _, err := NewTypedTable(TypedTableSchema{
		Name:         "invalid",
		Columns:      []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: -1},
	}); !errors.Is(err, ErrTypedTableMemoryBudgetInvalid) {
		t.Fatalf("negative budget error = %v, want ErrTypedTableMemoryBudgetInvalid", err)
	}
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "unlimited",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("key", []TypedTableValue{TypedString("value")}); err != nil {
		t.Fatal(err)
	}
	if usage := table.MemoryUsage(); usage != (TypedTableMemoryUsage{}) {
		t.Fatalf("disabled MemoryUsage() = %#v, want zero", usage)
	}
}
