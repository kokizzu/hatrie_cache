package hatSql

import (
	"fmt"
	"testing"
)

func TestCH012DeleteBitmapPacksLogicalDeletes(t *testing.T) {
	bitmap := newTypedTableDeleteBitmap(17)
	if got := len(bitmap.words); got != 1 {
		t.Fatalf("newTypedTableDeleteBitmap(17) words = %d, want 1", got)
	}
	if bitmap.contains(0) || bitmap.contains(16) {
		t.Fatal("new bitmap contains a deleted row")
	}

	bitmap.set(0)
	bitmap.set(16)
	if !bitmap.contains(0) || !bitmap.contains(16) || bitmap.contains(1) {
		t.Fatalf("bitmap membership = [%t %t %t], want [true true false]", bitmap.contains(0), bitmap.contains(16), bitmap.contains(1))
	}

	bitmap.clear(0)
	if bitmap.contains(0) || !bitmap.contains(16) {
		t.Fatal("bitmap clear changed the wrong rows")
	}

	bitmap.truncate(8)
	if len(bitmap.words) != 1 || bitmap.contains(16) {
		t.Fatalf("truncated bitmap = %#v, want one word without row 16", bitmap.words)
	}
	bitmap.set(7)
	if !bitmap.contains(7) {
		t.Fatal("bitmap could not set the last retained row")
	}
}

func TestCH012TypedTableLogicalDeletesUseBitmap(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "bitmap-events",
		PatchParts: TypedTablePatchOptions{
			Enabled:        true,
			MergeThreshold: 1000,
		},
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 130; index++ {
		key := fmt.Sprintf("k-%03d", index)
		if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			t.Fatalf("Upsert(%q): %v", key, err)
		}
	}
	for _, index := range []int{0, 64, 129} {
		if _, err := table.Delete(fmt.Sprintf("k-%03d", index)); err != nil {
			t.Fatalf("Delete(%d): %v", index, err)
		}
	}
	if got := len(table.patchParts.deleted.words); got != 3 {
		t.Fatalf("delete bitmap words = %d, want 3 for 130 rows", got)
	}
	if table.patchParts.deletedCount != 3 {
		t.Fatalf("deleted count = %d, want 3", table.patchParts.deletedCount)
	}
	if !table.patchParts.deleted.contains(0) || !table.patchParts.deleted.contains(64) || !table.patchParts.deleted.contains(129) {
		t.Fatal("bitmap is missing one of the deleted rows")
	}

	if _, err := table.Upsert("k-064", []TypedTableValue{TypedInt64(6400)}); err != nil {
		t.Fatal(err)
	}
	if table.patchParts.deleted.contains(64) || table.patchParts.deletedCount != 2 {
		t.Fatal("reinsert did not clear exactly one tombstone")
	}
	if err := table.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	if len(table.keys) != 128 || table.patchParts.deletedCount != 0 || len(table.patchParts.deleted.words) != 2 {
		t.Fatalf("post-compaction table = rows %d, deleted %d, words %d; want 128, 0, 2", len(table.keys), table.patchParts.deletedCount, len(table.patchParts.deleted.words))
	}
}
