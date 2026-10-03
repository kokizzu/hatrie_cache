package hatSql

import (
	"errors"
	"strings"
	"testing"
)

func TestCHU06PersistentDeleteBitmapRoundTrip(t *testing.T) {
	source := ch005NewPatchTable(t, "events")
	keys := []string{strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64)}
	for _, key := range keys {
		if _, err := source.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Delete(keys[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Delete(keys[3]); err != nil {
		t.Fatal(err)
	}

	encoded, err := source.MarshalDeleteBitmap()
	if err != nil {
		t.Fatalf("MarshalDeleteBitmap() error = %v", err)
	}
	full, err := source.MarshalPatchState()
	if err != nil {
		t.Fatalf("MarshalPatchState() error = %v", err)
	}
	if len(encoded) >= len(full) {
		t.Fatalf("compact bitmap bytes = %d, full patch state bytes = %d; want compact snapshot", len(encoded), len(full))
	}

	restored := ch005NewPatchTable(t, "events")
	for _, key := range keys {
		if _, err := restored.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.RestoreDeleteBitmap(encoded); err != nil {
		t.Fatalf("RestoreDeleteBitmap() error = %v", err)
	}
	if !restored.patchParts.deleted.contains(1) || !restored.patchParts.deleted.contains(3) || restored.patchParts.deletedCount != 2 {
		t.Fatalf("restored delete bits = %#v/%d, want rows 1 and 3", restored.patchParts.deleted.words, restored.patchParts.deletedCount)
	}
	rows, err := restored.ResolveSQLSource("CACHE", "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["value"] != int64(1) || rows[1]["value"] != int64(1) {
		t.Fatalf("restored rows = %#v, want two live rows", rows)
	}
}

func TestCHU06PersistentDeleteBitmapFingerprintInvalidatesAfterPhysicalChanges(t *testing.T) {
	source := ch005NewPatchTable(t, "events")
	keys := []string{strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)}
	for _, key := range keys {
		if _, err := source.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.MarshalDeleteBitmap(); err != nil {
		t.Fatal(err)
	}
	newKey := strings.Repeat("d", 64)
	if _, err := source.Upsert(newKey, []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	encoded, err := source.MarshalDeleteBitmap()
	if err != nil {
		t.Fatal(err)
	}
	restored := ch005NewPatchTable(t, "events")
	for _, key := range append(keys, newKey) {
		if _, err := restored.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.RestoreDeleteBitmap(encoded); err != nil {
		t.Fatalf("restore after append error = %v", err)
	}

	compactedSource := ch005NewPatchTable(t, "events")
	for _, key := range keys {
		if _, err := compactedSource.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := compactedSource.Delete(keys[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := compactedSource.MarshalDeleteBitmap(); err != nil {
		t.Fatal(err)
	}
	if err := compactedSource.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	compacted, err := compactedSource.MarshalDeleteBitmap()
	if err != nil {
		t.Fatal(err)
	}
	compactedRestored := ch005NewPatchTable(t, "events")
	for _, key := range []string{keys[0], keys[2]} {
		if _, err := compactedRestored.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := compactedRestored.RestoreDeleteBitmap(compacted); err != nil {
		t.Fatalf("restore after compaction error = %v", err)
	}
}

func TestCHU06PersistentDeleteBitmapRejectsCorruptionAndMismatchAtomically(t *testing.T) {
	source := ch005NewPatchTable(t, "events")
	keys := []string{strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)}
	for _, key := range keys {
		if _, err := source.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Delete(keys[1]); err != nil {
		t.Fatal(err)
	}
	encoded, err := source.MarshalDeleteBitmap()
	if err != nil {
		t.Fatal(err)
	}

	restored := ch005NewPatchTable(t, "events")
	for _, key := range keys {
		if _, err := restored.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := restored.Delete(keys[0]); err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if err := restored.RestoreDeleteBitmap(corrupt); !errors.Is(err, ErrTypedTableDeleteBitmapStateInvalid) {
		t.Fatalf("corrupt restore error = %v, want %v", err, ErrTypedTableDeleteBitmapStateInvalid)
	}
	rows, err := restored.ResolveSQLSource("CACHE", "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("state changed after corrupt restore = %#v, want two live rows", rows)
	}
	if !restored.patchParts.deleted.contains(0) || restored.patchParts.deleted.contains(1) || restored.patchParts.deletedCount != 1 {
		t.Fatalf("delete state changed after corrupt restore = %#v/%d, want only row 0", restored.patchParts.deleted.words, restored.patchParts.deletedCount)
	}

	mismatched := ch005NewPatchTable(t, "events")
	for _, key := range []string{keys[0], keys[2], keys[1]} {
		if _, err := mismatched.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := mismatched.RestoreDeleteBitmap(encoded); !errors.Is(err, ErrTypedTableDeleteBitmapStateInvalid) {
		t.Fatalf("mismatched restore error = %v, want %v", err, ErrTypedTableDeleteBitmapStateInvalid)
	}
	if rows := mismatched.Rows(); len(rows) != 3 {
		t.Fatalf("mismatched restore changed rows = %#v", rows)
	}
	if mismatched.patchParts.deletedCount != 0 {
		t.Fatalf("mismatched restore installed deletes = %d, want 0", mismatched.patchParts.deletedCount)
	}
}

func TestCHU06PersistentDeleteBitmapRequiresPatchParts(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.MarshalDeleteBitmap(); !errors.Is(err, ErrTypedTableDeleteBitmapStateUnsupported) {
		t.Fatalf("disabled marshal error = %v, want %v", err, ErrTypedTableDeleteBitmapStateUnsupported)
	}
	if err := table.RestoreDeleteBitmap(nil); !errors.Is(err, ErrTypedTableDeleteBitmapStateUnsupported) {
		t.Fatalf("disabled restore error = %v, want %v", err, ErrTypedTableDeleteBitmapStateUnsupported)
	}
}
