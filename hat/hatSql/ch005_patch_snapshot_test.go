package hatSql

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestCH005PatchStateRoundTripPreservesLogicalDeletes(t *testing.T) {
	source := ch005NewPatchTable(t, "events")
	for _, row := range []struct {
		key   string
		value int64
	}{
		{key: "a", value: 1},
		{key: "b", value: 2},
		{key: "c", value: 3},
	} {
		if _, err := source.Upsert(row.key, []TypedTableValue{TypedInt64(row.value)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Delete("b"); err != nil {
		t.Fatal(err)
	}
	encoded, err := source.MarshalPatchState()
	if err != nil {
		t.Fatalf("MarshalPatchState() error = %v", err)
	}

	restored := ch005NewPatchTable(t, "events")
	for _, row := range []struct {
		key   string
		value int64
	}{
		{key: "a", value: 1},
		{key: "b", value: 2},
		{key: "c", value: 3},
	} {
		if _, err := restored.Upsert(row.key, []TypedTableValue{TypedInt64(row.value)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.RestorePatchState(encoded); err != nil {
		t.Fatalf("RestorePatchState() error = %v", err)
	}
	rows, err := restored.ResolveSQLSource("CACHE", "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["value"] != int64(1) || rows[1]["value"] != int64(3) {
		t.Fatalf("restored rows = %#v, want live a/c", rows)
	}
	if got := len(restored.keys); got != 3 {
		t.Fatalf("restored physical rows = %d, want 3", got)
	}
}

func TestCH005PatchStateRejectsCorruptionAndMismatchedRowsAtomically(t *testing.T) {
	source := ch005NewPatchTable(t, "events")
	for _, key := range []string{"a", "b", "c"} {
		if _, err := source.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Delete("b"); err != nil {
		t.Fatal(err)
	}
	encoded, err := source.MarshalPatchState()
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	restored := ch005NewPatchTable(t, "events")
	for _, key := range []string{"a", "b", "c"} {
		if _, err := restored.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := restored.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := restored.RestorePatchState(corrupt); !errors.Is(err, ErrTypedTablePatchStateInvalid) {
		t.Fatalf("corrupt restore error = %v, want %v", err, ErrTypedTablePatchStateInvalid)
	}
	rows, err := restored.ResolveSQLSource("CACHE", "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["value"] != int64(1) || rows[1]["value"] != int64(1) {
		t.Fatalf("state changed after corrupt restore = %#v, want b/c live", rows)
	}

	mismatched := ch005NewPatchTable(t, "events")
	for _, key := range []string{"a", "c", "b"} {
		if _, err := mismatched.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := mismatched.RestorePatchState(encoded); !errors.Is(err, ErrTypedTablePatchStateInvalid) {
		t.Fatalf("mismatched restore error = %v, want %v", err, ErrTypedTablePatchStateInvalid)
	}
	if rows := mismatched.Rows(); len(rows) != 3 {
		t.Fatalf("mismatched restore changed rows = %#v", rows)
	}
}

func TestCH005PatchStateRequiresEnabledPatchParts(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.MarshalPatchState(); !errors.Is(err, ErrTypedTablePatchStateUnsupported) {
		t.Fatalf("disabled marshal error = %v, want %v", err, ErrTypedTablePatchStateUnsupported)
	}
	if err := table.RestorePatchState(nil); !errors.Is(err, ErrTypedTablePatchStateUnsupported) {
		t.Fatalf("disabled restore error = %v, want %v", err, ErrTypedTablePatchStateUnsupported)
	}
}

func TestCH005PatchStateRejectsUnboundedRowCount(t *testing.T) {
	table := ch005NewPatchTable(t, "events")
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	encoded, err := table.MarshalPatchState()
	if err != nil {
		t.Fatal(err)
	}
	position := len(typedTablePatchStateMagic) + 4
	nameLength := binary.LittleEndian.Uint32(encoded[position:])
	position += 4 + int(nameLength)
	binary.LittleEndian.PutUint32(encoded[position:], ^uint32(0))
	payloadLength := len(encoded) - 4
	binary.LittleEndian.PutUint32(encoded[payloadLength:], crc32.ChecksumIEEE(encoded[:payloadLength]))
	if err := table.RestorePatchState(encoded); !errors.Is(err, ErrTypedTablePatchStateInvalid) {
		t.Fatalf("unbounded row count error = %v, want %v", err, ErrTypedTablePatchStateInvalid)
	}
}

func ch005NewPatchTable(t *testing.T, name string) *TypedTable {
	t.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name:       name,
		PatchParts: TypedTablePatchOptions{Enabled: true, MergeThreshold: 100},
		Columns:    []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}
