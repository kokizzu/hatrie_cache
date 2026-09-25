package hatSql

import (
	"errors"
	"hash/crc32"
	"testing"
)

func TestCH005PatchSnapshotUsesCompactKeyDigest(t *testing.T) {
	source := ch005CompactPatchSnapshotFixture(t)
	encoded, err := source.MarshalPatchState()
	if err != nil {
		t.Fatalf("MarshalPatchState() error = %v", err)
	}
	if got := encoded[len(typedTablePatchStateMagic)]; got != 2 {
		t.Fatalf("snapshot version = %d, want compact version 2", got)
	}
	if len(encoded) >= 4096 {
		t.Fatalf("compact snapshot length = %d, want less than physical row count", len(encoded))
	}
	t.Logf("compact snapshot bytes=%d", len(encoded))

	restored := ch005CompactPatchSnapshotFixture(t)
	if err := restored.RestorePatchState(encoded); err != nil {
		t.Fatalf("RestorePatchState() error = %v", err)
	}
	assertCH005CompactPatchSnapshotRows(t, restored)
}

func TestCH005PatchSnapshotRestoresLegacyKeyList(t *testing.T) {
	source := ch005CompactPatchSnapshotFixture(t)
	legacy := ch005LegacyPatchSnapshot(t, source)
	t.Logf("legacy snapshot bytes=%d", len(legacy))
	restored := ch005CompactPatchSnapshotFixture(t)
	if err := restored.RestorePatchState(legacy); err != nil {
		t.Fatalf("legacy RestorePatchState() error = %v", err)
	}
	assertCH005CompactPatchSnapshotRows(t, restored)
}

func TestCH005PatchSnapshotRejectsDigestMismatchAtomically(t *testing.T) {
	source := ch005CompactPatchSnapshotFixture(t)
	encoded, err := source.MarshalPatchState()
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewTypedTable(TypedTableSchema{
		Name:       "compact_snapshot",
		PatchParts: TypedTablePatchOptions{Enabled: true, MergeThreshold: 1 << 30},
		Columns:    []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		key := "different-key"
		if index != 0 {
			key = "customer-region-00000000-immutable-key"
		}
		if _, err := target.Upsert(key, []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := target.Delete("customer-region-00000000-immutable-key"); err != nil {
		t.Fatal(err)
	}
	before := len(target.Rows())
	if err := target.RestorePatchState(encoded); !errors.Is(err, ErrTypedTablePatchStateInvalid) {
		t.Fatalf("digest mismatch error = %v, want %v", err, ErrTypedTablePatchStateInvalid)
	}
	if got := len(target.Rows()); got != before {
		t.Fatalf("digest mismatch changed target rows = %d, want %d", got, before)
	}
}

func TestCH005PatchSnapshotDigestCacheInvalidatesAfterPhysicalChanges(t *testing.T) {
	source := ch005CompactPatchSnapshotFixture(t)
	if _, err := source.MarshalPatchState(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Upsert("customer-region-append-immutable-key", []TypedTableValue{TypedInt64(4096)}); err != nil {
		t.Fatal(err)
	}
	encoded, err := source.MarshalPatchState()
	if err != nil {
		t.Fatal(err)
	}
	target := ch005CompactPatchSnapshotFixture(t)
	if _, err := target.Upsert("customer-region-append-immutable-key", []TypedTableValue{TypedInt64(4096)}); err != nil {
		t.Fatal(err)
	}
	if err := target.RestorePatchState(encoded); err != nil {
		t.Fatalf("RestorePatchState() after append error = %v", err)
	}

	deleteKey := "customer-region-00000006-immutable-key"
	if _, err := source.Delete(deleteKey); err != nil {
		t.Fatal(err)
	}
	if err := source.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	encoded, err = source.MarshalPatchState()
	if err != nil {
		t.Fatal(err)
	}
	target = ch005CompactPatchSnapshotFixture(t)
	if _, err := target.Upsert("customer-region-append-immutable-key", []TypedTableValue{TypedInt64(4096)}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Delete(deleteKey); err != nil {
		t.Fatal(err)
	}
	if err := target.CompactPatchParts(); err != nil {
		t.Fatal(err)
	}
	if err := target.RestorePatchState(encoded); err != nil {
		t.Fatalf("RestorePatchState() after compaction error = %v", err)
	}
}

func ch005LegacyPatchSnapshot(tb testing.TB, table *TypedTable) []byte {
	tb.Helper()
	table.mu.RLock()
	defer table.mu.RUnlock()
	payload := append([]byte(nil), typedTablePatchStateMagic...)
	payload = append(payload, 1, 0, 0, 0)
	payload = appendUint32(payload, uint32(len(table.schema.Name)))
	payload = append(payload, table.schema.Name...)
	payload = appendUint32(payload, uint32(len(table.keys)))
	payload = appendUint32(payload, uint32(table.patchParts.deletedCount))
	payload = appendUint32(payload, uint32(len(table.patchParts.deleted.words)))
	for _, key := range table.keys {
		payload = appendUint32(payload, uint32(len(key)))
		payload = append(payload, key...)
	}
	for _, word := range table.patchParts.deleted.words {
		payload = appendUint64(payload, word)
	}
	return appendUint32(payload, crc32.ChecksumIEEE(payload))
}

func assertCH005CompactPatchSnapshotRows(t *testing.T, table *TypedTable) {
	t.Helper()
	rows, err := table.ResolveSQLSource("CACHE", "compact_snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(rows), 3276; got != want {
		t.Fatalf("live rows = %d, want %d", got, want)
	}
	if rows[0]["value"] != int64(1) || rows[len(rows)-1]["value"] != int64(4094) {
		t.Fatalf("restored row bounds = %#v / %#v", rows[0], rows[len(rows)-1])
	}
}
