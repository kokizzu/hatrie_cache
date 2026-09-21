package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestM212LogicalCompactionAdvancesFrontierWithoutRewritingChains(t *testing.T) {
	table := newM211RetainedTable(t, "items")
	first, err := table.Upsert("a", []TypedTableValue{TypedString("v0")})
	if err != nil {
		t.Fatalf("first Upsert() error = %v", err)
	}
	second, err := table.Upsert("a", []TypedTableValue{TypedString("v1")})
	if err != nil {
		t.Fatalf("second Upsert() error = %v", err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedString("v2")}); err != nil {
		t.Fatalf("third Upsert() error = %v", err)
	}

	before, err := table.SnapshotAt(first.Sequence)
	if err != nil {
		t.Fatalf("SnapshotAt(first) error = %v", err)
	}
	headBefore := table.mvcc.heads["a"]
	chainBefore := m212VersionChainLength(headBefore)

	if err := table.AdvanceMVCCCompactionThrough(second.Sequence); err != nil {
		t.Fatalf("AdvanceMVCCCompactionThrough() error = %v", err)
	}
	if table.mvcc.heads["a"] != headBefore {
		t.Fatal("logical compaction rewrote the current version chain head")
	}
	if got := m212VersionChainLength(table.mvcc.heads["a"]); got != chainBefore {
		t.Fatalf("logical compaction chain length = %d, want unchanged %d", got, chainBefore)
	}
	bounds, err := table.SQLFrontierBounds()
	if err != nil {
		t.Fatalf("SQLFrontierBounds() error = %v", err)
	}
	if want := (SQLFrontierBounds{Since: second.Sequence, Upper: second.Sequence + 2}); !reflect.DeepEqual(bounds, want) {
		t.Fatalf("SQLFrontierBounds() = %#v, want %#v", bounds, want)
	}
	if _, err := table.SnapshotAt(first.Sequence); !errors.Is(err, ErrTypedTableMVCCCompacted) {
		t.Fatalf("SnapshotAt(before logical frontier) error = %v, want ErrTypedTableMVCCCompacted", err)
	}
	if err := table.AdvanceMVCCCompactionThrough(first.Sequence); err == nil {
		t.Fatal("logical compaction accepted a frontier older than the retained frontier")
	}
	if err := table.AdvanceMVCCCompactionThrough(table.sequence + 1); err == nil {
		t.Fatal("logical compaction accepted a frontier newer than the table")
	}

	beforeRows := before.Rows()
	if want := []Row{{"value": "v0"}}; !reflect.DeepEqual(beforeRows, want) {
		t.Fatalf("pre-compaction snapshot rows = %#v, want %#v", beforeRows, want)
	}
	after, err := table.SnapshotAt(second.Sequence)
	if err != nil {
		t.Fatalf("SnapshotAt(second) error = %v", err)
	}
	if want := []Row{{"value": "v1"}}; !reflect.DeepEqual(after.Rows(), want) {
		t.Fatalf("post-logical snapshot rows = %#v, want %#v", after.Rows(), want)
	}

	if err := table.CompactMVCCThrough(second.Sequence); err != nil {
		t.Fatalf("physical CompactMVCCThrough() after logical compaction error = %v", err)
	}
	if table.mvcc.heads["a"] == headBefore {
		t.Fatal("physical compaction did not replace the old version chain")
	}
	if got := m212VersionChainLength(table.mvcc.heads["a"]); got != 2 {
		t.Fatalf("physically compacted chain length = %d, want 2", got)
	}
	if want := []Row{{"value": "v0"}}; !reflect.DeepEqual(before.Rows(), want) {
		t.Fatalf("pre-compaction snapshot after physical compaction = %#v, want %#v", before.Rows(), want)
	}

	plain, err := NewTypedTable(TypedTableSchema{
		Name:    "plain",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatalf("NewTypedTable(plain) error = %v", err)
	}
	if err := plain.AdvanceMVCCCompactionThrough(0); !errors.Is(err, ErrTypedTableMVCCDisabled) {
		t.Fatalf("logical compaction without MVCC error = %v, want ErrTypedTableMVCCDisabled", err)
	}
}

func m212VersionChainLength(head *typedTableMVCCVersion) int {
	length := 0
	for version := head; version != nil; version = version.previous {
		length++
	}
	return length
}
