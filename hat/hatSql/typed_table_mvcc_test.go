package hatSql

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestTypedTableMVCCSnapshotsRetainHistoricalRows(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "users",
		MVCC: TypedTableMVCCOptions{Enabled: true},
		Columns: []TypedTableColumn{
			{Name: "name", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	insertA, err := table.Upsert("a", []TypedTableValue{TypedString("old"), TypedInt64(1)})
	if err != nil {
		t.Fatalf("Upsert(a) error = %v", err)
	}
	insertB, err := table.Upsert("b", []TypedTableValue{TypedString("keep"), TypedInt64(2)})
	if err != nil {
		t.Fatalf("Upsert(b) error = %v", err)
	}
	oldSnapshot, err := table.SnapshotAt(insertA.Sequence)
	if err != nil {
		t.Fatalf("SnapshotAt(insertA) error = %v", err)
	}
	if got, want := oldSnapshot.Sequence(), insertA.Sequence; got != want {
		t.Fatalf("historical snapshot sequence = %d, want %d", got, want)
	}
	if got, want := oldSnapshot.Rows(), []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("historical rows = %#v, want %#v", got, want)
	}

	updateA, err := table.Upsert("a", []TypedTableValue{TypedString("new"), TypedInt64(3)})
	if err != nil {
		t.Fatalf("Upsert(a update) error = %v", err)
	}
	if _, err := table.Delete("b"); err != nil {
		t.Fatalf("Delete(b) error = %v", err)
	}
	current, err := table.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if got, want := current.Rows(), []Row{{"name": "new", "score": int64(3)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current rows = %#v, want %#v", got, want)
	}
	if got, want := oldSnapshot.Rows(), []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("historical rows after writes = %#v, want %#v", got, want)
	}
	lateHistorical, err := table.SnapshotAt(insertA.Sequence)
	if err != nil {
		t.Fatalf("late SnapshotAt(insertA) error = %v", err)
	}
	if got, want := lateHistorical.Rows(), []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("late historical rows = %#v, want %#v", got, want)
	}
	resolved, err := oldSnapshot.ResolveSQLSource("CACHE", "users")
	if err != nil {
		t.Fatalf("historical ResolveSQLSource() error = %v", err)
	}
	if !reflect.DeepEqual(resolved, []Row{{"name": "old", "score": int64(1)}}) {
		t.Fatalf("historical resolved rows = %#v", resolved)
	}
	version, found, err := oldSnapshot.SQLSourceVersion("CACHE", "users")
	if err != nil || !found || version != "1" {
		t.Fatalf("historical SQLSourceVersion() = %q/%v/%v, want 1/true/nil", version, found, err)
	}
	batch, found, err := oldSnapshot.ResolveSQLColumnarSource("CACHE", "users", []string{"name", "score"})
	if err != nil || !found {
		t.Fatalf("historical ResolveSQLColumnarSource() = %#v/%v/%v", batch, found, err)
	}
	if got, want := batch.Columns["name"], []interface{}{"old"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("historical columnar name = %#v, want %#v", got, want)
	}

	if err := table.CompactMVCCThrough(updateA.Sequence); err != nil {
		t.Fatalf("CompactMVCCThrough() error = %v", err)
	}
	if _, err := table.SnapshotAt(insertB.Sequence); !errors.Is(err, ErrTypedTableMVCCCompacted) {
		t.Fatalf("SnapshotAt(compacted) error = %v, want ErrTypedTableMVCCCompacted", err)
	}
	if got, want := oldSnapshot.Rows(), []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("historical rows after compaction = %#v, want %#v", got, want)
	}
	if got, want := lateHistorical.Rows(), []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("late historical rows after compaction = %#v, want %#v", got, want)
	}
}

func TestTypedTableMVCCIsDisabledByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "users",
		Columns: []TypedTableColumn{{Name: "name", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.Snapshot(); !errors.Is(err, ErrTypedTableMVCCDisabled) {
		t.Fatalf("Snapshot() error = %v, want ErrTypedTableMVCCDisabled", err)
	}
}

func TestTypedTableMVCCSnapshotReadsWhileTableAdvances(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "users",
		MVCC: TypedTableMVCCOptions{Enabled: true},
		Columns: []TypedTableColumn{
			{Name: "name", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.Upsert("hot", []TypedTableValue{TypedString("old"), TypedInt64(1)}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	snapshot, err := table.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	want := []Row{{"name": "old", "score": int64(1)}}
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for index := 0; index < 1000; index++ {
			if _, err := table.Upsert("hot", []TypedTableValue{TypedString("new"), TypedInt64(int64(index))}); err != nil {
				t.Errorf("Upsert() error = %v", err)
				return
			}
		}
	}()
	for index := 0; index < 1000; index++ {
		if got := snapshot.Rows(); !reflect.DeepEqual(got, want) {
			t.Fatalf("snapshot rows while writer advances = %#v, want %#v", got, want)
		}
	}
	writer.Wait()
}
