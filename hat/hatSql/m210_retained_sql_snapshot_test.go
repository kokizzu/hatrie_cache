package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTypedTableSQLSnapshotRegistryServesHistoricalSQLAcrossTables(t *testing.T) {
	users := newM210RetainedTable(t, "users",
		TypedTableColumn{Name: "name", Kind: TypedTableString},
		TypedTableColumn{Name: "score", Kind: TypedTableInt64},
	)
	orders := newM210RetainedTable(t, "orders",
		TypedTableColumn{Name: "state", Kind: TypedTableString},
		TypedTableColumn{Name: "version", Kind: TypedTableString},
	)
	userInsert, err := users.Upsert("a", []TypedTableValue{TypedString("old"), TypedInt64(1)})
	if err != nil {
		t.Fatalf("users initial Upsert() error = %v", err)
	}
	if _, err := orders.Upsert("a", []TypedTableValue{TypedString("pending"), TypedString("old")}); err != nil {
		t.Fatalf("orders initial Upsert() error = %v", err)
	}
	if _, err := users.Upsert("a", []TypedTableValue{TypedString("new"), TypedInt64(2)}); err != nil {
		t.Fatalf("users update Upsert() error = %v", err)
	}
	if _, err := orders.Upsert("a", []TypedTableValue{TypedString("shipped"), TypedString("new")}); err != nil {
		t.Fatalf("orders update Upsert() error = %v", err)
	}

	registry := NewTypedTableSQLSnapshotRegistry()
	if err := registry.Register(users); err != nil {
		t.Fatalf("Register(users) error = %v", err)
	}
	if err := registry.Register(orders); err != nil {
		t.Fatalf("Register(orders) error = %v", err)
	}
	var _ SQLFrontierSnapshotProvider = registry

	frontier := userInsert.Sequence
	snapshot, release, err := registry.BeginSQLSnapshotAt(context.Background(), frontier)
	if err != nil {
		t.Fatalf("BeginSQLSnapshotAt() error = %v", err)
	}
	if snapshot == nil || release == nil {
		t.Fatalf("BeginSQLSnapshotAt() returned snapshot nil=%v release nil=%v", snapshot == nil, release == nil)
	}
	defer release()

	usersAtFrontier, err := snapshot.ResolveSQLSource("CACHE", "users")
	if err != nil {
		t.Fatalf("historical users ResolveSQLSource() error = %v", err)
	}
	if want := []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(usersAtFrontier, want) {
		t.Fatalf("historical users = %#v, want %#v", usersAtFrontier, want)
	}
	ordersAtFrontier, err := snapshot.ResolveSQLSource("CACHE", "orders")
	if err != nil {
		t.Fatalf("historical orders ResolveSQLSource() error = %v", err)
	}
	if want := []Row{{"state": "pending", "version": "old"}}; !reflect.DeepEqual(ordersAtFrontier, want) {
		t.Fatalf("historical orders = %#v, want %#v", ordersAtFrontier, want)
	}
	columnarResolver, ok := snapshot.(ColumnarSourceResolver)
	if !ok {
		t.Fatal("historical resolver does not expose columnar reads")
	}
	batch, found, err := columnarResolver.ResolveSQLColumnarSource("CACHE", "users", []string{"name"})
	if err != nil || !found {
		t.Fatalf("historical columnar users = %#v/%v/%v", batch, found, err)
	}
	if want := []interface{}{"old"}; !reflect.DeepEqual(batch.Columns["name"], want) {
		t.Fatalf("historical columnar name = %#v, want %#v", batch.Columns["name"], want)
	}
	versionResolver, ok := snapshot.(SourceVersionResolver)
	if !ok {
		t.Fatal("historical resolver does not expose source versions")
	}
	version, available, err := versionResolver.SQLSourceVersion("CACHE", "users")
	if err != nil || !available || version != "1" {
		t.Fatalf("historical source version = %q/%v/%v, want 1/true/nil", version, available, err)
	}

	if _, err := users.Upsert("a", []TypedTableValue{TypedString("latest"), TypedInt64(3)}); err != nil {
		t.Fatalf("users latest Upsert() error = %v", err)
	}
	stillHistorical, err := snapshot.ResolveSQLSource("CACHE", "users")
	if err != nil {
		t.Fatalf("snapshot after write ResolveSQLSource() error = %v", err)
	}
	if want := []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(stillHistorical, want) {
		t.Fatalf("snapshot changed after write = %#v, want %#v", stillHistorical, want)
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('users') SELECT name, score", registry, SQLQueryOptions{AsOfFrontier: &frontier})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext(AS OF) error = %v", err)
	}
	if want := []Row{{"name": "old", "score": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("AS OF result = %#v, want %#v", result.Rows, want)
	}
}

func TestTypedTableSQLSnapshotRegistryRejectsInvalidRegistration(t *testing.T) {
	registry := NewTypedTableSQLSnapshotRegistry()
	if err := registry.Register(nil); !errors.Is(err, ErrTypedTableSQLSnapshotTableRequired) {
		t.Fatalf("Register(nil) error = %v, want ErrTypedTableSQLSnapshotTableRequired", err)
	}

	disabled, err := NewTypedTable(TypedTableSchema{
		Name:    "disabled",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatalf("NewTypedTable(disabled) error = %v", err)
	}
	if err := registry.Register(disabled); !errors.Is(err, ErrTypedTableSQLSnapshotMVCCRequired) {
		t.Fatalf("Register(disabled) error = %v, want ErrTypedTableSQLSnapshotMVCCRequired", err)
	}

	first := newM210RetainedTable(t, "same", TypedTableColumn{Name: "value", Kind: TypedTableString})
	second := newM210RetainedTable(t, "same", TypedTableColumn{Name: "value", Kind: TypedTableString})
	if err := registry.Register(first); err != nil {
		t.Fatalf("Register(first) error = %v", err)
	}
	if err := registry.Register(second); !errors.Is(err, ErrTypedTableSQLSnapshotDuplicate) {
		t.Fatalf("Register(second) error = %v, want ErrTypedTableSQLSnapshotDuplicate", err)
	}
}

func TestTypedTableSQLSnapshotRegistryHonorsCancellationAndCompaction(t *testing.T) {
	table := newM210RetainedTable(t, "items", TypedTableColumn{Name: "value", Kind: TypedTableString})
	change, err := table.Upsert("a", []TypedTableValue{TypedString("value")})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	registry := NewTypedTableSQLSnapshotRegistry()
	if err := registry.Register(table); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := registry.BeginSQLSnapshotAt(ctx, change.Sequence); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled BeginSQLSnapshotAt() error = %v, want context.Canceled", err)
	}

	if err := table.CompactMVCCThrough(change.Sequence); err != nil {
		t.Fatalf("CompactMVCCThrough() error = %v", err)
	}
	if _, _, err := registry.BeginSQLSnapshotAt(context.Background(), 0); !errors.Is(err, ErrTypedTableMVCCCompacted) {
		t.Fatalf("compacted BeginSQLSnapshotAt() error = %v, want ErrTypedTableMVCCCompacted", err)
	}
}

func TestTypedTableProvidesDirectHistoricalSQLSnapshot(t *testing.T) {
	table := newM210RetainedTable(t, "items", TypedTableColumn{Name: "value", Kind: TypedTableString})
	change, err := table.Upsert("a", []TypedTableValue{TypedString("old")})
	if err != nil {
		t.Fatalf("initial Upsert() error = %v", err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedString("new")}); err != nil {
		t.Fatalf("update Upsert() error = %v", err)
	}
	var _ SQLFrontierSnapshotProvider = table

	snapshot, release, err := table.BeginSQLSnapshotAt(context.Background(), change.Sequence)
	if err != nil {
		t.Fatalf("BeginSQLSnapshotAt() error = %v", err)
	}
	if release == nil {
		t.Fatal("BeginSQLSnapshotAt() returned nil release")
	}
	release()
	rows, err := snapshot.ResolveSQLSource("CACHE", "items")
	if err != nil {
		t.Fatalf("historical ResolveSQLSource() error = %v", err)
	}
	if want := []Row{{"value": "old"}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("historical rows = %#v, want %#v", rows, want)
	}
}

func newM210RetainedTable(t *testing.T, name string, columns ...TypedTableColumn) *TypedTable {
	t.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name:    name,
		MVCC:    TypedTableMVCCOptions{Enabled: true},
		Columns: columns,
	})
	if err != nil {
		t.Fatalf("NewTypedTable(%q) error = %v", name, err)
	}
	return table
}
