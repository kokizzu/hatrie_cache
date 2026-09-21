package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestM211TypedTableRejectsFrontiersOutsideRetainedBounds(t *testing.T) {
	table := newM211RetainedTable(t, "items")
	first, err := table.Upsert("a", []TypedTableValue{TypedString("old")})
	if err != nil {
		t.Fatalf("first Upsert() error = %v", err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedString("new")}); err != nil {
		t.Fatalf("second Upsert() error = %v", err)
	}
	if err := table.CompactMVCCThrough(first.Sequence); err != nil {
		t.Fatalf("CompactMVCCThrough() error = %v", err)
	}

	bounds, err := table.SQLFrontierBounds()
	if err != nil {
		t.Fatalf("SQLFrontierBounds() error = %v", err)
	}
	if want := (SQLFrontierBounds{Since: first.Sequence, Upper: first.Sequence + 2}); !reflect.DeepEqual(bounds, want) {
		t.Fatalf("SQLFrontierBounds() = %#v, want %#v", bounds, want)
	}
	if err := bounds.Validate(first.Sequence - 1); !errors.Is(err, ErrSQLFrontierBeforeSince) {
		t.Fatalf("before-since Validate() error = %v, want ErrSQLFrontierBeforeSince", err)
	}
	if err := bounds.Validate(bounds.Upper); !errors.Is(err, ErrSQLFrontierAtOrAfterUpper) {
		t.Fatalf("at-upper Validate() error = %v, want ErrSQLFrontierAtOrAfterUpper", err)
	}

	if _, _, err := table.BeginSQLSnapshotAt(context.Background(), first.Sequence-1); !errors.Is(err, ErrSQLFrontierBeforeSince) {
		t.Fatalf("before-since BeginSQLSnapshotAt() error = %v, want ErrSQLFrontierBeforeSince", err)
	}
	if _, _, err := table.BeginSQLSnapshotAt(context.Background(), bounds.Upper); !errors.Is(err, ErrSQLFrontierAtOrAfterUpper) {
		t.Fatalf("at-upper BeginSQLSnapshotAt() error = %v, want ErrSQLFrontierAtOrAfterUpper", err)
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT value", table, SQLQueryOptions{AsOfFrontier: uint64Pointer(bounds.Upper)}); !errors.Is(err, ErrSQLFrontierAtOrAfterUpper) {
		t.Fatalf("at-upper SQL query error = %v, want ErrSQLFrontierAtOrAfterUpper", err)
	}

	snapshot, release, err := table.BeginSQLSnapshotAt(context.Background(), first.Sequence)
	if err != nil {
		t.Fatalf("retained BeginSQLSnapshotAt() error = %v", err)
	}
	defer release()
	rows, err := snapshot.ResolveSQLSource("CACHE", "items")
	if err != nil {
		t.Fatalf("retained ResolveSQLSource() error = %v", err)
	}
	if want := []Row{{"value": "old"}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("retained rows = %#v, want %#v", rows, want)
	}
}

func TestM211FrontierBarrierRejectsAtUpperBeforeOpeningSnapshot(t *testing.T) {
	table := newM211RetainedTable(t, "items")
	first, err := table.Upsert("a", []TypedTableValue{TypedString("value")})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	bounds, err := table.SQLFrontierBounds()
	if err != nil {
		t.Fatalf("SQLFrontierBounds() error = %v", err)
	}
	barrier := newFrontierSnapshotTestBarrier(t, 1)
	if _, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "a", Frontier: bounds.Upper}); err != nil {
		t.Fatalf("barrier Observe() error = %v", err)
	}
	if _, _, err := BeginSQLFrontierSnapshot(context.Background(), table, barrier, bounds.Upper); !errors.Is(err, ErrSQLFrontierAtOrAfterUpper) {
		t.Fatalf("at-upper BeginSQLFrontierSnapshot() error = %v, want ErrSQLFrontierAtOrAfterUpper", err)
	}
	if _, _, err := BeginSQLFrontierSnapshot(context.Background(), table, barrier, first.Sequence); err != nil {
		t.Fatalf("retained BeginSQLFrontierSnapshot() error = %v", err)
	}
}

func TestM211RegistryUsesIntersectionOfSourceBounds(t *testing.T) {
	users := newM211RetainedTable(t, "users")
	firstUser, err := users.Upsert("u1", []TypedTableValue{TypedString("old")})
	if err != nil {
		t.Fatalf("users first Upsert() error = %v", err)
	}
	if _, err := users.Upsert("u1", []TypedTableValue{TypedString("new")}); err != nil {
		t.Fatalf("users second Upsert() error = %v", err)
	}
	if err := users.CompactMVCCThrough(firstUser.Sequence); err != nil {
		t.Fatalf("users CompactMVCCThrough() error = %v", err)
	}

	orders := newM211RetainedTable(t, "orders")
	if _, err := orders.Upsert("o1", []TypedTableValue{TypedString("order")}); err != nil {
		t.Fatalf("orders Upsert() error = %v", err)
	}
	registry := NewTypedTableSQLSnapshotRegistry()
	if err := registry.Register(users); err != nil {
		t.Fatalf("Register(users) error = %v", err)
	}
	if err := registry.Register(orders); err != nil {
		t.Fatalf("Register(orders) error = %v", err)
	}

	bounds, err := registry.SQLFrontierBounds()
	if err != nil {
		t.Fatalf("registry SQLFrontierBounds() error = %v", err)
	}
	if want := (SQLFrontierBounds{Since: firstUser.Sequence, Upper: firstUser.Sequence + 1}); !reflect.DeepEqual(bounds, want) {
		t.Fatalf("registry SQLFrontierBounds() = %#v, want %#v", bounds, want)
	}
	if _, _, err := registry.BeginSQLSnapshotAt(context.Background(), 0); !errors.Is(err, ErrSQLFrontierBeforeSince) {
		t.Fatalf("registry before-since error = %v, want ErrSQLFrontierBeforeSince", err)
	}
	if _, _, err := registry.BeginSQLSnapshotAt(context.Background(), bounds.Upper); !errors.Is(err, ErrSQLFrontierAtOrAfterUpper) {
		t.Fatalf("registry at-upper error = %v, want ErrSQLFrontierAtOrAfterUpper", err)
	}
	if _, _, err := registry.BeginSQLSnapshotAt(context.Background(), firstUser.Sequence); err != nil {
		t.Fatalf("registry retained frontier error = %v", err)
	}
}

func TestM211LegacySnapshotProviderRemainsUnbounded(t *testing.T) {
	resolver := m211LegacySnapshotResolver{}
	frontier := uint64(99)
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT value", resolver, SQLQueryOptions{AsOfFrontier: &frontier})
	if err != nil {
		t.Fatalf("legacy AS OF query error = %v", err)
	}
	if want := []Row{{"value": "legacy"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("legacy AS OF rows = %#v, want %#v", result.Rows, want)
	}
}

type m211LegacySnapshotResolver struct{}

func (m211LegacySnapshotResolver) ResolveSQLSource(_, key string) ([]Row, error) {
	if key != "items" {
		return nil, nil
	}
	return []Row{{"value": "legacy"}}, nil
}

func (m211LegacySnapshotResolver) BeginSQLSnapshotAt(context.Context, uint64) (SQLSourceResolver, func(), error) {
	return m211LegacySnapshotResolver{}, func() {}, nil
}

func newM211RetainedTable(t *testing.T, name string) *TypedTable {
	t.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: name,
		MVCC: TypedTableMVCCOptions{Enabled: true},
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableString},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable(%q) error = %v", name, err)
	}
	return table
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
