package hatSql

import (
	"context"
	"testing"
)

var m210RetainedSQLSnapshotRegistrySink int

func BenchmarkM210TypedTableBeginSQLSnapshotAt(b *testing.B) {
	table := newM210BenchmarkTable(b, "items", TypedTableColumn{Name: "value", Kind: TypedTableString})
	if _, err := table.Upsert("a", []TypedTableValue{TypedString("ready")}); err != nil {
		b.Fatal(err)
	}
	frontier := uint64(1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resolver, release, err := table.BeginSQLSnapshotAt(context.Background(), frontier)
		if err != nil {
			b.Fatal(err)
		}
		rows, err := resolver.ResolveSQLSource("CACHE", "items")
		if err != nil {
			b.Fatal(err)
		}
		m210RetainedSQLSnapshotRegistrySink = len(rows)
		release()
	}
}

func BenchmarkM210TypedTableSQLSnapshotRegistry(b *testing.B) {
	users := newM210BenchmarkTable(b, "users", TypedTableColumn{Name: "name", Kind: TypedTableString})
	orders := newM210BenchmarkTable(b, "orders", TypedTableColumn{Name: "state", Kind: TypedTableString})
	if _, err := users.Upsert("a", []TypedTableValue{TypedString("ready")}); err != nil {
		b.Fatal(err)
	}
	if _, err := orders.Upsert("a", []TypedTableValue{TypedString("pending")}); err != nil {
		b.Fatal(err)
	}
	registry := NewTypedTableSQLSnapshotRegistry()
	if err := registry.Register(users); err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(orders); err != nil {
		b.Fatal(err)
	}
	frontier := uint64(1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resolver, release, err := registry.BeginSQLSnapshotAt(context.Background(), frontier)
		if err != nil {
			b.Fatal(err)
		}
		usersRows, err := resolver.ResolveSQLSource("CACHE", "users")
		if err != nil {
			b.Fatal(err)
		}
		ordersRows, err := resolver.ResolveSQLSource("CACHE", "orders")
		if err != nil {
			b.Fatal(err)
		}
		m210RetainedSQLSnapshotRegistrySink = len(usersRows) + len(ordersRows)
		release()
	}
}

func newM210BenchmarkTable(b *testing.B, name string, columns ...TypedTableColumn) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name:    name,
		MVCC:    TypedTableMVCCOptions{Enabled: true},
		Columns: columns,
	})
	if err != nil {
		b.Fatal(err)
	}
	return table
}
