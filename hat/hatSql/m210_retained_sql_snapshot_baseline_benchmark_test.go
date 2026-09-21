package hatSql

import "testing"

var m210RetainedSQLSnapshotBaselineSink int

func BenchmarkM210TypedTableSnapshotAtBaseline(b *testing.B) {
	users := newM210BenchmarkTable(b, "users", TypedTableColumn{Name: "name", Kind: TypedTableString})
	orders := newM210BenchmarkTable(b, "orders", TypedTableColumn{Name: "state", Kind: TypedTableString})
	if _, err := users.Upsert("a", []TypedTableValue{TypedString("ready")}); err != nil {
		b.Fatal(err)
	}
	if _, err := orders.Upsert("a", []TypedTableValue{TypedString("pending")}); err != nil {
		b.Fatal(err)
	}
	frontier := uint64(1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		usersSnapshot, err := users.SnapshotAt(frontier)
		if err != nil {
			b.Fatal(err)
		}
		ordersSnapshot, err := orders.SnapshotAt(frontier)
		if err != nil {
			b.Fatal(err)
		}
		usersRows, err := usersSnapshot.ResolveSQLSource("CACHE", "users")
		if err != nil {
			b.Fatal(err)
		}
		ordersRows, err := ordersSnapshot.ResolveSQLSource("CACHE", "orders")
		if err != nil {
			b.Fatal(err)
		}
		m210RetainedSQLSnapshotBaselineSink = len(usersRows) + len(ordersRows)
	}
}

func BenchmarkM210TypedTableSnapshotAtSingleBaseline(b *testing.B) {
	table := newM210BenchmarkTable(b, "items", TypedTableColumn{Name: "value", Kind: TypedTableString})
	if _, err := table.Upsert("a", []TypedTableValue{TypedString("ready")}); err != nil {
		b.Fatal(err)
	}
	frontier := uint64(1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snapshot, err := table.SnapshotAt(frontier)
		if err != nil {
			b.Fatal(err)
		}
		rows, err := snapshot.ResolveSQLSource("CACHE", "items")
		if err != nil {
			b.Fatal(err)
		}
		m210RetainedSQLSnapshotBaselineSink = len(rows)
	}
}
