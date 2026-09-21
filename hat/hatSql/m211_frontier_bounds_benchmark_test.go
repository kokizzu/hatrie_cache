package hatSql

import "testing"

var m211FrontierBoundsSink int

func BenchmarkM211TypedTableBeginSQLSnapshotAt(b *testing.B) {
	table := newM211BenchmarkTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resolver, release, err := table.BeginSQLSnapshotAt(nil, 1)
		if err != nil {
			b.Fatal(err)
		}
		if resolver != nil {
			m211FrontierBoundsSink = 1
		} else {
			m211FrontierBoundsSink = 0
		}
		release()
	}
}

func BenchmarkM211SQLFrontierBoundsValidate(b *testing.B) {
	table := newM211BenchmarkTable(b)
	bounds, err := table.SQLFrontierBounds()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := bounds.Validate(1); err != nil {
			b.Fatal(err)
		}
	}
}
