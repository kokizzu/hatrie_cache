package hatSql

import (
	"testing"
	"time"
)

func BenchmarkM242TypedOperatorUpdate(b *testing.B) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{
		Updates:    1,
		Batches:    1,
		Frontier:   1,
		ObservedAt: time.Unix(1, 0),
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := catalog.ObserveOperator(SQLDataflowObjectCompute, "join", SQLDataflowOperatorObservation{
			Updates:    uint64(iteration + 2),
			Batches:    uint64(iteration + 2),
			Frontier:   uint64(iteration + 2),
			ObservedAt: time.Unix(int64(iteration+2), 0),
		}); err != nil {
			b.Fatal(err)
		}
	}
}
