package hatSql

import (
	"testing"
	"time"
)

func BenchmarkM245ProgressUpdate(b *testing.B) {
	catalog, err := NewSQLDataflowMetricsCatalog(SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{
		InputTimestamp:   1,
		OutputTimestamp:  1,
		InputObservedAt:  time.Unix(1, 0),
		OutputObservedAt: time.Unix(1, 0),
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		at := time.Unix(int64(iteration+2), 0)
		if err := catalog.ObserveProgress(SQLDataflowObjectSource, "orders", SQLDataflowProgressObservation{
			InputTimestamp:   uint64(iteration + 2),
			OutputTimestamp:  uint64(iteration + 2),
			InputObservedAt:  at,
			OutputObservedAt: at,
		}); err != nil {
			b.Fatal(err)
		}
	}
}
