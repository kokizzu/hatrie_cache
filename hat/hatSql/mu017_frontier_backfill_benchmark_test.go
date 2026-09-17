package hatSql

import (
	"context"
	"testing"
)

func BenchmarkMU017BackfillAtFrontier(b *testing.B) {
	runner, barrier := newMU017BenchmarkRunner(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := runner.BackfillAtFrontier(context.Background(), []string{"people"}, barrier, 7); err != nil {
			b.Fatal(err)
		}
	}
}
