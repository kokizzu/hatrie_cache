package hatSql

import "testing"

func BenchmarkM212RetainedStatePublishAndCompact(b *testing.B) {
	state, err := NewSQLRetainedState(SQLRetainedStateOptions{MaxFrontiers: 4})
	if err != nil {
		b.Fatal(err)
	}
	if err := state.Publish(1, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frontier := uint64(i + 2)
		if err := state.Publish(frontier, nil); err != nil {
			b.Fatal(err)
		}
		if _, err := state.Compact(frontier - 1); err != nil {
			b.Fatal(err)
		}
	}
}
