package hatCache

import (
	"testing"
	"time"
)

func BenchmarkTU36BackupRotationBaseline(b *testing.B) {
	last := time.Unix(100, 0)
	now := last.Add(time.Hour)
	lastSequence := uint64(10)
	currentSequence := uint64(110)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		elapsed := now.Sub(last)
		due := elapsed >= time.Hour && currentSequence-lastSequence >= 100
		if !due {
			b.Fatal("baseline decision unexpectedly not due")
		}
	}
}
