package hatCache

import (
	"testing"
	"time"
)

func BenchmarkTR038SQLTransactionTimeoutGuard(b *testing.B) {
	for _, test := range []struct {
		name     string
		deadline time.Time
	}{
		{name: "disabled", deadline: time.Time{}},
		{name: "enabled", deadline: time.Now().Add(time.Hour)},
	} {
		b.Run(test.name, func(b *testing.B) {
			transaction := &SQLTransaction{deadline: test.deadline}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				transaction.mu.Lock()
				err := transaction.checkTimeoutLocked()
				transaction.mu.Unlock()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
