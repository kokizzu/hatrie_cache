package hatJournal

import (
	"testing"
	"time"
)

func BenchmarkTR018SyncPolicyDecision(b *testing.B) {
	now := time.Unix(100, 0)
	for _, test := range []struct {
		name     string
		mode     SyncMode
		interval time.Duration
	}{
		{name: "immediate", mode: SyncModeImmediate},
		{name: "periodic", mode: SyncModePeriodic, interval: time.Second},
		{name: "disabled", mode: SyncModeDisabled},
	} {
		b.Run(test.name, func(b *testing.B) {
			policy, err := NewSyncPolicy(test.mode, test.interval)
			if err != nil {
				b.Fatal(err)
			}
			policy.MarkSynced(now)
			b.ReportAllocs()
			b.ResetTimer()
			var due bool
			for index := 0; index < b.N; index++ {
				due = policy.ShouldSync(now)
			}
			if due && test.mode == SyncModeDisabled {
				b.Fatal("disabled policy became due")
			}
		})
	}
}
