//go:build tu36_backup_rotation

package hatCache

import (
	"testing"
	"time"
)

func BenchmarkTU36BackupRotationPolicy(b *testing.B) {
	policy := BackupRotationPolicy{
		MinimumInterval:      time.Minute,
		MaximumInterval:      time.Hour,
		JournalSequenceDelta: 100,
	}
	state := BackupRotationState{
		HasBackup:              true,
		LastCreatedAt:          time.Unix(100, 0),
		LastJournalSequence:    10,
		CurrentJournalSequence: 50,
	}
	now := time.Unix(200, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decision, err := policy.Decide(now, state)
		if err != nil || decision.Due {
			b.Fatalf("Decide() = %#v, %v, want not due", decision, err)
		}
	}
}
