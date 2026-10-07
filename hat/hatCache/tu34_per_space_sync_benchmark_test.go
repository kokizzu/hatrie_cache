package hatCache

import (
	"testing"
	"time"
)

func BenchmarkTU34LegacySpaceSyncDecision(b *testing.B) {
	journal := &CommandJournal{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !journal.shouldSyncSpaceLocked("bulk", time.Time{}) {
			b.Fatal("legacy decision unexpectedly disabled sync")
		}
	}
}

func BenchmarkTU34DisabledSpaceSyncDecision(b *testing.B) {
	journal := &CommandJournal{
		spaceSyncPolicies: map[string]CommandJournalSpaceSyncPolicy{
			"bulk": {Mode: CommandJournalSpaceSyncDisabled},
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if journal.shouldSyncSpaceLocked("bulk", time.Time{}) {
			b.Fatal("disabled policy unexpectedly required sync")
		}
	}
}

func BenchmarkTU34PeriodicSpaceSyncDecision(b *testing.B) {
	journal := &CommandJournal{
		spaceSyncPolicies: map[string]CommandJournalSpaceSyncPolicy{
			"reports": {Mode: CommandJournalSpaceSyncPeriodic, Interval: time.Hour},
		},
		lastSpaceSync: time.Now(),
	}
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if journal.shouldSyncSpaceLocked("reports", now) {
			b.Fatal("unexpired periodic policy unexpectedly required sync")
		}
	}
}
