package hatCache

import "testing"

func BenchmarkCommandJournalRetentionBoundary(b *testing.B) {
	for _, test := range []struct {
		name   string
		leases int
	}{
		{name: "NoBackupLease"},
		{name: "OneBackupLease", leases: 1},
		{name: "FourBackupLeases", leases: 4},
	} {
		b.Run(test.name, func(b *testing.B) {
			journal := &CommandJournal{}
			if test.leases > 0 {
				journal.backupRetentionLeases = make(map[*CommandJournalBackupRetentionLease]struct{}, test.leases)
				for id := 1; id <= test.leases; id++ {
					journal.backupRetentionLeases[&CommandJournalBackupRetentionLease{sequence: uint64(id)}] = struct{}{}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				journal.mu.Lock()
				_, _ = journal.retentionThroughLocked()
				journal.mu.Unlock()
			}
		})
	}
}
