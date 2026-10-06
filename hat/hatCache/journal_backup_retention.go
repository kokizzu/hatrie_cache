package hatCache

import "fmt"

// CommandJournalBackupRetentionLease keeps journal records after a hot-backup
// snapshot coordinate until the caller has finished copying or consuming them.
// Leases are process-local and must be released explicitly.
type CommandJournalBackupRetentionLease struct {
	journal  *CommandJournal
	sequence uint64
}

// AcquireBackupRetentionLease protects records after sequence from journal
// compaction. The sequence must be present in the current journal and must not
// already be older than the compacted boundary.
func (journal *CommandJournal) AcquireBackupRetentionLease(sequence uint64) (*CommandJournalBackupRetentionLease, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.acquireBackupRetentionLeaseLocked(sequence)
}

func (journal *CommandJournal) acquireBackupRetentionLeaseLocked(sequence uint64) (*CommandJournalBackupRetentionLease, error) {
	if journal.closed {
		return nil, ErrCommandJournalClosed
	}
	if sequence > journal.lastSequenceLocked() {
		return nil, fmt.Errorf("hatriecache: backup retention lease sequence %d exceeds journal tail %d", sequence, journal.lastSequenceLocked())
	}
	if sequence < journal.compactedThrough {
		return nil, fmt.Errorf("hatriecache: backup retention lease sequence %d is before compacted sequence %d", sequence, journal.compactedThrough)
	}
	lease := &CommandJournalBackupRetentionLease{journal: journal, sequence: sequence}
	if journal.backupRetentionLeases == nil {
		journal.backupRetentionLeases = make(map[*CommandJournalBackupRetentionLease]struct{})
	}
	journal.backupRetentionLeases[lease] = struct{}{}
	return lease, nil
}

func (journal *CommandJournal) snapshotCaptureBarrierWithBackupRetentionLease(acquired **CommandJournalBackupRetentionLease) snapshotCaptureBarrier {
	return func() (uint64, func(), error) {
		journal.mu.Lock()
		if journal.closed {
			journal.mu.Unlock()
			return 0, nil, ErrCommandJournalClosed
		}
		sequence := journal.lastSequenceLocked()
		lease, err := journal.acquireBackupRetentionLeaseLocked(sequence)
		if err != nil {
			journal.mu.Unlock()
			return 0, nil, err
		}
		*acquired = lease
		return sequence, journal.mu.Unlock, nil
	}
}

// Sequence returns the snapshot or replay coordinate protected by the lease.
func (lease *CommandJournalBackupRetentionLease) Sequence() uint64 {
	if lease == nil {
		return 0
	}
	return lease.sequence
}

// Release stops protecting the lease coordinate. It is safe to call more than
// once; only the first call for an active lease returns true.
func (lease *CommandJournalBackupRetentionLease) Release() bool {
	if lease == nil || lease.journal == nil {
		return false
	}
	journal := lease.journal
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.backupRetentionLeases == nil {
		return false
	}
	if _, exists := journal.backupRetentionLeases[lease]; !exists {
		return false
	}
	delete(journal.backupRetentionLeases, lease)
	if len(journal.backupRetentionLeases) == 0 {
		journal.backupRetentionLeases = nil
	}
	return true
}

func (journal *CommandJournal) backupRetentionThroughLocked() (uint64, bool) {
	var through uint64
	found := false
	for lease := range journal.backupRetentionLeases {
		sequence := lease.sequence
		if !found || sequence < through {
			through = sequence
			found = true
		}
	}
	return through, found
}

func (journal *CommandJournal) retentionThroughLocked() (uint64, bool) {
	through, found := journal.projectionRetentionThroughLocked()
	backupThrough, backupFound := journal.backupRetentionThroughLocked()
	if backupFound && (!found || backupThrough < through) {
		through = backupThrough
		found = true
	}
	return through, found
}
