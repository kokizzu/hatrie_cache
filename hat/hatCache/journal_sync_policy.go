package hatCache

import (
	"time"

	"hatrie_cache/hat/hatJournal"
)

func commandJournalSyncModeStrength(mode hatJournal.SyncMode) int {
	switch mode {
	case hatJournal.SyncModeSynchronous:
		return 3
	case hatJournal.SyncModePeriodic:
		return 2
	case hatJournal.SyncModeDisabled:
		return 1
	default:
		return 3
	}
}

func strongerCommandJournalSyncMode(current, candidate hatJournal.SyncMode) hatJournal.SyncMode {
	if commandJournalSyncModeStrength(candidate) > commandJournalSyncModeStrength(current) {
		return candidate
	}
	return current
}

func (journal *CommandJournal) syncModeForRequest(request CacheCommandRequest) hatJournal.SyncMode {
	mode := journal.syncPolicy.ModeForKey(request.Key)
	for _, nested := range request.Batch {
		mode = strongerCommandJournalSyncMode(mode, journal.syncModeForRequest(nested))
	}
	return mode
}

func (journal *CommandJournal) syncModeForRequests(requests []CacheCommandRequest) hatJournal.SyncMode {
	mode := hatJournal.SyncModeDisabled
	for _, request := range requests {
		mode = strongerCommandJournalSyncMode(mode, journal.syncModeForRequest(request))
	}
	return mode
}

func (journal *CommandJournal) syncModeForJobs(jobs []*commandJournalJob) hatJournal.SyncMode {
	mode := hatJournal.SyncModeDisabled
	for _, job := range jobs {
		if job != nil {
			mode = strongerCommandJournalSyncMode(mode, journal.syncModeForRequest(job.journalRequest))
		}
	}
	return mode
}

func (journal *CommandJournal) syncModeForIdempotentEntries(entries []commandJournalIdempotentGroupEntry) hatJournal.SyncMode {
	mode := hatJournal.SyncModeDisabled
	for index := range entries {
		if entries[index].job != nil {
			mode = strongerCommandJournalSyncMode(mode, journal.syncModeForRequest(entries[index].job.journalRequest))
		}
	}
	return mode
}

func (journal *CommandJournal) syncModeForJournalRecords(records []CommandJournalRecord) hatJournal.SyncMode {
	mode := hatJournal.SyncModeDisabled
	for _, record := range records {
		mode = strongerCommandJournalSyncMode(mode, journal.syncModeForRequest(record.Request))
	}
	return mode
}

func (journal *CommandJournal) syncModeForCompactRecords(records []compactCommandJournalRecord) hatJournal.SyncMode {
	mode := hatJournal.SyncModeDisabled
	for _, record := range records {
		mode = strongerCommandJournalSyncMode(mode, journal.syncModeForRequest(record.request()))
	}
	return mode
}

func (journal *CommandJournal) syncForModeLocked(mode hatJournal.SyncMode) error {
	switch mode {
	case hatJournal.SyncModeDisabled:
		return nil
	case hatJournal.SyncModePeriodic:
		if journal.lastSyncedSequence >= journal.lastSequenceLocked() {
			return nil
		}
		if journal.lastSyncAt.IsZero() || journal.lastSyncAt.Add(journal.syncPolicy.PeriodicInterval).After(time.Now()) {
			return nil
		}
	}
	return journal.syncLocked()
}
