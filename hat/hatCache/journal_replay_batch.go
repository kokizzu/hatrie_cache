package hatCache

import (
	"fmt"
	"strings"
)

const maxCommandJournalReplayScalarBatchRecords = 256

func (journal *CommandJournal) replayCommandJournalEntry(trie *HatTrie, entry commandJournalEntry) error {
	if journal.idempotency.enabled() && strings.TrimSpace(entry.Request.IdempotencyKey) != "" {
		response := trie.ExecuteCommand(entry.Request)
		if !response.OK {
			return fmt.Errorf("hatriecache: replay command journal entry %d failed: %s", entry.Sequence, response.Message)
		}
		check, err := newCommandIdempotencyCheck(entry.Request)
		if err != nil {
			return fmt.Errorf("hatriecache: replay command journal entry %d idempotency check failed: %s", entry.Sequence, err)
		}
		journal.idempotency.remember(check, response, entry.Sequence)
		return nil
	}
	if err := executeCommandForReplay(trie, entry.Request); err != nil {
		return fmt.Errorf("hatriecache: replay command journal entry %d failed: %s", entry.Sequence, err)
	}
	return nil
}

func (journal *CommandJournal) replayCommandJournalScalarBatch(trie *HatTrie, records []CommandJournalRecord, progress *commandJournalReplayProgressState) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) < minNativeCommandBatchSize {
		for _, record := range records {
			if err := journal.replayCommandJournalEntry(trie, commandJournalEntry{Sequence: record.Sequence, Request: record.Request}); err != nil {
				return err
			}
			if progress != nil {
				progress.markApplied(record.Sequence)
			}
		}
		return nil
	}

	trie.commandTransactionMu.RLock()
	applied, response, used := trie.executeJournalScalarBatch(records)
	trie.commandTransactionMu.RUnlock()
	if !used {
		for _, record := range records {
			if err := journal.replayCommandJournalEntry(trie, commandJournalEntry{Sequence: record.Sequence, Request: record.Request}); err != nil {
				return err
			}
			if progress != nil {
				progress.markApplied(record.Sequence)
			}
		}
		return nil
	}
	for index := 0; index < applied; index++ {
		if progress != nil {
			progress.markApplied(records[index].Sequence)
		}
	}
	if response.OK {
		return nil
	}
	failed := applied
	if failed >= len(records) {
		failed = len(records) - 1
	}
	return fmt.Errorf("hatriecache: replay command journal entry %d failed: %s", records[failed].Sequence, response.Message)
}
