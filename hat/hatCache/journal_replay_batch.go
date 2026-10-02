package hatCache

import "fmt"

func (journal *CommandJournal) replayScalarRecords(trie *HatTrie, records []CommandJournalRecord, progress *commandJournalReplayProgressState) error {
	if len(records) == 0 {
		return nil
	}
	applied, response, used := trie.executeJournalScalarBatchCanonical(records)
	if !used {
		for _, record := range records {
			if err := executeCommandForReplay(trie, record.Request); err != nil {
				return fmt.Errorf("hatriecache: replay command journal entry %d failed: %s", record.Sequence, err)
			}
			if progress != nil {
				progress.markApplied(record.Sequence)
			}
		}
		return nil
	}
	for index := 0; index < applied && index < len(records); index++ {
		if progress != nil {
			progress.markApplied(records[index].Sequence)
		}
	}
	if response.OK {
		return nil
	}
	failureIndex := applied
	if failureIndex >= len(records) {
		failureIndex = len(records) - 1
	}
	return fmt.Errorf("hatriecache: replay command journal entry %d failed: %s", records[failureIndex].Sequence, response.Message)
}
