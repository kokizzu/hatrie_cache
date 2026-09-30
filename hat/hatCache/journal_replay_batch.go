package hatCache

import (
	"fmt"
	"strings"
	"unsafe"
)

const maxCommandJournalReplayScalarBatchRecords = 256

type commandJournalReplayScalarBatchDescriptor struct {
	sequence    uint64
	family      nativeCommandBatchFamily
	keyStart    int
	keyLength   int
	valueStart  int
	valueLength int
}

type commandJournalReplayScalarBatchArena struct {
	data        []byte
	descriptors [maxCommandJournalReplayScalarBatchRecords]commandJournalReplayScalarBatchDescriptor
	count       int
}

func (arena *commandJournalReplayScalarBatchArena) append(entry commandJournalEntry, family nativeCommandBatchFamily) {
	if arena.count >= len(arena.descriptors) {
		panic("hatriecache: scalar replay batch arena is full")
	}
	keyStart := len(arena.data)
	arena.data = append(arena.data, entry.Request.Key...)
	valueStart := len(arena.data)
	arena.data = append(arena.data, entry.Request.Value...)
	arena.descriptors[arena.count] = commandJournalReplayScalarBatchDescriptor{
		sequence:    entry.Sequence,
		family:      family,
		keyStart:    keyStart,
		keyLength:   valueStart - keyStart,
		valueStart:  valueStart,
		valueLength: len(arena.data) - valueStart,
	}
	arena.count++
}

func (arena *commandJournalReplayScalarBatchArena) materialize(records []CommandJournalRecord) []CommandJournalRecord {
	records = records[:arena.count]
	for index, descriptor := range arena.descriptors[:arena.count] {
		command := "SET"
		if descriptor.family == nativeCommandBatchSetCounter {
			command = "SETINT"
		}
		value := borrowReplayArenaString(arena.data[descriptor.valueStart : descriptor.valueStart+descriptor.valueLength])
		if descriptor.family == nativeCommandBatchSetString {
			value = string(arena.data[descriptor.valueStart : descriptor.valueStart+descriptor.valueLength])
		}
		records[index] = CommandJournalRecord{
			Sequence: descriptor.sequence,
			Request: CacheCommandRequest{
				Command: command,
				Key:     borrowReplayArenaString(arena.data[descriptor.keyStart : descriptor.keyStart+descriptor.keyLength]),
				Value:   value,
			},
		}
	}
	return records
}

func (arena *commandJournalReplayScalarBatchArena) reset() {
	arena.count = 0
	arena.data = arena.data[:0]
}

func borrowReplayArenaString(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(value), len(value))
}

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
