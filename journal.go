package hatriecache

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const commandJournalVersion = 1

type commandJournalEntry struct {
	Version    int                 `json:"version"`
	Sequence   uint64              `json:"sequence"`
	Checkpoint bool                `json:"checkpoint,omitempty"`
	Request    CacheCommandRequest `json:"request,omitempty"`
}

type CommandJournal struct {
	mu           sync.Mutex
	path         string
	nextSequence uint64
}

func OpenCommandJournal(path string) (*CommandJournal, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("hatriecache: journal path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}

	entries, validBytes, err := readCommandJournalEntriesWithEnd(path)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(path); err != nil {
		return nil, err
	} else if validBytes < info.Size() {
		if err := os.Truncate(path, validBytes); err != nil {
			return nil, err
		}
	}
	var maxSequence uint64
	for _, entry := range entries {
		if entry.Sequence > maxSequence {
			maxSequence = entry.Sequence
		}
	}
	return &CommandJournal{path: path, nextSequence: maxSequence + 1}, nil
}

func (journal *CommandJournal) ExecuteCommand(trie *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	if !commandShouldJournal(request) {
		return trie.ExecuteCommand(request)
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()

	if err := journal.appendLocked(request); err != nil {
		return commandError(err.Error())
	}
	return trie.ExecuteCommand(request)
}

func (journal *CommandJournal) Replay(trie *HatTrie, afterSequence uint64) (uint64, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	entries, err := readCommandJournalEntries(journal.path)
	if err != nil {
		return 0, err
	}
	var maxSequence uint64
	for _, entry := range entries {
		if entry.Sequence > maxSequence {
			maxSequence = entry.Sequence
		}
		if entry.Checkpoint {
			continue
		}
		if entry.Sequence <= afterSequence {
			continue
		}
		trie.ExecuteCommand(entry.Request)
	}
	if maxSequence >= journal.nextSequence {
		journal.nextSequence = maxSequence + 1
	}
	return maxSequence, nil
}

func (journal *CommandJournal) SaveSnapshot(trie *HatTrie, path string) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	sequence := journal.lastSequenceLocked()
	if err := trie.SaveSnapshotWithJournalSequence(path, sequence); err != nil {
		return err
	}
	return journal.compactLocked(sequence)
}

func (journal *CommandJournal) Sequence() uint64 {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	return journal.lastSequenceLocked()
}

func (journal *CommandJournal) appendLocked(request CacheCommandRequest) error {
	entry := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: journal.nextSequence,
		Request:  request,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	file, err := os.OpenFile(journal.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	journal.nextSequence++
	return nil
}

func (journal *CommandJournal) compactLocked(throughSequence uint64) error {
	entries, err := readCommandJournalEntries(journal.path)
	if err != nil {
		return err
	}

	remaining := make([]commandJournalEntry, 0, len(entries)+1)
	if throughSequence > 0 {
		remaining = append(remaining, commandJournalEntry{
			Version:    commandJournalVersion,
			Sequence:   throughSequence,
			Checkpoint: true,
		})
	}
	for _, entry := range entries {
		if !entry.Checkpoint && entry.Sequence > throughSequence {
			remaining = append(remaining, entry)
		}
	}

	data := []byte{}
	for _, entry := range remaining {
		payload, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		data = append(data, payload...)
		data = append(data, '\n')
	}
	return writeFileAtomic(journal.path, data)
}

func (journal *CommandJournal) lastSequenceLocked() uint64 {
	if journal.nextSequence == 0 {
		return 0
	}
	return journal.nextSequence - 1
}

func readCommandJournalEntries(path string) ([]commandJournalEntry, error) {
	entries, _, err := readCommandJournalEntriesWithEnd(path)
	return entries, err
}

func readCommandJournalEntriesWithEnd(path string) ([]commandJournalEntry, int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	var entries []commandJournalEntry
	var validBytes int64
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			entry, err := decodeCommandJournalEntry(line)
			if err != nil {
				return nil, 0, err
			}
			entries = append(entries, entry)
			validBytes += int64(len(line))
		}

		if errors.Is(err, io.EOF) {
			return entries, validBytes, nil
		}
		if err != nil {
			return nil, 0, err
		}
	}
}

func decodeCommandJournalEntry(data []byte) (commandJournalEntry, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()

	var entry commandJournalEntry
	if err := decoder.Decode(&entry); err != nil {
		return commandJournalEntry{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return commandJournalEntry{}, errors.New("hatriecache: invalid journal JSON")
		}
		return commandJournalEntry{}, err
	}
	if entry.Version != commandJournalVersion {
		return commandJournalEntry{}, errors.New("hatriecache: unsupported journal version")
	}
	if entry.Sequence == 0 {
		return commandJournalEntry{}, errors.New("hatriecache: invalid journal sequence")
	}
	return entry, nil
}

func commandShouldJournal(request CacheCommandRequest) bool {
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	key := strings.TrimSpace(request.Key)
	if command == "" || key == "" {
		return false
	}

	switch command {
	case "SET", "SETSTR":
		response, ok := validateOptionalCommandTTL(request.TTLSeconds)
		return !ok || response.OK
	case "SETX", "SETSTRX":
		_, ok := requirePositiveTTL(request.TTLSeconds)
		return ok
	case "SETINT":
		if _, ok := parseCommandInt32(request.Value); !ok {
			return false
		}
		response, ok := validateOptionalCommandTTL(request.TTLSeconds)
		return !ok || response.OK
	case "SETINTX":
		if _, ok := parseCommandInt32(request.Value); !ok {
			return false
		}
		_, ok := requirePositiveTTL(request.TTLSeconds)
		return ok
	case "INC":
		_, ok := parseCommandIncrement(request.Value)
		return ok
	case "DEL":
		return true
	case "INTERNALSET":
		_, err := commandSnapshotOperation(key, request.Value)
		return err == nil
	case "INTERNALDEL":
		return true
	case "EXPIRE":
		_, ok := requirePositiveTTL(request.TTLSeconds)
		return ok
	case "EXPIREAT":
		_, ok := commandExpireAt(request)
		return ok
	case "PUTMAP":
		_, ok := commandMapFields(request)
		return ok
	case "TAKEMAP":
		return strings.TrimSpace(request.Subkey) != ""
	case "PUSHSLICE":
		_, ok := commandSliceValues(request)
		return ok
	case "POPSLICE", "SHIFTSLICE":
		return true
	case "ADDSET", "REMSET":
		_, ok := commandSliceValues(request)
		return ok
	case "PUSHPQ", "PUSHPRIORITY":
		if _, ok := commandPriority(request); !ok {
			return false
		}
		_, ok := commandSliceValues(request)
		return ok
	case "POPPQ", "POPPRIORITY":
		return true
	default:
		return false
	}
}
