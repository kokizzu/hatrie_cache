package hatriecache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const commandJournalVersion = 1

const maxCommandJournalBinaryRecordBytes = 1 << 30

const (
	DefaultCommandJournalTailLimit   = 1000
	MaxCommandJournalTailLimit       = 10000
	DefaultCommandJournalPullBatches = 100
	MaxCommandJournalPullBatches     = 1000
)

var ErrCommandJournalClosed = errors.New("hatriecache: command journal is closed")
var ErrCommandJournalCompacted = errors.New("hatriecache: command journal entries are compacted")
var ErrCommandJournalSequenceExhausted = errors.New("hatriecache: command journal sequence is exhausted")
var errCommandJournalBinaryRecordTooLarge = errors.New("hatriecache: command journal binary record is too large")

type CommandJournalRecord struct {
	Sequence uint64              `json:"sequence"`
	Request  CacheCommandRequest `json:"request"`
}

type CommandJournalTail struct {
	LastSequence     uint64                 `json:"last_sequence"`
	CompactedThrough uint64                 `json:"compacted_through,omitempty"`
	Limit            int                    `json:"limit,omitempty"`
	HasMore          bool                   `json:"has_more,omitempty"`
	Entries          []CommandJournalRecord `json:"entries"`
}

type CommandJournalPullResult struct {
	Source           string `json:"source"`
	AfterSequence    uint64 `json:"after_sequence"`
	LastSequence     uint64 `json:"last_sequence"`
	CompactedThrough uint64 `json:"compacted_through,omitempty"`
	Applied          int    `json:"applied"`
	AppliedThrough   uint64 `json:"applied_through"`
	Batches          int    `json:"batches,omitempty"`
	HasMore          bool   `json:"has_more,omitempty"`
}

type commandJournalEntry struct {
	Version    int                 `json:"version"`
	Sequence   uint64              `json:"sequence"`
	Checkpoint bool                `json:"checkpoint,omitempty"`
	Request    CacheCommandRequest `json:"request,omitempty"`
}

type CommandJournal struct {
	mu                sync.Mutex
	path              string
	format            CommandJournalFormat
	file              *os.File
	closed            bool
	nextSequence      uint64
	sequenceExhausted bool
}

type commandJournalAppendState struct {
	offset            int64
	nextSequence      uint64
	sequenceExhausted bool
}

func OpenCommandJournal(path string) (*CommandJournal, error) {
	return OpenCommandJournalWithFormat(path, DefaultCommandJournalFormat)
}

func OpenCommandJournalWithFormat(path string, format CommandJournalFormat) (*CommandJournal, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("hatriecache: journal path is required")
	}
	format, err := ParseCommandJournalFormat(string(format))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	var maxSequence uint64
	validBytes, err := scanCommandJournalEntries(path, func(entry commandJournalEntry) error {
		if entry.Sequence > maxSequence {
			maxSequence = entry.Sequence
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		// The append handle below will create the first journal file.
	} else if err != nil {
		return nil, err
	} else if validBytes < info.Size() {
		if err := os.Truncate(path, validBytes); err != nil {
			return nil, err
		}
	}
	file, err := openCommandJournalAppendFile(path)
	if err != nil {
		return nil, err
	}
	journal := &CommandJournal{path: path, format: format, file: file}
	journal.advanceSequenceLocked(maxSequence)
	if journal.nextSequence == 0 && !journal.sequenceExhausted {
		journal.nextSequence = 1
	}
	return journal, nil
}

func (journal *CommandJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()

	if journal.closed {
		return nil
	}
	journal.closed = true
	return journal.closeAppendFileLocked()
}

func (journal *CommandJournal) ExecuteCommand(trie *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	if !commandShouldJournal(request) {
		return trie.ExecuteCommand(request)
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()

	appendState, err := journal.appendLocked(normalizeJournalRequest(request, trie.currentTime()))
	if err != nil {
		return commandError(err.Error())
	}
	response := trie.ExecuteCommand(request)
	if !response.OK {
		if err := journal.rollbackAppendLocked(appendState); err != nil {
			return commandError(response.Message + "; failed to remove rejected journal entry: " + err.Error())
		}
	}
	return response
}

func (journal *CommandJournal) Replay(trie *HatTrie, afterSequence uint64) (uint64, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	if journal.closed {
		return 0, ErrCommandJournalClosed
	}
	var maxSequence uint64
	var compactedThrough uint64
	if _, err := scanCommandJournalEntries(journal.path, func(entry commandJournalEntry) error {
		if entry.Sequence > maxSequence {
			maxSequence = entry.Sequence
		}
		if entry.Checkpoint && entry.Sequence > compactedThrough {
			compactedThrough = entry.Sequence
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if afterSequence < compactedThrough {
		return 0, fmt.Errorf("%w: requested sequence %d is before compacted sequence %d", ErrCommandJournalCompacted, afterSequence, compactedThrough)
	}
	if _, err := scanCommandJournalEntries(journal.path, func(entry commandJournalEntry) error {
		if entry.Checkpoint {
			return nil
		}
		if entry.Sequence <= afterSequence {
			return nil
		}
		response := trie.ExecuteCommand(entry.Request)
		if !response.OK {
			return fmt.Errorf("hatriecache: replay command journal entry %d failed: %s", entry.Sequence, response.Message)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	journal.advanceSequenceLocked(maxSequence)
	return maxSequence, nil
}

func (journal *CommandJournal) Tail(afterSequence uint64, limit int) (CommandJournalTail, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	if journal.closed {
		return CommandJournalTail{}, ErrCommandJournalClosed
	}
	if limit < 0 {
		return CommandJournalTail{}, errors.New("hatriecache: journal tail limit must be non-negative")
	}
	if limit > MaxCommandJournalTailLimit {
		return CommandJournalTail{}, fmt.Errorf("hatriecache: journal tail limit must be <= %d", MaxCommandJournalTailLimit)
	}
	tail, err := readCommandJournalTail(journal.path, afterSequence, limit)
	if err != nil {
		return CommandJournalTail{}, err
	}
	if afterSequence < tail.CompactedThrough {
		tail.Entries = []CommandJournalRecord{}
		tail.HasMore = false
		return tail, fmt.Errorf("%w: requested sequence %d is before compacted sequence %d", ErrCommandJournalCompacted, afterSequence, tail.CompactedThrough)
	}
	return tail, nil
}

func (journal *CommandJournal) SaveSnapshot(trie *HatTrie, path string) error {
	return journal.SaveSnapshotWithFormat(trie, path, DefaultSnapshotFormat)
}

func (journal *CommandJournal) SaveSnapshotWithFormat(trie *HatTrie, path string, format SnapshotFormat) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	if journal.closed {
		return ErrCommandJournalClosed
	}
	sequence := journal.lastSequenceLocked()
	if err := trie.SaveSnapshotWithJournalSequenceAndFormat(path, sequence, format); err != nil {
		return err
	}
	return journal.compactLocked(sequence)
}

func (journal *CommandJournal) Sequence() uint64 {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	return journal.lastSequenceLocked()
}

func (journal *CommandJournal) appendLocked(request CacheCommandRequest) (commandJournalAppendState, error) {
	if err := journal.ensureAppendFileLocked(); err != nil {
		return commandJournalAppendState{}, err
	}
	info, err := journal.file.Stat()
	if err != nil {
		return commandJournalAppendState{}, err
	}
	appendState := commandJournalAppendState{
		offset:            info.Size(),
		nextSequence:      journal.nextSequence,
		sequenceExhausted: journal.sequenceExhausted,
	}
	sequence, err := journal.nextAppendSequenceLocked()
	if err != nil {
		return commandJournalAppendState{}, err
	}
	entry := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: sequence,
		Request:  request,
	}
	data, err := marshalCommandJournalEntry(entry, journal.format)
	if err != nil {
		return commandJournalAppendState{}, err
	}

	n, err := journal.file.Write(data)
	if err != nil {
		return commandJournalAppendState{}, journal.rollbackFailedAppendLocked(appendState, err)
	}
	if n != len(data) {
		return commandJournalAppendState{}, journal.rollbackFailedAppendLocked(appendState, io.ErrShortWrite)
	}
	if err := journal.file.Sync(); err != nil {
		return commandJournalAppendState{}, journal.rollbackFailedAppendLocked(appendState, err)
	}
	journal.markAppendedLocked(sequence)
	return appendState, nil
}

func (journal *CommandJournal) rollbackFailedAppendLocked(state commandJournalAppendState, cause error) error {
	if err := journal.rollbackAppendLocked(state); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (journal *CommandJournal) rollbackAppendLocked(state commandJournalAppendState) error {
	if journal.file == nil {
		return ErrCommandJournalClosed
	}
	if err := journal.file.Truncate(state.offset); err != nil {
		return err
	}
	if _, err := journal.file.Seek(state.offset, io.SeekStart); err != nil {
		return err
	}
	if err := journal.file.Sync(); err != nil {
		return err
	}
	journal.nextSequence = state.nextSequence
	journal.sequenceExhausted = state.sequenceExhausted
	return nil
}

func (journal *CommandJournal) compactLocked(throughSequence uint64) error {
	if err := journal.closeAppendFileLocked(); err != nil {
		return err
	}

	if err := writeCommandJournalCompactedWithFormat(journal.path, throughSequence, journal.format); err != nil {
		if reopenErr := journal.ensureAppendFileLocked(); reopenErr != nil {
			return errors.Join(err, reopenErr)
		}
		return err
	}
	return journal.ensureAppendFileLocked()
}

func writeCommandJournalCompacted(path string, throughSequence uint64) error {
	return writeCommandJournalCompactedWithFormat(path, throughSequence, DefaultCommandJournalFormat)
}

func writeCommandJournalCompactedWithFormat(path string, throughSequence uint64, format CommandJournalFormat) error {
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		if throughSequence > 0 {
			if err := writeCommandJournalEntry(writer, commandJournalEntry{
				Version:    commandJournalVersion,
				Sequence:   throughSequence,
				Checkpoint: true,
			}, format); err != nil {
				return err
			}
		}
		_, err := scanCommandJournalEntries(path, func(entry commandJournalEntry) error {
			if entry.Checkpoint || entry.Sequence <= throughSequence {
				return nil
			}
			return writeCommandJournalEntry(writer, entry, format)
		})
		return err
	})
}

func writeCommandJournalEntry(writer io.Writer, entry commandJournalEntry, format CommandJournalFormat) error {
	payload, err := marshalCommandJournalEntry(entry, format)
	if err != nil {
		return err
	}
	_, err = writer.Write(payload)
	return err
}

func (journal *CommandJournal) ensureAppendFileLocked() error {
	if journal.closed {
		return ErrCommandJournalClosed
	}
	if journal.file != nil {
		return nil
	}
	file, err := openCommandJournalAppendFile(journal.path)
	if err != nil {
		return err
	}
	journal.file = file
	return nil
}

func (journal *CommandJournal) closeAppendFileLocked() error {
	if journal.file == nil {
		return nil
	}
	file := journal.file
	journal.file = nil
	return file.Close()
}

func openCommandJournalAppendFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
}

func (journal *CommandJournal) lastSequenceLocked() uint64 {
	if journal.sequenceExhausted {
		return ^uint64(0)
	}
	if journal.nextSequence == 0 {
		return 0
	}
	return journal.nextSequence - 1
}

func (journal *CommandJournal) advanceSequenceLocked(maxSequence uint64) {
	if maxSequence == 0 || journal.sequenceExhausted || maxSequence <= journal.lastSequenceLocked() {
		return
	}
	if maxSequence == ^uint64(0) {
		journal.nextSequence = ^uint64(0)
		journal.sequenceExhausted = true
		return
	}
	journal.nextSequence = maxSequence + 1
}

func (journal *CommandJournal) nextAppendSequenceLocked() (uint64, error) {
	if journal.sequenceExhausted || journal.nextSequence == 0 {
		return 0, ErrCommandJournalSequenceExhausted
	}
	return journal.nextSequence, nil
}

func (journal *CommandJournal) markAppendedLocked(sequence uint64) {
	if sequence == ^uint64(0) {
		journal.sequenceExhausted = true
		return
	}
	journal.nextSequence = sequence + 1
}

func scanCommandJournalEntries(path string, visit func(commandJournalEntry) error) (int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var validBytes int64
	var previousSequence uint64
	var hasPreviousSequence bool
	reader := bufio.NewReader(file)
	for {
		entry, bytesRead, complete, err := readCommandJournalEntry(reader)
		if err != nil {
			return 0, err
		}
		if !complete {
			return validBytes, nil
		}
		if err := validateCommandJournalEntrySequence(previousSequence, hasPreviousSequence, entry); err != nil {
			return 0, err
		}
		if visit != nil {
			if err := visit(entry); err != nil {
				return validBytes, err
			}
		}
		validBytes += int64(bytesRead)
		previousSequence = entry.Sequence
		hasPreviousSequence = true
	}
}

func readCommandJournalEntry(reader *bufio.Reader) (commandJournalEntry, int, bool, error) {
	header, err := reader.Peek(len(commandJournalBinaryMagic))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return readCommandJournalJSONEntry(reader)
		}
		return commandJournalEntry{}, 0, false, err
	}
	if bytes.Equal(header, commandJournalBinaryMagic) {
		return readCommandJournalBinaryEntry(reader)
	}
	return readCommandJournalJSONEntry(reader)
}

func readCommandJournalJSONEntry(reader *bufio.Reader) (commandJournalEntry, int, bool, error) {
	record, bytesRead, complete, err := readCommandJournalJSONRecord(reader)
	if err != nil || !complete {
		return commandJournalEntry{}, bytesRead, complete, err
	}
	entry, err := decodeCommandJournalEntryJSON(record)
	if err != nil {
		return commandJournalEntry{}, 0, false, err
	}
	return entry, bytesRead, true, nil
}

func readCommandJournalJSONRecord(reader *bufio.Reader) ([]byte, int, bool, error) {
	line, err := reader.ReadBytes('\n')
	if len(line) > 0 && line[len(line)-1] == '\n' {
		return line, len(line), true, nil
	}
	if errors.Is(err, io.EOF) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	return nil, 0, false, nil
}

func readCommandJournalBinaryEntry(reader *bufio.Reader) (commandJournalEntry, int, bool, error) {
	bytesRead, err := reader.Discard(len(commandJournalBinaryMagic))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return commandJournalEntry{}, 0, false, nil
		}
		return commandJournalEntry{}, 0, false, err
	}
	size, sizeBytes, complete, err := readCommandJournalRecordSize(reader)
	if err != nil || !complete {
		return commandJournalEntry{}, 0, complete, err
	}
	bytesRead += len(sizeBytes)
	if err := validateCommandJournalBinaryRecordSize(size); err != nil {
		return commandJournalEntry{}, 0, false, err
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(reader, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return commandJournalEntry{}, 0, false, nil
		}
		return commandJournalEntry{}, 0, false, err
	}
	bytesRead += len(payload)
	entry, err := decodeCommandJournalEntryBinaryPayload(payload)
	if err != nil {
		return commandJournalEntry{}, 0, false, err
	}
	return entry, bytesRead, true, nil
}

func validateCommandJournalBinaryRecordSize(size uint64) error {
	if size > maxCommandJournalBinaryRecordBytes || size > uint64(int(^uint(0)>>1)) {
		return errCommandJournalBinaryRecordTooLarge
	}
	return nil
}

func readCommandJournalRecordSize(reader *bufio.Reader) (uint64, []byte, bool, error) {
	var raw []byte
	for shift := uint(0); shift < 64; shift += 7 {
		value, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, nil, false, nil
			}
			return 0, nil, false, err
		}
		raw = append(raw, value)
		if value < 0x80 {
			size, n := binary.Uvarint(raw)
			if n <= 0 {
				return 0, nil, false, errors.New("hatriecache: invalid binary command journal record size")
			}
			return size, raw, true, nil
		}
	}
	return 0, nil, false, errors.New("hatriecache: invalid binary command journal record size")
}

func readCommandJournalTail(path string, afterSequence uint64, limit int) (CommandJournalTail, error) {
	tail := CommandJournalTail{Entries: []CommandJournalRecord{}}
	if limit > 0 {
		tail.Limit = limit
		tail.Entries = make([]CommandJournalRecord, 0, limit)
	}

	if _, err := scanCommandJournalEntries(path, func(entry commandJournalEntry) error {
		if entry.Sequence > tail.LastSequence {
			tail.LastSequence = entry.Sequence
		}
		if entry.Checkpoint && entry.Sequence > tail.CompactedThrough {
			tail.CompactedThrough = entry.Sequence
		}
		if entry.Checkpoint || entry.Sequence <= afterSequence {
			return nil
		}
		if limit > 0 && len(tail.Entries) >= limit {
			tail.HasMore = true
			return nil
		}
		tail.Entries = append(tail.Entries, CommandJournalRecord{
			Sequence: entry.Sequence,
			Request:  entry.Request,
		})
		return nil
	}); err != nil {
		return CommandJournalTail{}, err
	}
	return tail, nil
}

func validateCommandJournalEntryRequest(entry commandJournalEntry) error {
	if entry.Checkpoint {
		if commandJournalRequestEmpty(entry.Request) {
			return nil
		}
		return errors.New("hatriecache: command journal checkpoint cannot include a request")
	}
	if !commandShouldJournal(entry.Request) {
		return errors.New("hatriecache: command journal entry request is not journalable")
	}
	return nil
}

func commandJournalRequestEmpty(request CacheCommandRequest) bool {
	return strings.TrimSpace(request.Command) == "" &&
		strings.TrimSpace(request.Key) == "" &&
		request.Value == "" &&
		len(request.Values) == 0 &&
		request.Subkey == "" &&
		len(request.Pairs) == 0 &&
		request.Priority == nil &&
		request.TTLSeconds == nil &&
		request.UnixSeconds == nil
}

func validateCommandJournalEntrySequence(previous uint64, hasPrevious bool, entry commandJournalEntry) error {
	if !hasPrevious {
		if entry.Checkpoint || entry.Sequence == 1 {
			return nil
		}
		return fmt.Errorf("hatriecache: command journal starts at sequence %d without checkpoint", entry.Sequence)
	}
	if previous == ^uint64(0) {
		return fmt.Errorf("hatriecache: command journal sequence %d follows exhausted sequence", entry.Sequence)
	}
	expected := previous + 1
	if entry.Sequence != expected {
		return fmt.Errorf("hatriecache: command journal sequence %d does not continue after %d", entry.Sequence, previous)
	}
	return nil
}

func commandShouldJournal(request CacheCommandRequest) bool {
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	key := strings.TrimSpace(request.Key)
	if command == "" || key == "" {
		return false
	}

	switch command {
	case "SET", "SETSTR":
		response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds)
		return !ok || response.OK
	case "SETX", "SETSTRX":
		_, ok := requirePositiveTTL(request.TTLSeconds)
		return ok
	case "SETINT":
		if _, ok := parseCommandInt32(request.Value); !ok {
			return false
		}
		response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds)
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
	case "CREATEBF", "RESERVEBF", "BFRESERVE":
		_, _, err := commandBloomFilterConfig(request)
		return err == nil
	case "ADDBF", "BFADD":
		_, ok := commandSliceValues(request)
		return ok
	case "CREATECF", "RESERVECF", "CFRESERVE":
		_, _, err := commandCuckooFilterConfig(request)
		return err == nil
	case "ADDCF", "CFADD", "DELCF", "REMCF", "CFDEL":
		_, ok := commandSliceValues(request)
		return ok
	case "CREATEXF", "RESERVEXF", "XFRESERVE", "CREATEXOR":
		_, err := commandXorFilterExpectedItems(request)
		return err == nil
	case "ADDXF", "XFADD":
		_, ok := commandSliceValues(request)
		return ok
	case "BUILDXF", "XFBUILD":
		return true
	case "CREATERB", "CREATEROARING", "RBRESERVE":
		return true
	case "ADDRB", "RBADD", "REMRB", "DELRB", "RBREM", "RBDEL":
		_, err := roaringBitmapValuesFromCommand(request)
		return err == nil
	case "CREATESB", "CREATESPARSEBITSET", "SBRESERVE":
		return true
	case "ADDSB", "SBADD", "REMSB", "DELSB", "SBREM", "SBDEL":
		_, err := sparseBitsetValuesFromCommand(request)
		return err == nil
	case "CREATERT", "CREATERADIX", "RTCREATE":
		return true
	case "PUTRT", "RTPUT":
		_, ok := commandRadixTreeFields(request)
		return ok
	case "DELRT", "REMRT", "RTDEL", "RTREM":
		return strings.TrimSpace(request.Subkey) != ""
	case "CREATECMS", "RESERVECMS", "CMSRESERVE":
		_, _, err := commandCountMinSketchConfig(request)
		return err == nil
	case "INCRCMS", "ADDCMS", "CMSADD":
		if _, ok := commandSliceValues(request); !ok {
			return false
		}
		_, err := commandCountMinSketchIncrement(request)
		return err == nil
	case "CREATEHLL", "RESERVEHLL", "HLLRESERVE":
		_, err := commandHyperLogLogPrecision(request)
		return err == nil
	case "ADDHLL", "HLLADD":
		_, ok := commandSliceValues(request)
		return ok
	case "CREATETOPK", "RESERVETOPK", "TOPKRESERVE":
		_, err := commandTopKCapacity(request)
		return err == nil
	case "ADDTOPK", "TOPKADD":
		if _, ok := commandSliceValues(request); !ok {
			return false
		}
		_, err := commandTopKCount(request)
		return err == nil
	case "CREATERS", "CREATESAMPLE", "RESERVERS", "RSRESERVE":
		_, err := commandReservoirSampleCapacity(request)
		return err == nil
	case "ADDRS", "RSADD":
		_, ok := commandSliceValues(request)
		return ok
	case "CREATEQ", "CREATEQS", "CREATEQUANTILE", "RESERVEQ", "QSRESERVE":
		_, err := commandQuantileSketchEpsilon(request)
		return err == nil
	case "ADDQ", "ADDQS", "QADD", "QSADD":
		_, err := quantileSketchValuesFromCommand(request)
		return err == nil
	case "CREATEFW", "CREATEFENWICK", "RESERVEFW", "FWRESERVE":
		_, err := commandFenwickTreeSize(request)
		return err == nil
	case "ADDFW", "FWADD":
		_, _, err := commandFenwickTreeUpdate(request)
		return err == nil
	default:
		return false
	}
}

func normalizeJournalRequest(request CacheCommandRequest, now time.Time) CacheCommandRequest {
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	out := request
	switch command {
	case "SET", "SETSTR":
		out.Command = command
		normalizeRelativeTTL(&out, now)
	case "SETX", "SETSTRX":
		out.Command = "SETSTR"
		normalizeRelativeTTL(&out, now)
	case "SETINT":
		out.Command = command
		normalizeRelativeTTL(&out, now)
	case "SETINTX":
		out.Command = "SETINT"
		normalizeRelativeTTL(&out, now)
	case "EXPIRE":
		out.Command = "EXPIREAT"
		normalizeRelativeTTL(&out, now)
	default:
		out.Command = command
	}
	return out
}

func normalizeRelativeTTL(request *CacheCommandRequest, now time.Time) {
	if request.TTLSeconds == nil {
		return
	}
	expiresAt := now.Add(time.Duration(*request.TTLSeconds) * time.Second).Unix()
	request.TTLSeconds = nil
	request.UnixSeconds = &expiresAt
}
