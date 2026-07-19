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
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	commandJournalVersion              = 1
	commandJournalBinaryPayloadVersion = 2
)

const maxCommandJournalBinaryRecordBytes = 1 << 30
const maxCommandJournalJSONRecordBytes = 1 << 30
const maxCommandJournalReusablePayloadBufferBytes = 1 << 20

const (
	DefaultCommandJournalTailLimit                      = 1000
	MaxCommandJournalTailLimit                          = 10000
	DefaultCommandJournalPullBatches                    = 100
	MaxCommandJournalPullBatches                        = 1000
	DefaultJournalGroupCommitWindow       time.Duration = 0
	DefaultJournalGroupCommitMaxBatch                   = 64
	MaxJournalGroupCommitBatch                          = 4096
	DefaultCommandJournalSegmentMaxBytes  int64         = 64 << 20
	DefaultCommandJournalRetainedSegments               = 16
	MaxCommandJournalRetainedSegments                   = 1024
)

var ErrCommandJournalClosed = errors.New("hatriecache: command journal is closed")
var ErrCommandJournalCompacted = errors.New("hatriecache: command journal entries are compacted")
var ErrCommandJournalSequenceExhausted = errors.New("hatriecache: command journal sequence is exhausted")

// ErrNilCommandJournal reports a nil journal passed to a command journal API.
var ErrNilCommandJournal = errors.New("hatriecache: command journal is nil")
var errCommandJournalBinaryRecordTooLarge = errors.New("hatriecache: command journal binary record is too large")
var errCommandJournalJSONRecordTooLarge = errors.New("hatriecache: command journal JSON record is too large")

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
	FullSyncFallback bool   `json:"full_sync_fallback,omitempty"`
}

type commandJournalEntry struct {
	Version    int                 `json:"version"`
	Sequence   uint64              `json:"sequence"`
	Checkpoint bool                `json:"checkpoint,omitempty"`
	Request    CacheCommandRequest `json:"request,omitempty"`
}

// CommandJournalOptions configures journal encoding, durable group commit, and
// optional bounded segment rotation. SegmentMaxBytes zero keeps one file.
// A zero window opportunistically batches callers already queued; a positive
// window waits up to that duration for more callers. A maximum batch of one
// disables group commit and uses the immediate-fsync path.
type CommandJournalOptions struct {
	Format              CommandJournalFormat
	GroupCommitWindow   time.Duration
	GroupCommitMaxBatch int
	SegmentMaxBytes     int64
	RetainedSegments    int
}

type commandJournalJob struct {
	trie           *HatTrie
	request        CacheCommandRequest
	journalRequest CacheCommandRequest
	operation      *snapshotOperation
	prepared       bool
	result         chan CacheCommandResponse
}

type CommandJournal struct {
	mu                  sync.Mutex
	snapshotMu          sync.Mutex
	submitMu            sync.RWMutex
	closeOnce           sync.Once
	path                string
	format              CommandJournalFormat
	file                *os.File
	closed              bool
	accepting           bool
	nextSequence        uint64
	sequenceExhausted   bool
	groupCommitWindow   time.Duration
	groupCommitMaxBatch int
	segmentMaxBytes     int64
	retainedSegments    int
	activeSegmentStart  uint64
	groupCommitJobs     chan *commandJournalJob
	groupCommitDone     chan struct{}
	closeDone           chan struct{}
	closeErr            error
	syncHook            func() error
}

type commandJournalAppendState struct {
	offset            int64
	nextSequence      uint64
	sequenceExhausted bool
}

func OpenCommandJournal(path string) (*CommandJournal, error) {
	return OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
	})
}

func OpenCommandJournalWithFormat(path string, format CommandJournalFormat) (*CommandJournal, error) {
	return OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              format,
		GroupCommitWindow:   DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
	})
}

func OpenCommandJournalWithOptions(path string, options CommandJournalOptions) (*CommandJournal, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("hatriecache: journal path is required")
	}
	format, err := ParseCommandJournalFormat(string(options.Format))
	if err != nil {
		return nil, err
	}
	if options.GroupCommitWindow < 0 {
		return nil, errors.New("hatriecache: journal group commit window must be non-negative")
	}
	if options.GroupCommitMaxBatch < 1 {
		return nil, errors.New("hatriecache: journal group commit max batch must be positive")
	}
	if options.GroupCommitMaxBatch > MaxJournalGroupCommitBatch {
		return nil, fmt.Errorf("hatriecache: journal group commit max batch must be <= %d", MaxJournalGroupCommitBatch)
	}
	if options.SegmentMaxBytes < 0 {
		return nil, errors.New("hatriecache: journal segment max bytes must be non-negative")
	}
	if options.RetainedSegments < 0 {
		return nil, errors.New("hatriecache: retained journal segments must be non-negative")
	}
	if options.RetainedSegments > MaxCommandJournalRetainedSegments {
		return nil, fmt.Errorf("hatriecache: retained journal segments must be <= %d", MaxCommandJournalRetainedSegments)
	}
	if options.SegmentMaxBytes > 0 && options.RetainedSegments == 0 {
		return nil, errors.New("hatriecache: retained journal segments must be positive when segmentation is enabled")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	var maxSequence uint64
	validBytes, err := scanCommandJournalSet(path, options.SegmentMaxBytes > 0, func(entry commandJournalEntry) error {
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
	if options.SegmentMaxBytes > 0 && validBytes == 0 && maxSequence > 0 {
		if err := writeCommandJournalCheckpointWithFormat(path, maxSequence, format); err != nil {
			return nil, err
		}
	}
	file, err := openCommandJournalAppendFile(path)
	if err != nil {
		return nil, err
	}
	journal := &CommandJournal{
		path:                path,
		format:              format,
		file:                file,
		accepting:           true,
		groupCommitWindow:   options.GroupCommitWindow,
		groupCommitMaxBatch: options.GroupCommitMaxBatch,
		segmentMaxBytes:     options.SegmentMaxBytes,
		retainedSegments:    options.RetainedSegments,
		closeDone:           make(chan struct{}),
	}
	journal.advanceSequenceLocked(maxSequence)
	if journal.nextSequence == 0 && !journal.sequenceExhausted {
		journal.nextSequence = 1
	}
	journal.activeSegmentStart, err = commandJournalActiveSegmentStart(path, journal.lastSequenceLocked())
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if journal.segmented() {
		if err := journal.pruneSegmentsLocked(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if journal.groupCommitEnabled() {
		journal.groupCommitJobs = make(chan *commandJournalJob, journal.groupCommitMaxBatch)
		journal.groupCommitDone = make(chan struct{})
		go journal.runGroupCommit()
	}
	return journal, nil
}

func (journal *CommandJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.closeOnce.Do(func() {
		journal.snapshotMu.Lock()
		defer journal.snapshotMu.Unlock()

		journal.submitMu.Lock()
		journal.accepting = false
		if journal.groupCommitJobs != nil {
			close(journal.groupCommitJobs)
		}
		journal.submitMu.Unlock()
		if journal.groupCommitDone != nil {
			<-journal.groupCommitDone
		}

		journal.mu.Lock()
		journal.closed = true
		journal.closeErr = journal.closeAppendFileLocked()
		journal.mu.Unlock()
		close(journal.closeDone)
	})
	<-journal.closeDone
	return journal.closeErr
}

func (journal *CommandJournal) groupCommitEnabled() bool {
	return journal.groupCommitMaxBatch > 1
}

func (journal *CommandJournal) ExecuteCommand(trie *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	if journal == nil {
		return commandError(ErrNilCommandJournal.Error())
	}
	if trie == nil {
		return commandError(ErrNilHatTrie.Error())
	}
	if !commandShouldJournal(request) {
		return trie.ExecuteCommand(request)
	}
	journalRequest := normalizeJournalRequest(request, trie.currentTime())
	if journal.groupCommitEnabled() {
		return journal.submitGroupCommit(&commandJournalJob{
			trie:           trie,
			request:        request,
			journalRequest: journalRequest,
			result:         make(chan CacheCommandResponse, 1),
		})
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()

	appendState, err := journal.appendLocked(journalRequest)
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

func (journal *CommandJournal) submitGroupCommit(job *commandJournalJob) CacheCommandResponse {
	journal.submitMu.RLock()
	if !journal.accepting {
		journal.submitMu.RUnlock()
		return commandError(ErrCommandJournalClosed.Error())
	}
	journal.groupCommitJobs <- job
	journal.submitMu.RUnlock()
	return <-job.result
}

func (journal *CommandJournal) runGroupCommit() {
	defer close(journal.groupCommitDone)
	for {
		first, ok := <-journal.groupCommitJobs
		if !ok {
			return
		}
		batch := make([]*commandJournalJob, 1, journal.groupCommitMaxBatch)
		batch[0] = first
		if journal.groupCommitWindow == 0 {
			runtime.Gosched()
			channelClosed := false
		drain:
			for len(batch) < journal.groupCommitMaxBatch {
				select {
				case job, open := <-journal.groupCommitJobs:
					if !open {
						channelClosed = true
						break drain
					}
					batch = append(batch, job)
				default:
					break drain
				}
			}
			journal.processGroupCommit(batch)
			if channelClosed {
				return
			}
			continue
		}
		timer := time.NewTimer(journal.groupCommitWindow)
		channelClosed := false
	collect:
		for len(batch) < journal.groupCommitMaxBatch {
			select {
			case job, open := <-journal.groupCommitJobs:
				if !open {
					channelClosed = true
					break collect
				}
				batch = append(batch, job)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		journal.processGroupCommit(batch)
		if channelClosed {
			return
		}
	}
}

func (journal *CommandJournal) processGroupCommit(batch []*commandJournalJob) {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	pending := batch
	for len(pending) > 0 {
		batchState, err := journal.currentAppendStateLocked()
		if err != nil {
			completeCommandJournalJobs(pending, commandError(err.Error()))
			return
		}
		states := make([]commandJournalAppendState, len(pending))
		offset := batchState.offset
		for idx, job := range pending {
			states[idx] = commandJournalAppendState{
				offset:            offset,
				nextSequence:      journal.nextSequence,
				sequenceExhausted: journal.sequenceExhausted,
			}
			var written int
			written, err = journal.writeWithoutSyncLocked(job.journalRequest)
			if err != nil {
				err = journal.rollbackPreparedBatchLocked(batchState, err)
				completeCommandJournalJobs(pending, commandError(err.Error()))
				return
			}
			offset += int64(written)
		}
		if err := journal.syncLocked(); err != nil {
			err = journal.rollbackPreparedBatchLocked(batchState, err)
			completeCommandJournalJobs(pending, commandError(err.Error()))
			return
		}

		rejected := -1
		for idx, job := range pending {
			response := job.execute()
			if response.OK {
				job.result <- response
				continue
			}
			rollbackErr := journal.rollbackAppendLocked(states[idx])
			if rollbackErr != nil {
				response.Message += "; failed to remove rejected journal entries: " + rollbackErr.Error()
			}
			job.result <- response
			rejected = idx
			if rollbackErr != nil {
				completeCommandJournalJobs(pending[idx+1:], commandError(rollbackErr.Error()))
				return
			}
			break
		}
		if rejected < 0 {
			return
		}
		pending = pending[rejected+1:]
	}
}

func (job *commandJournalJob) execute() CacheCommandResponse {
	if job.prepared {
		return executePreparedInternalReplicationCommand(job.trie, job.request, job.operation)
	}
	return job.trie.ExecuteCommand(job.request)
}

func completeCommandJournalJobs(jobs []*commandJournalJob, response CacheCommandResponse) {
	for _, job := range jobs {
		job.result <- response
	}
}

func (journal *CommandJournal) executeJournalRecordsBatch(trie *HatTrie, records []CommandJournalRecord) (int, CacheCommandResponse) {
	if journal == nil {
		return 0, commandError(ErrNilCommandJournal.Error())
	}
	if trie == nil {
		return 0, commandError(ErrNilHatTrie.Error())
	}
	if len(records) == 0 {
		return 0, CacheCommandResponse{OK: true}
	}
	batchable := true
	for _, record := range records {
		if !commandShouldJournal(record.Request) {
			batchable = false
			break
		}
	}
	if !batchable {
		for idx, record := range records {
			response := journal.ExecuteCommand(trie, record.Request)
			if !response.OK {
				return idx, response
			}
		}
		return len(records), CacheCommandResponse{OK: true}
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	batchState, err := journal.currentAppendStateLocked()
	if err != nil {
		return 0, commandError(err.Error())
	}
	states := make([]commandJournalAppendState, len(records))
	offset := batchState.offset
	now := trie.currentTime()
	for idx, record := range records {
		states[idx] = commandJournalAppendState{
			offset:            offset,
			nextSequence:      journal.nextSequence,
			sequenceExhausted: journal.sequenceExhausted,
		}
		sequence, err := journal.nextAppendSequenceLocked()
		if err != nil {
			return 0, commandError(journal.rollbackPreparedBatchLocked(batchState, err).Error())
		}
		entry := commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: sequence,
			Request:  normalizeJournalRequest(record.Request, now),
		}
		data, err := marshalCommandJournalEntry(entry, journal.format)
		if err != nil {
			return 0, commandError(journal.rollbackPreparedBatchLocked(batchState, err).Error())
		}
		n, err := journal.file.Write(data)
		if err != nil {
			return 0, commandError(journal.rollbackPreparedBatchLocked(batchState, err).Error())
		}
		if n != len(data) {
			return 0, commandError(journal.rollbackPreparedBatchLocked(batchState, io.ErrShortWrite).Error())
		}
		offset += int64(n)
		journal.markAppendedLocked(sequence)
	}
	if err := journal.syncLocked(); err != nil {
		return 0, commandError(journal.rollbackPreparedBatchLocked(batchState, err).Error())
	}
	for idx, record := range records {
		response := trie.ExecuteCommand(record.Request)
		if response.OK {
			continue
		}
		if rollbackErr := journal.rollbackAppendLocked(states[idx]); rollbackErr != nil {
			response.Message += "; failed to remove rejected journal entries: " + rollbackErr.Error()
		}
		return idx, response
	}
	return len(records), CacheCommandResponse{OK: true}
}

func (journal *CommandJournal) rollbackPreparedBatchLocked(state commandJournalAppendState, cause error) error {
	if err := journal.rollbackAppendLocked(state); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (journal *CommandJournal) executePreparedInternalReplicationCommand(trie *HatTrie, request CacheCommandRequest, operation *snapshotOperation) CacheCommandResponse {
	if journal == nil {
		return commandError(ErrNilCommandJournal.Error())
	}
	if trie == nil {
		return commandError(ErrNilHatTrie.Error())
	}
	command := normalizedCommand(request.Command)
	if command != "INTERNALSET" && command != "INTERNALDEL" {
		return commandError("prepared internal replication command must be INTERNALSET or INTERNALDEL")
	}
	if command == "INTERNALSET" && operation == nil {
		return commandError("prepared internal set operation is required")
	}
	journalRequest := normalizeJournalRequest(request, trie.currentTime())
	if journal.groupCommitEnabled() {
		return journal.submitGroupCommit(&commandJournalJob{
			trie:           trie,
			request:        request,
			journalRequest: journalRequest,
			operation:      operation,
			prepared:       true,
			result:         make(chan CacheCommandResponse, 1),
		})
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()

	appendState, err := journal.appendLocked(journalRequest)
	if err != nil {
		return commandError(err.Error())
	}
	response := executePreparedInternalReplicationCommand(trie, request, operation)
	if !response.OK {
		if err := journal.rollbackAppendLocked(appendState); err != nil {
			return commandError(response.Message + "; failed to remove rejected journal entry: " + err.Error())
		}
	}
	return response
}

func (journal *CommandJournal) Replay(trie *HatTrie, afterSequence uint64) (uint64, error) {
	if journal == nil {
		return 0, ErrNilCommandJournal
	}
	if trie == nil {
		return 0, ErrNilHatTrie
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()

	if journal.closed {
		return 0, ErrCommandJournalClosed
	}
	var maxSequence uint64
	var compactedThrough uint64
	if _, err := scanCommandJournalSet(journal.path, journal.segmented(), func(entry commandJournalEntry) error {
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
	if _, err := scanCommandJournalSet(journal.path, journal.segmented(), func(entry commandJournalEntry) error {
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
	if journal == nil {
		return CommandJournalTail{}, ErrNilCommandJournal
	}
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
	tail, err := readCommandJournalTailSet(journal.path, journal.segmented(), afterSequence, limit)
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
	if journal == nil {
		return ErrNilCommandJournal
	}
	if trie == nil {
		return ErrNilHatTrie
	}
	format, err := ParseSnapshotFormat(string(format))
	if err != nil {
		return err
	}
	journal.snapshotMu.Lock()
	defer journal.snapshotMu.Unlock()
	journal.mu.Lock()
	if journal.closed {
		journal.mu.Unlock()
		return ErrCommandJournalClosed
	}
	journal.mu.Unlock()

	capture, sequence, err := trie.captureSnapshotStreamForStoreAtBarrier(nil, nil, journal.snapshotCaptureBarrier())
	if err != nil {
		return err
	}
	if err := writeFileAtomicStream(path, func(writer io.Writer) error {
		return writeStreamSnapshot(writer, sequence, format, capture)
	}); err != nil {
		return err
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return ErrCommandJournalClosed
	}
	return journal.compactLocked(sequence)
}

// WriteSnapshotWithFormat writes a point-in-time snapshot without compacting the journal.
func (journal *CommandJournal) WriteSnapshotWithFormat(trie *HatTrie, writer io.Writer, format SnapshotFormat) (SnapshotMetadata, error) {
	if journal == nil {
		return SnapshotMetadata{}, ErrNilCommandJournal
	}
	if trie == nil {
		return SnapshotMetadata{}, ErrNilHatTrie
	}
	if writer == nil {
		return SnapshotMetadata{}, errors.New("hatriecache: snapshot writer is nil")
	}
	format, err := ParseSnapshotFormat(string(format))
	if err != nil {
		return SnapshotMetadata{}, err
	}
	journal.snapshotMu.Lock()
	defer journal.snapshotMu.Unlock()
	journal.mu.Lock()
	if journal.closed {
		journal.mu.Unlock()
		return SnapshotMetadata{}, ErrCommandJournalClosed
	}
	journal.mu.Unlock()

	capture, sequence, err := trie.captureSnapshotStreamForStoreAtBarrier(nil, nil, journal.snapshotCaptureBarrier())
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if err := writeStreamSnapshot(writer, sequence, format, capture); err != nil {
		return SnapshotMetadata{}, err
	}
	return SnapshotMetadata{JournalSequence: sequence}, nil
}

func (journal *CommandJournal) snapshotCaptureBarrier() snapshotCaptureBarrier {
	return func() (uint64, func(), error) {
		journal.mu.Lock()
		if journal.closed {
			journal.mu.Unlock()
			return 0, nil, ErrCommandJournalClosed
		}
		return journal.lastSequenceLocked(), journal.mu.Unlock, nil
	}
}

// ReplaceWithSnapshot replaces in-memory data, then resets the local journal
// sequence to the snapshot's source checkpoint.
func (journal *CommandJournal) ReplaceWithSnapshot(trie *HatTrie, path string) (SnapshotMetadata, error) {
	if journal == nil {
		return SnapshotMetadata{}, ErrNilCommandJournal
	}
	if trie == nil {
		return SnapshotMetadata{}, ErrNilHatTrie
	}
	journal.snapshotMu.Lock()
	defer journal.snapshotMu.Unlock()

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return SnapshotMetadata{}, ErrCommandJournalClosed
	}
	metadata, err := trie.LoadSnapshotWithMetadata(path)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if err := journal.resetToCheckpointLocked(metadata.JournalSequence); err != nil {
		return SnapshotMetadata{}, err
	}
	return metadata, nil
}

func (journal *CommandJournal) Sequence() uint64 {
	if journal == nil {
		return 0
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()

	return journal.lastSequenceLocked()
}

func (journal *CommandJournal) appendLocked(request CacheCommandRequest) (commandJournalAppendState, error) {
	appendState, err := journal.appendWithoutSyncLocked(request)
	if err != nil {
		return commandJournalAppendState{}, journal.rollbackFailedAppendLocked(appendState, err)
	}
	if err := journal.syncLocked(); err != nil {
		return commandJournalAppendState{}, journal.rollbackFailedAppendLocked(appendState, err)
	}
	return appendState, nil
}

func (journal *CommandJournal) currentAppendStateLocked() (commandJournalAppendState, error) {
	if err := journal.ensureAppendFileLocked(); err != nil {
		return commandJournalAppendState{}, err
	}
	if err := journal.rotateSegmentIfFullLocked(); err != nil {
		return commandJournalAppendState{}, err
	}
	info, err := journal.file.Stat()
	if err != nil {
		return commandJournalAppendState{}, err
	}
	return commandJournalAppendState{
		offset:            info.Size(),
		nextSequence:      journal.nextSequence,
		sequenceExhausted: journal.sequenceExhausted,
	}, nil
}

func (journal *CommandJournal) appendWithoutSyncLocked(request CacheCommandRequest) (commandJournalAppendState, error) {
	appendState, err := journal.currentAppendStateLocked()
	if err != nil {
		return commandJournalAppendState{}, err
	}
	_, err = journal.writeWithoutSyncLocked(request)
	return appendState, err
}

func (journal *CommandJournal) writeWithoutSyncLocked(request CacheCommandRequest) (int, error) {
	sequence, err := journal.nextAppendSequenceLocked()
	if err != nil {
		return 0, err
	}
	entry := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: sequence,
		Request:  request,
	}
	data, err := marshalCommandJournalEntry(entry, journal.format)
	if err != nil {
		return 0, err
	}

	n, err := journal.file.Write(data)
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	journal.markAppendedLocked(sequence)
	return n, nil
}

func (journal *CommandJournal) syncLocked() error {
	if journal.syncHook != nil {
		return journal.syncHook()
	}
	if journal.file == nil {
		return ErrCommandJournalClosed
	}
	return journal.file.Sync()
}

func (journal *CommandJournal) rollbackFailedAppendLocked(state commandJournalAppendState, cause error) error {
	if err := journal.rollbackAppendLocked(state); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (journal *CommandJournal) rollbackAppendLocked(state commandJournalAppendState) error {
	if err := journal.rollbackAppendWithoutSyncLocked(state); err != nil {
		return err
	}
	return journal.syncLocked()
}

func (journal *CommandJournal) rollbackAppendWithoutSyncLocked(state commandJournalAppendState) error {
	if journal.file == nil {
		return ErrCommandJournalClosed
	}
	if err := journal.file.Truncate(state.offset); err != nil {
		return err
	}
	if _, err := journal.file.Seek(state.offset, io.SeekStart); err != nil {
		return err
	}
	journal.nextSequence = state.nextSequence
	journal.sequenceExhausted = state.sequenceExhausted
	return nil
}

func (journal *CommandJournal) compactLocked(throughSequence uint64) error {
	if journal.segmented() {
		return journal.rotateSegmentLocked(true)
	}
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

func (journal *CommandJournal) resetToCheckpointLocked(sequence uint64) error {
	if err := journal.closeAppendFileLocked(); err != nil {
		return err
	}
	if journal.segmented() {
		if err := os.RemoveAll(commandJournalSegmentDir(journal.path)); err != nil {
			return err
		}
	}
	if err := writeCommandJournalCheckpointWithFormat(journal.path, sequence, journal.format); err != nil {
		if reopenErr := journal.ensureAppendFileLocked(); reopenErr != nil {
			return errors.Join(err, reopenErr)
		}
		return err
	}
	if sequence == ^uint64(0) {
		journal.nextSequence = sequence
		journal.sequenceExhausted = true
	} else {
		journal.nextSequence = sequence + 1
		journal.sequenceExhausted = false
	}
	journal.activeSegmentStart = journal.nextSequence
	return journal.ensureAppendFileLocked()
}

func writeCommandJournalCheckpointWithFormat(path string, sequence uint64, format CommandJournalFormat) error {
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		if sequence == 0 {
			return nil
		}
		return writeCommandJournalEntry(writer, commandJournalEntry{
			Version:    commandJournalVersion,
			Sequence:   sequence,
			Checkpoint: true,
		}, format)
	})
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
	if journal.segmented() {
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			journal.file = nil
			return err
		}
		if info.Size() == 0 {
			sequence := journal.lastSequenceLocked()
			if sequence > 0 {
				if err := journal.writeCheckpointWithoutSyncLocked(sequence); err != nil {
					_ = file.Close()
					journal.file = nil
					return err
				}
			}
			journal.activeSegmentStart = journal.nextSequence
		}
	}
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
	var readBuffer commandJournalReadBuffer
	reader := bufio.NewReader(file)
	for {
		entry, bytesRead, complete, err := readCommandJournalEntryBuffered(reader, &readBuffer)
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

type commandJournalReadBuffer struct {
	payload []byte
}

func readCommandJournalEntry(reader *bufio.Reader) (commandJournalEntry, int, bool, error) {
	return readCommandJournalEntryBuffered(reader, nil)
}

func readCommandJournalEntryBuffered(reader *bufio.Reader, buffer *commandJournalReadBuffer) (commandJournalEntry, int, bool, error) {
	header, err := reader.Peek(len(commandJournalBinaryMagic))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return readCommandJournalJSONEntry(reader)
		}
		return commandJournalEntry{}, 0, false, err
	}
	if bytes.Equal(header, commandJournalBinaryMagic) {
		return readCommandJournalBinaryEntry(reader, buffer)
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
	return readCommandJournalJSONRecordLimited(reader, maxCommandJournalJSONRecordBytes)
}

func readCommandJournalJSONRecordLimited(reader *bufio.Reader, limit int) ([]byte, int, bool, error) {
	if limit <= 0 {
		return nil, 0, false, errCommandJournalJSONRecordTooLarge
	}
	var record []byte
	bytesRead := 0
	for {
		fragment, err := reader.ReadSlice('\n')
		bytesRead += len(fragment)
		if bytesRead > limit {
			return nil, bytesRead, false, errCommandJournalJSONRecordTooLarge
		}
		if err == nil {
			if record == nil {
				return fragment, bytesRead, true, nil
			}
			record = append(record, fragment...)
			return record, bytesRead, true, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			record = append(record, fragment...)
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
}

func readCommandJournalBinaryEntry(reader *bufio.Reader, buffer *commandJournalReadBuffer) (commandJournalEntry, int, bool, error) {
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
	payload := commandJournalPayloadBuffer(buffer, int(size))
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

func commandJournalPayloadBuffer(buffer *commandJournalReadBuffer, size int) []byte {
	if buffer == nil || size > maxCommandJournalReusablePayloadBufferBytes {
		return make([]byte, size)
	}
	if size > cap(buffer.payload) || cap(buffer.payload) > maxCommandJournalReusablePayloadBufferBytes {
		buffer.payload = make([]byte, size)
	} else {
		buffer.payload = buffer.payload[:size]
	}
	return buffer.payload
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
	if command == "" {
		return false
	}
	if command == "BATCH" {
		return commandBatchShouldJournal(request)
	}
	key := strings.TrimSpace(request.Key)
	if key == "" {
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

func commandBatchShouldJournal(request CacheCommandRequest) bool {
	for _, payload := range request.Batch {
		if commandShouldJournal(payload) {
			return true
		}
	}
	return false
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
