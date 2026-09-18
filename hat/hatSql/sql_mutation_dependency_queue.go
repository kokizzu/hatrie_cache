package hatSql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	sQLMutationDependencyQueueMagic          = "HMQ1"
	sQLMutationDependencyQueueVersion        = 1
	sQLMutationDependencyQueueHeaderBytes    = 4 + 1 + 1 + 4 + 8
	sQLMutationDependencyQueueChecksumBytes  = 4
	maxSQLMutationDependencyQueueRecordBytes = 16 << 20
)

const (
	sQLMutationDependencyQueueOpAdd byte = iota + 1
	sQLMutationDependencyQueueOpClaim
	sQLMutationDependencyQueueOpComplete
	sQLMutationDependencyQueueOpFail
	sQLMutationDependencyQueueOpRetry
	sQLMutationDependencyQueueOpRequeue
	sQLMutationDependencyQueueOpSnapshot
)

var (
	// ErrSQLMutationDependencyQueueNil reports a method call on a nil queue.
	ErrSQLMutationDependencyQueueNil = errors.New("SQL mutation dependency queue is nil")
	// ErrSQLMutationDependencyQueueClosed reports an operation after Close.
	ErrSQLMutationDependencyQueueClosed = errors.New("SQL mutation dependency queue is closed")
	// ErrSQLMutationDependencyQueuePath reports a missing queue path.
	ErrSQLMutationDependencyQueuePath = errors.New("SQL mutation dependency queue path is invalid")
	// ErrSQLMutationDependencyQueueCorrupt reports invalid durable bytes.
	ErrSQLMutationDependencyQueueCorrupt = errors.New("SQL mutation dependency queue log is corrupt")
)

// SQLMutationDependencyQueue is a durable, dependency-aware mutation queue.
// Every successful transition is written to a CRC-protected binary log and
// synced before the method returns. Open replays the log into an in-memory
// SQLMutationDependencyGraph. An incomplete final record is treated as a
// crash tail and truncated; a complete record with a bad checksum is rejected.
// The queue serializes its own callers and does not change the standalone
// graph's caller-managed durability behavior.
type SQLMutationDependencyQueue struct {
	mu           sync.Mutex
	path         string
	file         *os.File
	graph        *SQLMutationDependencyGraph
	nextSequence uint64
	closed       bool
}

// OpenSQLMutationDependencyQueue opens or creates a queue log with mode 0600.
// A zero maxTasks uses DefaultSQLMutationDependencyGraphMaxTasks.
func OpenSQLMutationDependencyQueue(path string, maxTasks int) (*SQLMutationDependencyQueue, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrSQLMutationDependencyQueuePath
	}
	graph, err := NewSQLMutationDependencyGraph(maxTasks)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open mutation dependency queue: %w", err)
	}
	queue := &SQLMutationDependencyQueue{path: path, file: file, graph: graph}
	sequence, replayErr := replaySQLMutationDependencyQueue(file, graph)
	if replayErr != nil {
		_ = file.Close()
		return nil, replayErr
	}
	queue.nextSequence = sequence
	return queue, nil
}

// Add registers a task and durably records it before returning.
func (queue *SQLMutationDependencyQueue) Add(task SQLMutationTask) error {
	if queue == nil {
		return ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err := queue.ensureOpenLocked(); err != nil {
		return err
	}
	id, dependencies, err := normalizeSQLMutationTask(task)
	if err != nil {
		return err
	}
	normalized := SQLMutationTask{ID: id, DependsOn: dependencies}
	if err := validateSQLMutationDependencyGraphAdd(queue.graph, normalized); err != nil {
		return err
	}
	payload, err := encodeSQLMutationDependencyQueueAdd(normalized)
	if err != nil {
		return err
	}
	return queue.appendAndApplyLocked(sQLMutationDependencyQueueOpAdd, payload, func() error {
		return queue.graph.Add(normalized)
	})
}

// ClaimReady durably claims up to limit ready tasks in deterministic ID order.
// A non-positive limit claims every currently ready task.
func (queue *SQLMutationDependencyQueue) ClaimReady(limit int) ([]SQLMutationTaskRecord, error) {
	if queue == nil {
		return nil, ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err := queue.ensureOpenLocked(); err != nil {
		return nil, err
	}
	claimed := queue.graph.ClaimReady(limit)
	if len(claimed) == 0 {
		return nil, nil
	}
	payload, err := encodeSQLMutationDependencyQueueClaims(claimed)
	if err != nil {
		return nil, rollbackSQLMutationDependencyGraphClaims(queue.graph, claimed, err)
	}
	if err := queue.appendAndApplyLocked(sQLMutationDependencyQueueOpClaim, payload, func() error {
		return nil
	}); err != nil {
		return nil, rollbackSQLMutationDependencyGraphClaims(queue.graph, claimed, err)
	}
	return claimed, nil
}

// Complete durably marks an owned task complete.
func (queue *SQLMutationDependencyQueue) Complete(id string, attempt uint64) error {
	return queue.finish(id, attempt, sQLMutationDependencyQueueOpComplete, "")
}

// Fail durably records a bounded failure for an owned task.
func (queue *SQLMutationDependencyQueue) Fail(id string, attempt uint64, reason string) error {
	reason = strings.TrimSpace(reason)
	return queue.finish(id, attempt, sQLMutationDependencyQueueOpFail, reason)
}

func (queue *SQLMutationDependencyQueue) finish(id string, attempt uint64, operation byte, reason string) error {
	if queue == nil {
		return ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err := queue.ensureOpenLocked(); err != nil {
		return err
	}
	if err := validateSQLMutationDependencyGraphFinish(queue.graph, id, attempt, reason, operation == sQLMutationDependencyQueueOpFail); err != nil {
		return err
	}
	payload, err := encodeSQLMutationDependencyQueueFinish(id, attempt, reason, operation == sQLMutationDependencyQueueOpFail)
	if err != nil {
		return err
	}
	return queue.appendAndApplyLocked(operation, payload, func() error {
		var err error
		switch operation {
		case sQLMutationDependencyQueueOpComplete:
			err = queue.graph.Complete(id, attempt)
		case sQLMutationDependencyQueueOpFail:
			err = queue.graph.Fail(id, attempt, reason)
		default:
			err = fmt.Errorf("unsupported mutation queue finish operation %d", operation)
		}
		return err
	})
}

// Retry durably moves a failed task back to pending.
func (queue *SQLMutationDependencyQueue) Retry(id string) error {
	if queue == nil {
		return ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err := queue.ensureOpenLocked(); err != nil {
		return err
	}
	if err := validateSQLMutationDependencyGraphRetry(queue.graph, id); err != nil {
		return err
	}
	payload, err := encodeSQLMutationDependencyQueueID(id)
	if err != nil {
		return err
	}
	return queue.appendAndApplyLocked(sQLMutationDependencyQueueOpRetry, payload, func() error {
		return queue.graph.Retry(id)
	})
}

// RequeueRunning durably returns all running tasks to pending for crash
// recovery. It returns the number of changed tasks.
func (queue *SQLMutationDependencyQueue) RequeueRunning() (int, error) {
	if queue == nil {
		return 0, ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err := queue.ensureOpenLocked(); err != nil {
		return 0, err
	}
	count := countSQLMutationDependencyGraphRunning(queue.graph)
	if count == 0 {
		return 0, nil
	}
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, uint32(count))
	if err := queue.appendAndApplyLocked(sQLMutationDependencyQueueOpRequeue, payload, func() error {
		if got := queue.graph.RequeueRunning(); got != count {
			return fmt.Errorf("requeue count %d, want %d", got, count)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

// Task returns a detached task record. A closed queue returns no task.
func (queue *SQLMutationDependencyQueue) Task(id string) (SQLMutationTaskRecord, bool) {
	if queue == nil {
		return SQLMutationTaskRecord{}, false
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return SQLMutationTaskRecord{}, false
	}
	return queue.graph.Task(id)
}

// Snapshot returns all task records sorted by ID. A closed queue returns nil.
func (queue *SQLMutationDependencyQueue) Snapshot() []SQLMutationTaskRecord {
	if queue == nil {
		return nil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return nil
	}
	return queue.graph.Snapshot()
}

// Sync flushes the durable queue log without changing graph state.
func (queue *SQLMutationDependencyQueue) Sync() error {
	if queue == nil {
		return ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err := queue.ensureOpenLocked(); err != nil {
		return err
	}
	return queue.file.Sync()
}

// Compact rewrites the log to one validated snapshot record. It preserves
// task state and resets the journal sequence, keeping restart cost bounded.
func (queue *SQLMutationDependencyQueue) Compact() error {
	if queue == nil {
		return ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if err := queue.ensureOpenLocked(); err != nil {
		return err
	}
	payload, err := encodeSQLMutationDependencyQueueSnapshot(queue.graph.Snapshot())
	if err != nil {
		return err
	}
	if len(payload) > maxSQLMutationDependencyQueueRecordBytes {
		return fmt.Errorf("%w: snapshot payload exceeds %d bytes", ErrSQLMutationDependencyQueueCorrupt, maxSQLMutationDependencyQueueRecordBytes)
	}
	temporary, err := os.CreateTemp(filepath.Dir(queue.path), "."+filepath.Base(queue.path)+".compact-*")
	if err != nil {
		return fmt.Errorf("create mutation queue compaction file: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	frame := encodeSQLMutationDependencyQueueFrame(1, sQLMutationDependencyQueueOpSnapshot, payload)
	if err := writeSQLMutationDependencyQueueFrame(temporary, frame); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync mutation queue compaction file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("close mutation queue compaction file: %w", err)
	}
	if err := os.Rename(temporaryPath, queue.path); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("replace mutation queue log: %w", err)
	}
	newFile, err := os.OpenFile(queue.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		queue.closed = true
		_ = queue.file.Close()
		return fmt.Errorf("reopen compacted mutation queue log: %w", err)
	}
	oldFile := queue.file
	queue.file = newFile
	queue.nextSequence = 1
	if err := oldFile.Close(); err != nil {
		return fmt.Errorf("close previous mutation queue log: %w", err)
	}
	return nil
}

// Close syncs and closes the queue. It is idempotent.
func (queue *SQLMutationDependencyQueue) Close() error {
	if queue == nil {
		return ErrSQLMutationDependencyQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return nil
	}
	queue.closed = true
	syncErr := queue.file.Sync()
	closeErr := queue.file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (queue *SQLMutationDependencyQueue) ensureOpenLocked() error {
	if queue.file == nil || queue.graph == nil {
		return ErrSQLMutationDependencyQueueClosed
	}
	if queue.closed {
		return ErrSQLMutationDependencyQueueClosed
	}
	return nil
}

func (queue *SQLMutationDependencyQueue) appendAndApplyLocked(operation byte, payload []byte, apply func() error) error {
	if len(payload) > maxSQLMutationDependencyQueueRecordBytes {
		return fmt.Errorf("%w: record payload exceeds %d bytes", ErrSQLMutationDependencyQueueCorrupt, maxSQLMutationDependencyQueueRecordBytes)
	}
	start, err := queue.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("locate mutation queue log end: %w", err)
	}
	sequence := queue.nextSequence + 1
	if sequence == 0 {
		return fmt.Errorf("%w: sequence exhausted", ErrSQLMutationDependencyQueueCorrupt)
	}
	frame := encodeSQLMutationDependencyQueueFrame(sequence, operation, payload)
	if err := writeSQLMutationDependencyQueueFrame(queue.file, frame); err != nil {
		return queue.truncateFailedAppendLocked(start, err)
	}
	if err := queue.file.Sync(); err != nil {
		return queue.truncateFailedAppendLocked(start, fmt.Errorf("sync mutation queue log: %w", err))
	}
	if err := apply(); err != nil {
		return queue.truncateFailedAppendLocked(start, err)
	}
	queue.nextSequence = sequence
	return nil
}

func (queue *SQLMutationDependencyQueue) truncateFailedAppendLocked(start int64, cause error) error {
	if err := queue.file.Truncate(start); err != nil {
		return fmt.Errorf("%w; truncate failed append: %v", cause, err)
	}
	if _, err := queue.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("%w; seek after truncation: %v", cause, err)
	}
	return cause
}

func validateSQLMutationDependencyGraphAdd(graph *SQLMutationDependencyGraph, task SQLMutationTask) error {
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	if _, exists := graph.tasks[task.ID]; exists {
		return fmt.Errorf("%w: %q", ErrSQLMutationDependencyGraphDuplicate, task.ID)
	}
	if len(graph.tasks) >= graph.maxTasks {
		return fmt.Errorf("%w: maximum is %d", ErrSQLMutationDependencyGraphCapacity, graph.maxTasks)
	}
	for _, dependency := range task.DependsOn {
		if _, exists := graph.tasks[dependency]; !exists {
			return fmt.Errorf("%w: task %q depends on %q", ErrSQLMutationDependencyGraphMissingDependency, task.ID, dependency)
		}
	}
	return nil
}

func rollbackSQLMutationDependencyGraphClaims(graph *SQLMutationDependencyGraph, claimed []SQLMutationTaskRecord, cause error) error {
	graph.mu.Lock()
	defer graph.mu.Unlock()
	for _, record := range claimed {
		task, exists := graph.tasks[record.ID]
		if !exists || task.State != SQLMutationTaskRunning || task.Attempt != record.Attempt {
			return fmt.Errorf("%w; claim rollback failed for %q", cause, record.ID)
		}
	}
	for _, record := range claimed {
		task := graph.tasks[record.ID]
		task.State = SQLMutationTaskPending
		task.Attempt = record.Attempt - 1
	}
	return cause
}

func validateSQLMutationDependencyGraphFinish(graph *SQLMutationDependencyGraph, id string, attempt uint64, reason string, includeReason bool) error {
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	task, exists := graph.tasks[id]
	if !exists {
		return fmt.Errorf("%w: %q", ErrSQLMutationDependencyGraphTaskNotFound, id)
	}
	if task.State != SQLMutationTaskRunning {
		return fmt.Errorf("%w: task %q is in state %q", ErrSQLMutationDependencyGraphState, id, task.State)
	}
	if attempt == 0 || task.Attempt != attempt {
		return fmt.Errorf("%w: task %q attempt %d", ErrSQLMutationDependencyGraphStaleAttempt, id, attempt)
	}
	if includeReason && len(reason) > maxSQLMutationTaskErrorBytes {
		return fmt.Errorf("%w: failure reason exceeds %d bytes", ErrSQLMutationDependencyGraphInvalid, maxSQLMutationTaskErrorBytes)
	}
	return nil
}

func validateSQLMutationDependencyGraphRetry(graph *SQLMutationDependencyGraph, id string) error {
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	task, exists := graph.tasks[id]
	if !exists {
		return fmt.Errorf("%w: %q", ErrSQLMutationDependencyGraphTaskNotFound, id)
	}
	if task.State != SQLMutationTaskFailed {
		return fmt.Errorf("%w: cannot retry task %q in state %q", ErrSQLMutationDependencyGraphState, id, task.State)
	}
	return nil
}

func countSQLMutationDependencyGraphRunning(graph *SQLMutationDependencyGraph) int {
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	count := 0
	for _, task := range graph.tasks {
		if task.State == SQLMutationTaskRunning {
			count++
		}
	}
	return count
}

func replaySQLMutationDependencyQueue(file *os.File, graph *SQLMutationDependencyGraph) (uint64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("%w: seek log: %v", ErrSQLMutationDependencyQueueCorrupt, err)
	}
	var sequence uint64
	for {
		start, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("%w: locate record: %v", ErrSQLMutationDependencyQueueCorrupt, err)
		}
		header := make([]byte, sQLMutationDependencyQueueHeaderBytes)
		read, readErr := io.ReadFull(file, header)
		if readErr == io.EOF && read == 0 {
			break
		}
		if readErr == io.ErrUnexpectedEOF {
			if err := truncateQueueTail(file, start); err != nil {
				return 0, err
			}
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("%w: read header: %v", ErrSQLMutationDependencyQueueCorrupt, readErr)
		}
		if string(header[:4]) != sQLMutationDependencyQueueMagic || header[4] != sQLMutationDependencyQueueVersion {
			return 0, fmt.Errorf("%w: invalid magic or version", ErrSQLMutationDependencyQueueCorrupt)
		}
		payloadLength := binary.LittleEndian.Uint32(header[6:10])
		if payloadLength > maxSQLMutationDependencyQueueRecordBytes {
			return 0, fmt.Errorf("%w: payload length %d exceeds %d", ErrSQLMutationDependencyQueueCorrupt, payloadLength, maxSQLMutationDependencyQueueRecordBytes)
		}
		recordSequence := binary.LittleEndian.Uint64(header[10:18])
		if recordSequence == 0 || recordSequence != sequence+1 {
			return 0, fmt.Errorf("%w: sequence %d after %d", ErrSQLMutationDependencyQueueCorrupt, recordSequence, sequence)
		}
		payload := make([]byte, int(payloadLength))
		if _, readErr := io.ReadFull(file, payload); readErr != nil {
			if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
				if err := truncateQueueTail(file, start); err != nil {
					return 0, err
				}
				break
			}
			return 0, fmt.Errorf("%w: read payload: %v", ErrSQLMutationDependencyQueueCorrupt, readErr)
		}
		checksum := make([]byte, sQLMutationDependencyQueueChecksumBytes)
		if _, readErr := io.ReadFull(file, checksum); readErr != nil {
			if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
				if err := truncateQueueTail(file, start); err != nil {
					return 0, err
				}
				break
			}
			return 0, fmt.Errorf("%w: read checksum: %v", ErrSQLMutationDependencyQueueCorrupt, readErr)
		}
		hash := crc32.NewIEEE()
		_, _ = hash.Write(header)
		_, _ = hash.Write(payload)
		if binary.LittleEndian.Uint32(checksum) != hash.Sum32() {
			return 0, fmt.Errorf("%w: checksum at offset %d", ErrSQLMutationDependencyQueueCorrupt, start)
		}
		if err := applySQLMutationDependencyQueueRecord(graph, header[5], payload); err != nil {
			return 0, fmt.Errorf("%w: sequence %d: %v", ErrSQLMutationDependencyQueueCorrupt, recordSequence, err)
		}
		sequence = recordSequence
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("%w: seek append position: %v", ErrSQLMutationDependencyQueueCorrupt, err)
	}
	return sequence, nil
}

func truncateQueueTail(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("%w: truncate incomplete tail: %v", ErrSQLMutationDependencyQueueCorrupt, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("%w: sync truncated tail: %v", ErrSQLMutationDependencyQueueCorrupt, err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("%w: seek after tail truncation: %v", ErrSQLMutationDependencyQueueCorrupt, err)
	}
	return nil
}

func applySQLMutationDependencyQueueRecord(graph *SQLMutationDependencyGraph, operation byte, payload []byte) error {
	switch operation {
	case sQLMutationDependencyQueueOpAdd:
		task, err := decodeSQLMutationDependencyQueueAdd(payload)
		if err != nil {
			return err
		}
		return graph.Add(task)
	case sQLMutationDependencyQueueOpClaim:
		expected, err := decodeSQLMutationDependencyQueueClaims(payload)
		if err != nil {
			return err
		}
		claimed := graph.ClaimReady(len(expected))
		if len(claimed) != len(expected) {
			return fmt.Errorf("claim count %d, want %d", len(claimed), len(expected))
		}
		for index := range expected {
			if claimed[index].ID != expected[index].ID || claimed[index].Attempt != expected[index].Attempt {
				return fmt.Errorf("claim %d = %#v, want %#v", index, claimed[index], expected[index])
			}
		}
		return nil
	case sQLMutationDependencyQueueOpComplete, sQLMutationDependencyQueueOpFail:
		id, attempt, reason, err := decodeSQLMutationDependencyQueueFinish(payload, operation == sQLMutationDependencyQueueOpFail)
		if err != nil {
			return err
		}
		if operation == sQLMutationDependencyQueueOpComplete {
			return graph.Complete(id, attempt)
		}
		return graph.Fail(id, attempt, reason)
	case sQLMutationDependencyQueueOpRetry:
		id, err := decodeSQLMutationDependencyQueueID(payload)
		if err != nil {
			return err
		}
		return graph.Retry(id)
	case sQLMutationDependencyQueueOpRequeue:
		count, err := decodeSQLMutationDependencyQueueCount(payload)
		if err != nil {
			return err
		}
		if got := graph.RequeueRunning(); got != count {
			return fmt.Errorf("requeue count %d, want %d", got, count)
		}
		return nil
	case sQLMutationDependencyQueueOpSnapshot:
		tasks, err := decodeSQLMutationDependencyQueueSnapshot(payload)
		if err != nil {
			return err
		}
		return replaceSQLMutationDependencyGraphSnapshot(graph, tasks)
	default:
		return fmt.Errorf("unknown operation %d", operation)
	}
}

func encodeSQLMutationDependencyQueueFrame(sequence uint64, operation byte, payload []byte) []byte {
	frame := make([]byte, sQLMutationDependencyQueueHeaderBytes+len(payload)+sQLMutationDependencyQueueChecksumBytes)
	copy(frame[:4], sQLMutationDependencyQueueMagic)
	frame[4] = sQLMutationDependencyQueueVersion
	frame[5] = operation
	binary.LittleEndian.PutUint32(frame[6:10], uint32(len(payload)))
	binary.LittleEndian.PutUint64(frame[10:18], sequence)
	copy(frame[18:], payload)
	checksum := crc32.ChecksumIEEE(frame[:sQLMutationDependencyQueueHeaderBytes+len(payload)])
	binary.LittleEndian.PutUint32(frame[len(frame)-4:], checksum)
	return frame
}

func writeSQLMutationDependencyQueueFrame(writer io.Writer, frame []byte) error {
	written, err := writer.Write(frame)
	if err != nil {
		return fmt.Errorf("write mutation queue record: %w", err)
	}
	if written != len(frame) {
		return io.ErrShortWrite
	}
	return nil
}

func encodeSQLMutationDependencyQueueAdd(task SQLMutationTask) ([]byte, error) {
	payload := make([]byte, 0, len(task.ID)+8)
	var err error
	payload, err = appendSQLMutationDependencyQueueString(payload, task.ID, maxSQLMutationTaskIDBytes)
	if err != nil {
		return nil, err
	}
	payload = appendSQLMutationDependencyQueueUint32(payload, uint32(len(task.DependsOn)))
	for _, dependency := range task.DependsOn {
		payload, err = appendSQLMutationDependencyQueueString(payload, dependency, maxSQLMutationTaskIDBytes)
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func encodeSQLMutationDependencyQueueClaims(claimed []SQLMutationTaskRecord) ([]byte, error) {
	payload := appendSQLMutationDependencyQueueUint32(nil, uint32(len(claimed)))
	var err error
	for _, task := range claimed {
		payload, err = appendSQLMutationDependencyQueueString(payload, task.ID, maxSQLMutationTaskIDBytes)
		if err != nil {
			return nil, err
		}
		payload = appendSQLMutationDependencyQueueUint64(payload, task.Attempt)
	}
	return payload, nil
}

func encodeSQLMutationDependencyQueueFinish(id string, attempt uint64, reason string, includeReason bool) ([]byte, error) {
	payload, err := appendSQLMutationDependencyQueueString(nil, id, maxSQLMutationTaskIDBytes)
	if err != nil {
		return nil, err
	}
	payload = appendSQLMutationDependencyQueueUint64(payload, attempt)
	if includeReason {
		payload, err = appendSQLMutationDependencyQueueString(payload, reason, maxSQLMutationTaskErrorBytes)
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func encodeSQLMutationDependencyQueueID(id string) ([]byte, error) {
	return appendSQLMutationDependencyQueueString(nil, id, maxSQLMutationTaskIDBytes)
}

func encodeSQLMutationDependencyQueueSnapshot(tasks []SQLMutationTaskRecord) ([]byte, error) {
	payload := appendSQLMutationDependencyQueueUint32(nil, uint32(len(tasks)))
	for _, task := range tasks {
		var err error
		payload, err = appendSQLMutationDependencyQueueString(payload, task.ID, maxSQLMutationTaskIDBytes)
		if err != nil {
			return nil, err
		}
		payload = appendSQLMutationDependencyQueueUint32(payload, uint32(len(task.DependsOn)))
		for _, dependency := range task.DependsOn {
			payload, err = appendSQLMutationDependencyQueueString(payload, dependency, maxSQLMutationTaskIDBytes)
			if err != nil {
				return nil, err
			}
		}
		state, err := encodeSQLMutationDependencyQueueState(task.State)
		if err != nil {
			return nil, err
		}
		payload = append(payload, state)
		payload = appendSQLMutationDependencyQueueUint64(payload, task.Attempt)
		payload, err = appendSQLMutationDependencyQueueString(payload, task.LastError, maxSQLMutationTaskErrorBytes)
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func appendSQLMutationDependencyQueueString(dst []byte, value string, maxBytes int) ([]byte, error) {
	if len(value) > maxBytes {
		return nil, fmt.Errorf("%w: string exceeds %d bytes", ErrSQLMutationDependencyQueueCorrupt, maxBytes)
	}
	dst = appendSQLMutationDependencyQueueUint32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

func appendSQLMutationDependencyQueueUint32(dst []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendSQLMutationDependencyQueueUint64(dst []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(dst, encoded[:]...)
}

type sqlMutationDependencyQueueDecoder struct {
	data   []byte
	offset int
}

func (decoder *sqlMutationDependencyQueueDecoder) bytes(count int) ([]byte, error) {
	if count < 0 || count > len(decoder.data)-decoder.offset {
		return nil, fmt.Errorf("%w: payload ends at offset %d", ErrSQLMutationDependencyQueueCorrupt, decoder.offset)
	}
	value := decoder.data[decoder.offset : decoder.offset+count]
	decoder.offset += count
	return value, nil
}

func (decoder *sqlMutationDependencyQueueDecoder) uint32() (uint32, error) {
	value, err := decoder.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (decoder *sqlMutationDependencyQueueDecoder) uint64() (uint64, error) {
	value, err := decoder.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (decoder *sqlMutationDependencyQueueDecoder) string(maxBytes int) (string, error) {
	length, err := decoder.uint32()
	if err != nil {
		return "", err
	}
	if length > uint32(maxBytes) {
		return "", fmt.Errorf("%w: string length %d exceeds %d", ErrSQLMutationDependencyQueueCorrupt, length, maxBytes)
	}
	value, err := decoder.bytes(int(length))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (decoder *sqlMutationDependencyQueueDecoder) done() error {
	if decoder.offset != len(decoder.data) {
		return fmt.Errorf("%w: trailing payload bytes", ErrSQLMutationDependencyQueueCorrupt)
	}
	return nil
}

func decodeSQLMutationDependencyQueueAdd(payload []byte) (SQLMutationTask, error) {
	decoder := sqlMutationDependencyQueueDecoder{data: payload}
	id, err := decoder.string(maxSQLMutationTaskIDBytes)
	if err != nil {
		return SQLMutationTask{}, err
	}
	count, err := decoder.uint32()
	if err != nil || count > uint32(maxSQLMutationDependencyGraphTasks) {
		if err != nil {
			return SQLMutationTask{}, err
		}
		return SQLMutationTask{}, fmt.Errorf("%w: dependency count exceeds limit", ErrSQLMutationDependencyQueueCorrupt)
	}
	dependencies := make([]string, int(count))
	for index := range dependencies {
		dependencies[index], err = decoder.string(maxSQLMutationTaskIDBytes)
		if err != nil {
			return SQLMutationTask{}, err
		}
	}
	if err := decoder.done(); err != nil {
		return SQLMutationTask{}, err
	}
	return SQLMutationTask{ID: id, DependsOn: dependencies}, nil
}

func decodeSQLMutationDependencyQueueClaims(payload []byte) ([]SQLMutationTaskRecord, error) {
	decoder := sqlMutationDependencyQueueDecoder{data: payload}
	count, err := decoder.uint32()
	if err != nil || count > uint32(maxSQLMutationDependencyGraphTasks) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: claim count exceeds limit", ErrSQLMutationDependencyQueueCorrupt)
	}
	claimed := make([]SQLMutationTaskRecord, int(count))
	for index := range claimed {
		claimed[index].ID, err = decoder.string(maxSQLMutationTaskIDBytes)
		if err != nil {
			return nil, err
		}
		claimed[index].Attempt, err = decoder.uint64()
		if err != nil {
			return nil, err
		}
		if claimed[index].Attempt == 0 {
			return nil, fmt.Errorf("%w: zero claim attempt", ErrSQLMutationDependencyQueueCorrupt)
		}
	}
	if err := decoder.done(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func decodeSQLMutationDependencyQueueFinish(payload []byte, includeReason bool) (string, uint64, string, error) {
	decoder := sqlMutationDependencyQueueDecoder{data: payload}
	id, err := decoder.string(maxSQLMutationTaskIDBytes)
	if err != nil {
		return "", 0, "", err
	}
	attempt, err := decoder.uint64()
	if err != nil {
		return "", 0, "", err
	}
	if attempt == 0 {
		return "", 0, "", fmt.Errorf("%w: zero finish attempt", ErrSQLMutationDependencyQueueCorrupt)
	}
	var reason string
	if includeReason {
		reason, err = decoder.string(maxSQLMutationTaskErrorBytes)
		if err != nil {
			return "", 0, "", err
		}
	}
	if err := decoder.done(); err != nil {
		return "", 0, "", err
	}
	return id, attempt, reason, nil
}

func decodeSQLMutationDependencyQueueID(payload []byte) (string, error) {
	decoder := sqlMutationDependencyQueueDecoder{data: payload}
	id, err := decoder.string(maxSQLMutationTaskIDBytes)
	if err != nil {
		return "", err
	}
	if err := decoder.done(); err != nil {
		return "", err
	}
	return id, nil
}

func decodeSQLMutationDependencyQueueCount(payload []byte) (int, error) {
	decoder := sqlMutationDependencyQueueDecoder{data: payload}
	count, err := decoder.uint32()
	if err != nil {
		return 0, err
	}
	if err := decoder.done(); err != nil {
		return 0, err
	}
	if count > uint32(maxSQLMutationDependencyGraphTasks) {
		return 0, fmt.Errorf("%w: requeue count exceeds limit", ErrSQLMutationDependencyQueueCorrupt)
	}
	return int(count), nil
}

func encodeSQLMutationDependencyQueueState(state SQLMutationTaskState) (byte, error) {
	switch state {
	case SQLMutationTaskPending:
		return 1, nil
	case SQLMutationTaskRunning:
		return 2, nil
	case SQLMutationTaskCompleted:
		return 3, nil
	case SQLMutationTaskFailed:
		return 4, nil
	default:
		return 0, fmt.Errorf("%w: unknown task state %q", ErrSQLMutationDependencyQueueCorrupt, state)
	}
}

func decodeSQLMutationDependencyQueueState(value byte) (SQLMutationTaskState, error) {
	switch value {
	case 1:
		return SQLMutationTaskPending, nil
	case 2:
		return SQLMutationTaskRunning, nil
	case 3:
		return SQLMutationTaskCompleted, nil
	case 4:
		return SQLMutationTaskFailed, nil
	default:
		return "", fmt.Errorf("%w: unknown task state code %d", ErrSQLMutationDependencyQueueCorrupt, value)
	}
}

func decodeSQLMutationDependencyQueueSnapshot(payload []byte) ([]SQLMutationTaskRecord, error) {
	decoder := sqlMutationDependencyQueueDecoder{data: payload}
	count, err := decoder.uint32()
	if err != nil || count > uint32(maxSQLMutationDependencyGraphTasks) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: snapshot task count exceeds limit", ErrSQLMutationDependencyQueueCorrupt)
	}
	tasks := make([]SQLMutationTaskRecord, int(count))
	for index := range tasks {
		tasks[index].ID, err = decoder.string(maxSQLMutationTaskIDBytes)
		if err != nil {
			return nil, err
		}
		dependencyCount, dependencyErr := decoder.uint32()
		if dependencyErr != nil || dependencyCount > uint32(maxSQLMutationDependencyGraphTasks) {
			if dependencyErr != nil {
				return nil, dependencyErr
			}
			return nil, fmt.Errorf("%w: snapshot dependency count exceeds limit", ErrSQLMutationDependencyQueueCorrupt)
		}
		tasks[index].DependsOn = make([]string, int(dependencyCount))
		for dependencyIndex := range tasks[index].DependsOn {
			tasks[index].DependsOn[dependencyIndex], err = decoder.string(maxSQLMutationTaskIDBytes)
			if err != nil {
				return nil, err
			}
		}
		stateCode, stateErr := decoder.bytes(1)
		if stateErr != nil {
			return nil, stateErr
		}
		tasks[index].State, err = decodeSQLMutationDependencyQueueState(stateCode[0])
		if err != nil {
			return nil, err
		}
		tasks[index].Attempt, err = decoder.uint64()
		if err != nil {
			return nil, err
		}
		tasks[index].LastError, err = decoder.string(maxSQLMutationTaskErrorBytes)
		if err != nil {
			return nil, err
		}
	}
	if err := decoder.done(); err != nil {
		return nil, err
	}
	return tasks, nil
}

func replaceSQLMutationDependencyGraphSnapshot(graph *SQLMutationDependencyGraph, records []SQLMutationTaskRecord) error {
	if graph == nil {
		return ErrSQLMutationDependencyGraphNil
	}
	tasks, err := validateSQLMutationDependencySnapshot(SQLMutationDependencyGraphSnapshot{
		Version: sQLMutationDependencySnapshotVersion,
		Tasks:   records,
	})
	if err != nil {
		return err
	}
	if len(tasks) > graph.maxTasks {
		return fmt.Errorf("%w: snapshot has %d tasks, maximum is %d", ErrSQLMutationDependencyGraphCapacity, len(tasks), graph.maxTasks)
	}
	graph.mu.Lock()
	graph.tasks = tasks
	graph.mu.Unlock()
	return nil
}
