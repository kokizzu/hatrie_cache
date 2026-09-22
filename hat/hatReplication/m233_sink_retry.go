package hatReplication

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"
)

const (
	DefaultSinkRetryMaxPending       = 1024
	DefaultSinkRetryMaxBytes   int64 = 64 << 20
	DefaultSinkRetryBaseDelay        = 100 * time.Millisecond
	DefaultSinkRetryMaxDelay         = 30 * time.Second
	MaxSinkRetryPending              = 1 << 20
	MaxSinkRetryBytes                = int64(1 << 30)
	MaxSinkRetrySnapshotBytes        = 8 << 20
	MaxSinkRetrySnapshotItems        = 1 << 20
	sinkRetrySnapshotVersion         = 1
)

var (
	ErrSinkRetryInvalid          = errors.New("hatriecache: sink retry queue is invalid")
	ErrSinkRetryQueueFull        = errors.New("hatriecache: sink retry queue is full")
	ErrSinkRetryConflict         = errors.New("hatriecache: sink retry record conflicts")
	ErrSinkRetryInFlight         = errors.New("hatriecache: sink retry record is in flight")
	ErrSinkRetryNotFound         = errors.New("hatriecache: sink retry record is not found")
	ErrSinkRetryNotInFlight      = errors.New("hatriecache: sink retry record is not in flight")
	ErrSinkRetryAttemptExhausted = errors.New("hatriecache: sink retry attempts are exhausted")
	ErrSinkRetrySnapshotInvalid  = errors.New("hatriecache: sink retry snapshot is invalid")
)

// SinkRetryOptions bounds one retry outbox and controls its deterministic
// exponential backoff. Zero values use the package defaults.
type SinkRetryOptions struct {
	Source     string
	MaxPending int
	MaxBytes   int64
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// SinkRetryEnqueueAction describes how Enqueue handled one output identity.
type SinkRetryEnqueueAction uint8

const (
	SinkRetryEnqueued SinkRetryEnqueueAction = iota + 1
	SinkRetryDuplicate
	SinkRetryReplaced
)

// SinkRetryEnqueueResult reports the queue state after Enqueue.
type SinkRetryEnqueueResult struct {
	Action  SinkRetryEnqueueAction
	Pending int
	Bytes   int64
}

// SinkRetryDelivery is an immutable copy of work claimed by Next.
type SinkRetryDelivery struct {
	Record  ExactlyOnceUpsertSinkRecord
	Attempt uint32
}

// SinkRetryQueueStats describes bounded memory and in-flight work.
type SinkRetryQueueStats struct {
	Pending  int
	Bytes    int64
	InFlight int
}

// SinkRetryQueueSnapshot is the durable state for one retry outbox. An
// in-flight item is restored as immediately due so a crash cannot strand it.
type SinkRetryQueueSnapshot struct {
	Source string                  `json:"source"`
	Items  []SinkRetrySnapshotItem `json:"items"`
}

// SinkRetrySnapshotItem is one pending output and its delivery schedule.
type SinkRetrySnapshotItem struct {
	Record      ExactlyOnceUpsertSinkRecord `json:"record"`
	Attempts    uint32                      `json:"attempts"`
	DueUnixNano int64                       `json:"due_unix_nano"`
	InFlight    bool                        `json:"in_flight"`
}

// SinkRetryQueue is a bounded, deduplicating retry outbox for sink records.
// It does not perform network or filesystem I/O; callers claim work with
// Next and acknowledge or reschedule it with Ack or Retry.
type SinkRetryQueue struct {
	mu        sync.Mutex
	options   SinkRetryOptions
	items     map[string]*sinkRetryEntry
	ready     sinkRetryHeap
	bytes     int64
	nextToken uint64
}

type sinkRetryEntry struct {
	record   ExactlyOnceUpsertSinkRecord
	attempts uint32
	due      time.Time
	token    uint64
	inFlight bool
	size     int64
}

type sinkRetryReadyItem struct {
	outputID string
	due      time.Time
	token    uint64
}

type sinkRetryHeap []*sinkRetryReadyItem

func (h sinkRetryHeap) Len() int { return len(h) }

func (h sinkRetryHeap) Less(left, right int) bool {
	if h[left].due.Equal(h[right].due) {
		return h[left].outputID < h[right].outputID
	}
	return h[left].due.Before(h[right].due)
}

func (h sinkRetryHeap) Swap(left, right int) { h[left], h[right] = h[right], h[left] }

func (h *sinkRetryHeap) Push(value any) { *h = append(*h, value.(*sinkRetryReadyItem)) }

func (h *sinkRetryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// NewSinkRetryQueue creates an empty bounded retry outbox.
func NewSinkRetryQueue(options SinkRetryOptions) (*SinkRetryQueue, error) {
	options, err := normalizeSinkRetryOptions(options)
	if err != nil {
		return nil, err
	}
	queue := &SinkRetryQueue{
		options:   options,
		items:     make(map[string]*sinkRetryEntry),
		nextToken: 1,
	}
	heap.Init(&queue.ready)
	return queue, nil
}

// NewSinkRetryQueueFromSnapshot restores a validated retry outbox snapshot.
func NewSinkRetryQueueFromSnapshot(options SinkRetryOptions, snapshot SinkRetryQueueSnapshot) (*SinkRetryQueue, error) {
	queue, err := NewSinkRetryQueue(options)
	if err != nil {
		return nil, err
	}
	if err := queue.Restore(snapshot); err != nil {
		return nil, err
	}
	return queue, nil
}

// NewSinkRetryQueueFromBinary restores a compact SRT1 snapshot.
func NewSinkRetryQueueFromBinary(options SinkRetryOptions, data []byte) (*SinkRetryQueue, error) {
	snapshot, err := UnmarshalSinkRetryQueueSnapshot(data)
	if err != nil {
		return nil, err
	}
	return NewSinkRetryQueueFromSnapshot(options, snapshot)
}

// Enqueue adds a record due immediately. Identical records are deduplicated;
// a newer pending sequence for the same output identity replaces the older
// record, while an in-flight record is never replaced.
func (queue *SinkRetryQueue) Enqueue(record ExactlyOnceUpsertSinkRecord, now time.Time) (SinkRetryEnqueueResult, error) {
	if queue == nil || validateExactlyOnceUpsertSinkRecord(record) != nil {
		return SinkRetryEnqueueResult{}, ErrSinkRetryInvalid
	}
	now = normalizeSinkRetryTime(now)
	size := sinkRetryRecordSize(record)
	if size > queue.options.MaxBytes {
		return SinkRetryEnqueueResult{}, ErrSinkRetryQueueFull
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if existing := queue.items[record.OutputID]; existing != nil {
		if exactlyOnceUpsertSinkRecordsEqual(existing.record, record) {
			return queue.enqueueResultLocked(SinkRetryDuplicate), nil
		}
		if existing.inFlight {
			return SinkRetryEnqueueResult{}, ErrSinkRetryInFlight
		}
		if record.Sequence <= existing.record.Sequence {
			return SinkRetryEnqueueResult{}, ErrSinkRetryConflict
		}
		if queue.bytes-existing.size+size > queue.options.MaxBytes {
			return SinkRetryEnqueueResult{}, ErrSinkRetryQueueFull
		}
		oldSize := existing.size
		existing.record = cloneExactlyOnceUpsertSinkRecord(record)
		existing.attempts = 0
		existing.due = now
		existing.size = size
		existing.token = queue.nextTokenLocked()
		queue.bytes += size - oldSize
		heap.Push(&queue.ready, &sinkRetryReadyItem{outputID: record.OutputID, due: existing.due, token: existing.token})
		return queue.enqueueResultLocked(SinkRetryReplaced), nil
	}
	if len(queue.items) >= queue.options.MaxPending || queue.bytes+size > queue.options.MaxBytes {
		return SinkRetryEnqueueResult{}, ErrSinkRetryQueueFull
	}
	entry := &sinkRetryEntry{
		record: cloneExactlyOnceUpsertSinkRecord(record),
		due:    now,
		token:  queue.nextTokenLocked(),
		size:   size,
	}
	queue.items[record.OutputID] = entry
	queue.bytes += size
	heap.Push(&queue.ready, &sinkRetryReadyItem{outputID: record.OutputID, due: entry.due, token: entry.token})
	return queue.enqueueResultLocked(SinkRetryEnqueued), nil
}

// Next claims the earliest due record. The returned record is a copy and may
// be safely handed to an external client without retaining queue memory.
func (queue *SinkRetryQueue) Next(now time.Time) (SinkRetryDelivery, bool, error) {
	if queue == nil {
		return SinkRetryDelivery{}, false, ErrSinkRetryInvalid
	}
	now = normalizeSinkRetryTime(now)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for queue.ready.Len() > 0 {
		ready := heap.Pop(&queue.ready).(*sinkRetryReadyItem)
		entry := queue.items[ready.outputID]
		if entry == nil || entry.token != ready.token || entry.inFlight {
			continue
		}
		if ready.due.After(now) {
			heap.Push(&queue.ready, ready)
			return SinkRetryDelivery{}, false, nil
		}
		if entry.attempts == ^uint32(0) {
			heap.Push(&queue.ready, ready)
			return SinkRetryDelivery{}, false, ErrSinkRetryAttemptExhausted
		}
		entry.attempts++
		entry.inFlight = true
		return SinkRetryDelivery{
			Record:  cloneExactlyOnceUpsertSinkRecord(entry.record),
			Attempt: entry.attempts,
		}, true, nil
	}
	return SinkRetryDelivery{}, false, nil
}

// Ack removes one successfully applied in-flight record.
func (queue *SinkRetryQueue) Ack(outputID string, sequence uint64) error {
	if queue == nil || validateExactlyOnceUpsertSinkIdentity(outputID) != nil || sequence == 0 {
		return ErrSinkRetryInvalid
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	entry := queue.items[outputID]
	if entry == nil {
		return ErrSinkRetryNotFound
	}
	if entry.record.Sequence != sequence {
		return ErrSinkRetryConflict
	}
	if !entry.inFlight {
		return ErrSinkRetryNotInFlight
	}
	delete(queue.items, outputID)
	queue.bytes -= entry.size
	return nil
}

// Retry returns one in-flight record to the due heap using capped exponential
// backoff based only on the number of attempts already delivered.
func (queue *SinkRetryQueue) Retry(outputID string, sequence uint64, now time.Time) error {
	if queue == nil || validateExactlyOnceUpsertSinkIdentity(outputID) != nil || sequence == 0 {
		return ErrSinkRetryInvalid
	}
	now = normalizeSinkRetryTime(now)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	entry := queue.items[outputID]
	if entry == nil {
		return ErrSinkRetryNotFound
	}
	if entry.record.Sequence != sequence {
		return ErrSinkRetryConflict
	}
	if !entry.inFlight {
		return ErrSinkRetryNotInFlight
	}
	entry.inFlight = false
	entry.due = now.Add(sinkRetryBackoff(queue.options.BaseDelay, queue.options.MaxDelay, entry.attempts))
	entry.token = queue.nextTokenLocked()
	heap.Push(&queue.ready, &sinkRetryReadyItem{outputID: outputID, due: entry.due, token: entry.token})
	return nil
}

// Stats returns the current bounded queue state.
func (queue *SinkRetryQueue) Stats() SinkRetryQueueStats {
	if queue == nil {
		return SinkRetryQueueStats{}
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.statsLocked()
}

// Snapshot returns a deterministic copy of the queue state.
func (queue *SinkRetryQueue) Snapshot() SinkRetryQueueSnapshot {
	if queue == nil {
		return SinkRetryQueueSnapshot{}
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	items := make([]SinkRetrySnapshotItem, 0, len(queue.items))
	for _, entry := range queue.items {
		item := SinkRetrySnapshotItem{
			Record:   cloneExactlyOnceUpsertSinkRecord(entry.record),
			Attempts: entry.attempts,
			InFlight: entry.inFlight,
		}
		if !entry.inFlight {
			item.DueUnixNano = entry.due.UnixNano()
		}
		items = append(items, item)
	}
	sort.Slice(items, func(left, right int) bool {
		return items[left].Record.OutputID < items[right].Record.OutputID
	})
	return SinkRetryQueueSnapshot{Source: queue.options.Source, Items: items}
}

// Restore atomically replaces queue state after validating all bounds.
func (queue *SinkRetryQueue) Restore(snapshot SinkRetryQueueSnapshot) error {
	if queue == nil {
		return ErrSinkRetryInvalid
	}
	if err := validateSinkRetrySnapshot(snapshot, queue.options); err != nil {
		return err
	}
	items := make(map[string]*sinkRetryEntry, len(snapshot.Items))
	ready := make(sinkRetryHeap, 0, len(snapshot.Items))
	var totalBytes int64
	nextToken := uint64(1)
	for _, item := range snapshot.Items {
		due := time.Unix(0, item.DueUnixNano)
		if item.InFlight {
			due = time.Unix(0, 0)
		}
		entry := &sinkRetryEntry{
			record:   cloneExactlyOnceUpsertSinkRecord(item.Record),
			attempts: item.Attempts,
			due:      due,
			token:    nextToken,
			inFlight: false,
			size:     sinkRetryRecordSize(item.Record),
		}
		nextToken++
		items[item.Record.OutputID] = entry
		totalBytes += entry.size
		ready = append(ready, &sinkRetryReadyItem{outputID: item.Record.OutputID, due: due, token: entry.token})
	}
	heap.Init(&ready)
	queue.mu.Lock()
	queue.items = items
	queue.ready = ready
	queue.bytes = totalBytes
	queue.nextToken = nextToken
	queue.mu.Unlock()
	return nil
}

// MarshalBinary encodes the current queue state as compact SRT1.
func (queue *SinkRetryQueue) MarshalBinary() ([]byte, error) {
	if queue == nil {
		return nil, ErrSinkRetryInvalid
	}
	return queue.Snapshot().MarshalBinary()
}

// UnmarshalBinary decodes and restores a queue snapshot.
func (queue *SinkRetryQueue) UnmarshalBinary(data []byte) error {
	if queue == nil {
		return ErrSinkRetryInvalid
	}
	snapshot, err := UnmarshalSinkRetryQueueSnapshot(data)
	if err != nil {
		return err
	}
	return queue.Restore(snapshot)
}

// MarshalBinary encodes a snapshot without reflection or JSON field names.
func (snapshot SinkRetryQueueSnapshot) MarshalBinary() ([]byte, error) {
	if err := validateSinkRetrySnapshotWithoutOptions(snapshot); err != nil {
		return nil, err
	}
	if sinkRetrySnapshotUpperBound(snapshot) > MaxSinkRetrySnapshotBytes {
		return nil, ErrSinkRetrySnapshotInvalid
	}
	var buffer bytes.Buffer
	buffer.Grow(sinkRetrySnapshotUpperBound(snapshot))
	buffer.WriteString("srt1")
	buffer.WriteByte(sinkRetrySnapshotVersion)
	buffer.WriteByte(0)
	writeExactlyOnceUpsertSinkString(&buffer, snapshot.Source)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(snapshot.Items)))
	buffer.Write(count[:])
	for _, item := range snapshot.Items {
		writeExactlyOnceUpsertSinkUint64(&buffer, item.Record.Sequence)
		var attempts [4]byte
		binary.BigEndian.PutUint32(attempts[:], item.Attempts)
		buffer.Write(attempts[:])
		if item.InFlight {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
		writeExactlyOnceUpsertSinkUint64(&buffer, uint64(item.DueUnixNano))
		writeExactlyOnceUpsertSinkString(&buffer, item.Record.OutputID)
		writeExactlyOnceUpsertSinkBytes(&buffer, item.Record.Key)
		writeExactlyOnceUpsertSinkBytes(&buffer, item.Record.Value)
		if item.Record.Delete {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	}
	if buffer.Len() > MaxSinkRetrySnapshotBytes {
		return nil, ErrSinkRetrySnapshotInvalid
	}
	return buffer.Bytes(), nil
}

// UnmarshalSinkRetryQueueSnapshot decodes and validates SRT1.
func UnmarshalSinkRetryQueueSnapshot(data []byte) (SinkRetryQueueSnapshot, error) {
	if len(data) < 6 || len(data) > MaxSinkRetrySnapshotBytes || string(data[:4]) != "srt1" || data[4] != sinkRetrySnapshotVersion || data[5] != 0 {
		return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
	}
	reader := exactlyOnceUpsertSinkReader{data: data, offset: 6}
	snapshot := SinkRetryQueueSnapshot{}
	var ok bool
	if snapshot.Source, ok = reader.string(); !ok {
		return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
	}
	count, ok := sinkRetryReaderUint32(&reader)
	if !ok || count > MaxSinkRetrySnapshotItems || count > uint32(len(data)) {
		return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
	}
	snapshot.Items = make([]SinkRetrySnapshotItem, int(count))
	for index := range snapshot.Items {
		item := &snapshot.Items[index]
		if item.Record.Sequence, ok = reader.uint64(); !ok {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		if item.Attempts, ok = sinkRetryReaderUint32(&reader); !ok || reader.offset >= len(reader.data) {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		flags := reader.data[reader.offset]
		reader.offset++
		if flags > 1 {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		item.InFlight = flags == 1
		var due uint64
		if due, ok = reader.uint64(); !ok {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		item.DueUnixNano = int64(due)
		if item.Record.OutputID, ok = reader.string(); !ok {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		if item.Record.Key, ok = reader.bytes(MaxExactlyOnceUpsertSinkKeyBytes); !ok {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		if item.Record.Value, ok = reader.bytes(MaxExactlyOnceUpsertSinkValueBytes); !ok || reader.offset >= len(reader.data) {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		deleteFlag := reader.data[reader.offset]
		reader.offset++
		if deleteFlag > 1 {
			return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
		}
		item.Record.Delete = deleteFlag == 1
	}
	if reader.offset != len(reader.data) || validateSinkRetrySnapshotWithoutOptions(snapshot) != nil {
		return SinkRetryQueueSnapshot{}, ErrSinkRetrySnapshotInvalid
	}
	return snapshot, nil
}

func normalizeSinkRetryOptions(options SinkRetryOptions) (SinkRetryOptions, error) {
	if validateExactlyOnceUpsertSinkIdentity(options.Source) != nil {
		return SinkRetryOptions{}, ErrSinkRetryInvalid
	}
	if options.MaxPending == 0 {
		options.MaxPending = DefaultSinkRetryMaxPending
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultSinkRetryMaxBytes
	}
	if options.BaseDelay == 0 {
		options.BaseDelay = DefaultSinkRetryBaseDelay
	}
	if options.MaxDelay == 0 {
		options.MaxDelay = DefaultSinkRetryMaxDelay
	}
	if options.MaxPending < 1 || options.MaxPending > MaxSinkRetryPending || options.MaxBytes < 1 || options.MaxBytes > MaxSinkRetryBytes || options.BaseDelay < 1 || options.MaxDelay < options.BaseDelay {
		return SinkRetryOptions{}, ErrSinkRetryInvalid
	}
	return options, nil
}

func (queue *SinkRetryQueue) nextTokenLocked() uint64 {
	token := queue.nextToken
	queue.nextToken++
	if queue.nextToken == 0 {
		queue.nextToken = 1
	}
	if token == 0 {
		token = queue.nextTokenLocked()
	}
	return token
}

func (queue *SinkRetryQueue) enqueueResultLocked(action SinkRetryEnqueueAction) SinkRetryEnqueueResult {
	return SinkRetryEnqueueResult{Action: action, Pending: len(queue.items), Bytes: queue.bytes}
}

func (queue *SinkRetryQueue) statsLocked() SinkRetryQueueStats {
	stats := SinkRetryQueueStats{Pending: len(queue.items), Bytes: queue.bytes}
	for _, entry := range queue.items {
		if entry.inFlight {
			stats.InFlight++
		}
	}
	return stats
}

func sinkRetryRecordSize(record ExactlyOnceUpsertSinkRecord) int64 {
	return int64(8 + 4 + 1 + len(record.OutputID) + len(record.Key) + len(record.Value))
}

func sinkRetryBackoff(baseDelay, maxDelay time.Duration, attempts uint32) time.Duration {
	delay := baseDelay
	for step := uint32(1); step < attempts; step++ {
		if delay >= maxDelay || delay > maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func normalizeSinkRetryTime(now time.Time) time.Time {
	if now.IsZero() {
		return time.Unix(0, 0)
	}
	return now
}

func validateSinkRetrySnapshot(snapshot SinkRetryQueueSnapshot, options SinkRetryOptions) error {
	if err := validateSinkRetrySnapshotWithoutOptions(snapshot); err != nil {
		return err
	}
	if snapshot.Source != options.Source {
		return ErrSinkRetrySnapshotInvalid
	}
	var totalBytes int64
	for _, item := range snapshot.Items {
		totalBytes += sinkRetryRecordSize(item.Record)
	}
	if len(snapshot.Items) > options.MaxPending || totalBytes > options.MaxBytes {
		return ErrSinkRetrySnapshotInvalid
	}
	return nil
}

func validateSinkRetrySnapshotWithoutOptions(snapshot SinkRetryQueueSnapshot) error {
	if validateExactlyOnceUpsertSinkIdentity(snapshot.Source) != nil || len(snapshot.Items) > MaxSinkRetrySnapshotItems {
		return ErrSinkRetrySnapshotInvalid
	}
	seen := make(map[string]struct{}, len(snapshot.Items))
	for _, item := range snapshot.Items {
		if validateExactlyOnceUpsertSinkRecord(item.Record) != nil || item.Attempts == ^uint32(0) {
			return ErrSinkRetrySnapshotInvalid
		}
		if _, exists := seen[item.Record.OutputID]; exists {
			return ErrSinkRetrySnapshotInvalid
		}
		seen[item.Record.OutputID] = struct{}{}
	}
	return nil
}

func sinkRetrySnapshotUpperBound(snapshot SinkRetryQueueSnapshot) int {
	size := 4 + 1 + 1 + 4 + len(snapshot.Source)
	for _, item := range snapshot.Items {
		size += 8 + 4 + 1 + 8 + 4 + len(item.Record.OutputID) + 4 + len(item.Record.Key) + 4 + len(item.Record.Value) + 1
	}
	return size
}

func sinkRetryReaderUint32(reader *exactlyOnceUpsertSinkReader) (uint32, bool) {
	if reader.offset+4 > len(reader.data) {
		return 0, false
	}
	value := binary.BigEndian.Uint32(reader.data[reader.offset : reader.offset+4])
	reader.offset += 4
	return value, true
}
