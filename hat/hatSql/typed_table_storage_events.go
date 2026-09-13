package hatSql

import "time"

const typedTableStorageEventLogDefaultCapacity = 256

// TypedTableStorageEventLogOptions configures the optional bounded storage
// lifecycle history. It is disabled by default and retains only the newest
// Capacity events when enabled.
type TypedTableStorageEventLogOptions struct {
	Enabled  bool
	Capacity int
}

// TypedTableStorageEventKind identifies one typed-table storage lifecycle
// event. Base parts represent the first physical row storage, while patch
// parts represent batches of deferred logical deletes.
type TypedTableStorageEventKind string

const (
	TypedTableStorageEventBasePartCreated  TypedTableStorageEventKind = "base_part_created"
	TypedTableStorageEventPatchPartCreated TypedTableStorageEventKind = "patch_part_created"
	TypedTableStorageEventPatchPartMerged  TypedTableStorageEventKind = "patch_part_merged"
)

// TypedTableStorageEvent is an immutable snapshot of one retained storage
// lifecycle event. Rows are physical row counts; PendingDeletes describes the
// tombstones still waiting for a patch merge.
type TypedTableStorageEvent struct {
	Sequence           uint64
	TableSequence      uint64
	Kind               TypedTableStorageEventKind
	At                 time.Time
	PhysicalRowsBefore int
	PhysicalRowsAfter  int
	PendingDeletes     int
	DeletedRows        int
	Duration           time.Duration
}

type typedTableStorageEventLog struct {
	events       []TypedTableStorageEvent
	start        int
	nextSequence uint64
}

func normalizeTypedTableStorageEventLogOptions(options TypedTableStorageEventLogOptions) TypedTableStorageEventLogOptions {
	if !options.Enabled {
		return TypedTableStorageEventLogOptions{}
	}
	if options.Capacity <= 0 {
		options.Capacity = typedTableStorageEventLogDefaultCapacity
	}
	return options
}

func newTypedTableStorageEventLog(options TypedTableStorageEventLogOptions) *typedTableStorageEventLog {
	if !options.Enabled {
		return nil
	}
	return &typedTableStorageEventLog{
		events: make([]TypedTableStorageEvent, 0, options.Capacity),
	}
}

func (log *typedTableStorageEventLog) append(tableSequence uint64, kind TypedTableStorageEventKind, physicalRowsBefore, physicalRowsAfter, pendingDeletes, deletedRows int, duration time.Duration) {
	log.nextSequence++
	event := TypedTableStorageEvent{
		Sequence:           log.nextSequence,
		TableSequence:      tableSequence,
		Kind:               kind,
		At:                 time.Now().UTC(),
		PhysicalRowsBefore: physicalRowsBefore,
		PhysicalRowsAfter:  physicalRowsAfter,
		PendingDeletes:     pendingDeletes,
		DeletedRows:        deletedRows,
		Duration:           duration,
	}
	if len(log.events) < cap(log.events) {
		log.events = append(log.events, event)
		return
	}
	log.events[log.start] = event
	log.start = (log.start + 1) % len(log.events)
}

func (log *typedTableStorageEventLog) snapshot(limit int) []TypedTableStorageEvent {
	if len(log.events) == 0 {
		return nil
	}
	count := len(log.events)
	if limit > 0 && limit < count {
		count = limit
	}
	start := log.start
	if count < len(log.events) {
		start = (start + len(log.events) - count) % len(log.events)
	}
	events := make([]TypedTableStorageEvent, count)
	for index := range events {
		events[index] = log.events[(start+index)%len(log.events)]
	}
	return events
}

// StorageEvents returns retained lifecycle events in chronological order.
// A positive limit returns only the newest limit events. The bool is false
// when the opt-in event log is disabled.
func (table *TypedTable) StorageEvents(limit int) ([]TypedTableStorageEvent, bool) {
	if table == nil {
		return nil, false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.storageEvents == nil {
		return nil, false
	}
	return table.storageEvents.snapshot(limit), true
}

func (table *TypedTable) recordStorageEventLocked(kind TypedTableStorageEventKind, physicalRowsBefore, physicalRowsAfter, pendingDeletes, deletedRows int, duration time.Duration) {
	if table.storageEvents == nil {
		return
	}
	table.storageEvents.append(table.sequence, kind, physicalRowsBefore, physicalRowsAfter, pendingDeletes, deletedRows, duration)
}
