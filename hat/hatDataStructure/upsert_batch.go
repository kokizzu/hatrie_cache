package hatDataStructure

import "errors"

var (
	ErrUpsertBatchInvalid    = errors.New("hatriecache: upsert batch is invalid")
	ErrUpsertBatchKeyInvalid = errors.New("hatriecache: upsert batch key is empty")
)

// UpsertRecord is the final operation for one key in an UpsertBatch. Deleted
// records are tombstones and must be applied as deletes by the destination.
type UpsertRecord[T any] struct {
	Key     string `json:"key"`
	Value   T      `json:"value,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

// UpsertBatch consolidates source updates by key. It keeps the first-seen key
// order for deterministic output, replaces later values in place, and retains
// deletes as tombstones. It is not safe for concurrent use.
type UpsertBatch[T any] struct {
	indexes map[string]int
	records []UpsertRecord[T]
}

// NewUpsertBatch creates an empty batch with capacity reserved for capacity
// distinct keys. A non-positive capacity leaves the batch lazy and allocates
// only when the first record is added.
func NewUpsertBatch[T any](capacity int) *UpsertBatch[T] {
	if capacity <= 0 {
		return &UpsertBatch[T]{}
	}
	return &UpsertBatch[T]{
		indexes: make(map[string]int, capacity),
		records: make([]UpsertRecord[T], 0, capacity),
	}
}

// Upsert records the latest value for key.
func (batch *UpsertBatch[T]) Upsert(key string, value T) error {
	if batch == nil {
		return ErrUpsertBatchInvalid
	}
	if key == "" {
		return ErrUpsertBatchKeyInvalid
	}
	if index, ok := batch.indexes[key]; ok {
		batch.records[index].Value = value
		batch.records[index].Deleted = false
		return nil
	}
	if batch.indexes == nil {
		batch.indexes = make(map[string]int)
	}
	batch.indexes[key] = len(batch.records)
	batch.records = append(batch.records, UpsertRecord[T]{Key: key, Value: value})
	return nil
}

// Delete records a tombstone for key. A later Upsert replaces that tombstone.
func (batch *UpsertBatch[T]) Delete(key string) error {
	if batch == nil {
		return ErrUpsertBatchInvalid
	}
	if key == "" {
		return ErrUpsertBatchKeyInvalid
	}
	if index, ok := batch.indexes[key]; ok {
		var zero T
		batch.records[index].Value = zero
		batch.records[index].Deleted = true
		return nil
	}
	if batch.indexes == nil {
		batch.indexes = make(map[string]int)
	}
	batch.indexes[key] = len(batch.records)
	batch.records = append(batch.records, UpsertRecord[T]{Key: key, Deleted: true})
	return nil
}

// Lookup returns the current consolidated operation for key.
func (batch *UpsertBatch[T]) Lookup(key string) (UpsertRecord[T], bool) {
	if batch == nil || key == "" {
		return UpsertRecord[T]{}, false
	}
	index, ok := batch.indexes[key]
	if !ok {
		return UpsertRecord[T]{}, false
	}
	return batch.records[index], true
}

// Len returns the number of distinct keys in the batch, including tombstones.
func (batch *UpsertBatch[T]) Len() int {
	if batch == nil {
		return 0
	}
	return len(batch.records)
}

// ForEach visits consolidated records in first-seen key order.
func (batch *UpsertBatch[T]) ForEach(visit func(UpsertRecord[T])) {
	if batch == nil || visit == nil {
		return
	}
	for _, record := range batch.records {
		visit(record)
	}
}

// Reset removes all records while retaining allocated map and slice capacity
// for reuse by the next source batch.
func (batch *UpsertBatch[T]) Reset() {
	if batch == nil {
		return
	}
	var zero T
	for index := range batch.records {
		batch.records[index].Key = ""
		batch.records[index].Value = zero
		batch.records[index].Deleted = false
	}
	for key := range batch.indexes {
		delete(batch.indexes, key)
	}
	batch.records = batch.records[:0]
}
