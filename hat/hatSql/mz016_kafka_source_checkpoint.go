package hatSql

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrKafkaTableSourceCheckpointStoreRequired reports a missing checkpoint store.
	ErrKafkaTableSourceCheckpointStoreRequired = errors.New("Kafka table source checkpoint store is required")
	// ErrKafkaTableSourceCheckpointInvalid reports an invalid stored snapshot.
	ErrKafkaTableSourceCheckpointInvalid = errors.New("Kafka table source checkpoint is invalid")
	// ErrKafkaTableSourceCheckpointLoad reports a checkpoint load failure.
	ErrKafkaTableSourceCheckpointLoad = errors.New("Kafka table source checkpoint load failed")
	// ErrKafkaTableSourceCheckpointCommit reports a checkpoint commit failure.
	ErrKafkaTableSourceCheckpointCommit = errors.New("Kafka table source checkpoint commit failed")
)

// KafkaTableSourceCheckpointStore durably replaces one complete source
// snapshot. Implementations must make Commit atomic: rows, offsets, and
// ingestion replay markers must become visible together, or not at all.
type KafkaTableSourceCheckpointStore interface {
	Load(context.Context, string) (KafkaTableSourceSnapshot, bool, error)
	Commit(context.Context, KafkaTableSourceSnapshot) error
}

// ApplyBatchWithCheckpoint applies one source transaction and commits its
// rows, offsets, and replay markers as one durable checkpoint. The source lock
// remains held while Commit runs so no concurrent batch can be omitted from or
// included in the checkpoint accidentally. A failed Commit restores the exact
// pre-batch state.
func (source *KafkaTableSource) ApplyBatchWithCheckpoint(ctx context.Context, batch KafkaTableBatch, store KafkaTableSourceCheckpointStore) (KafkaTableIngestResult, error) {
	if source == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceNil
	}
	if store == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceCheckpointStoreRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return KafkaTableIngestResult{}, err
	}
	normalizedMessages, offsets, transactionID, err := source.normalizeBatch(batch)
	if err != nil {
		return KafkaTableIngestResult{}, err
	}

	source.mu.Lock()
	defer source.mu.Unlock()
	previous := source.snapshotLocked()
	previousStats := source.stats
	result, err := source.applyKafkaTableBatchLocked(normalizedMessages, offsets, transactionID)
	if err != nil {
		return KafkaTableIngestResult{}, err
	}
	if err := store.Commit(ctx, source.snapshotLocked()); err != nil {
		rollbackErr := source.restoreSnapshotLocked(previous)
		source.stats = previousStats
		if rollbackErr != nil {
			return KafkaTableIngestResult{}, fmt.Errorf("%w: %v; rollback: %v", ErrKafkaTableSourceCheckpointCommit, err, rollbackErr)
		}
		return KafkaTableIngestResult{}, fmt.Errorf("%w: %v", ErrKafkaTableSourceCheckpointCommit, err)
	}
	return result, nil
}

// RestoreCheckpoint loads and validates the latest complete source snapshot.
// It returns false when the store has no checkpoint for this source.
func (source *KafkaTableSource) RestoreCheckpoint(ctx context.Context, store KafkaTableSourceCheckpointStore) (bool, error) {
	if source == nil {
		return false, ErrKafkaTableSourceNil
	}
	if store == nil {
		return false, ErrKafkaTableSourceCheckpointStoreRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	snapshot, found, err := store.Load(ctx, source.source)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrKafkaTableSourceCheckpointLoad, err)
	}
	if !found {
		return false, nil
	}
	if err := source.Restore(snapshot); err != nil {
		return false, fmt.Errorf("%w: %v", ErrKafkaTableSourceCheckpointInvalid, err)
	}
	return true, nil
}

// ConsumeOnceWithCheckpoint persists the complete source state before asking
// the broker consumer to commit offsets. If the process stops after the store
// commit but before the broker commit, restart restores the replay marker and
// the repeated batch is a safe duplicate.
func (source *KafkaTableSource) ConsumeOnceWithCheckpoint(ctx context.Context, consumer KafkaTableConsumer, store KafkaTableSourceCheckpointStore) (KafkaTableIngestResult, error) {
	if source == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceNil
	}
	if consumer == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceConsumerRequired
	}
	if store == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceCheckpointStoreRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	batch, err := consumer.Poll(ctx)
	if err != nil {
		return KafkaTableIngestResult{}, err
	}
	if len(batch.Messages) == 0 {
		return KafkaTableIngestResult{}, nil
	}
	result, err := source.ApplyBatchWithCheckpoint(ctx, batch, store)
	if err != nil {
		return KafkaTableIngestResult{}, err
	}
	if err := consumer.Commit(ctx, cloneKafkaTableOffsets(result.Offsets)); err != nil {
		return result, err
	}
	result.ConsumerCommitted = true
	return result, nil
}

func (source *KafkaTableSource) applyKafkaTableBatchLocked(normalizedMessages []KafkaTableMessage, offsets []SQLSourceOffset, transactionID string) (KafkaTableIngestResult, error) {
	envelope := SQLSourceTransactionEnvelope{
		Source: source.source,
		Transaction: SQLSourceTransaction{
			ID:      transactionID,
			Offsets: offsets,
		},
		Relations: []string{source.table},
	}
	wasCommitted := source.ingestions.Committed(source.source, transactionID)
	result := KafkaTableIngestResult{
		TransactionID: transactionID,
		Offsets:       source.effectiveOffsetsLocked(offsets),
	}
	committed, err := source.ingestions.IngestEnvelope(envelope, func() error {
		applied, skipped, err := source.applyMessagesLocked(normalizedMessages, offsets)
		result.AppliedMessages = applied
		result.SkippedMessages = skipped
		return err
	})
	if err != nil {
		return KafkaTableIngestResult{}, err
	}
	result.Committed = committed || wasCommitted
	result.Duplicate = !committed && wasCommitted
	if committed {
		source.stats.AppliedBatches++
	} else if result.Duplicate {
		source.stats.DuplicateBatches++
	}
	return result, nil
}

func (source *KafkaTableSource) snapshotLocked() KafkaTableSourceSnapshot {
	rows := make(map[string]Row, len(source.rows))
	for key, row := range source.rows {
		rows[key] = cloneKafkaTableRow(row)
	}
	return KafkaTableSourceSnapshot{
		Source:     source.source,
		Table:      source.table,
		Topic:      source.topic,
		SourceKind: source.sourceKind,
		Rows:       rows,
		Offsets:    source.offsets.Snapshot(),
		Ingestions: source.ingestions.SnapshotEnvelopes(),
	}
}

func (source *KafkaTableSource) restoreSnapshotLocked(snapshot KafkaTableSourceSnapshot) error {
	if snapshot.Source != source.source || snapshot.Table != source.table || snapshot.Topic != source.topic || snapshot.SourceKind != source.sourceKind {
		return ErrKafkaTableSourceCheckpointInvalid
	}
	replacementRows := make(map[string]Row, len(snapshot.Rows))
	for key, row := range snapshot.Rows {
		if err := validateKafkaTableKey(key); err != nil || row == nil {
			return fmt.Errorf("%w: row %q", ErrKafkaTableSourceCheckpointInvalid, key)
		}
		replacementRows[key] = cloneKafkaTableRow(row)
	}
	replacementOffsets := NewSQLSourceOffsetTracker()
	if err := replacementOffsets.Restore(snapshot.Offsets); err != nil {
		return fmt.Errorf("%w: offsets: %v", ErrKafkaTableSourceCheckpointInvalid, err)
	}
	replacementIngestions := NewSQLSourceIngestionCoordinator()
	if err := replacementIngestions.RestoreEnvelopes(snapshot.Ingestions); err != nil {
		return fmt.Errorf("%w: ingestions: %v", ErrKafkaTableSourceCheckpointInvalid, err)
	}
	source.rows = replacementRows
	source.offsets = replacementOffsets
	source.ingestions = replacementIngestions
	return nil
}
