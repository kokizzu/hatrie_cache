package hatSql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultKafkaTableSourceKind is the resolver kind used by Kafka table
	// sources in CatalogResolver.
	DefaultKafkaTableSourceKind = "KAFKA"
	// DefaultKafkaTableSourceMaxBatchMessages bounds one polled batch when no
	// application-specific limit is supplied.
	DefaultKafkaTableSourceMaxBatchMessages = 10000
	// MaxKafkaTableSourceMaxBatchMessages prevents an accidental unbounded
	// allocation from a configuration value.
	MaxKafkaTableSourceMaxBatchMessages = 1000000
	// MaxKafkaTableMessageBytes bounds one record value accepted by the
	// dependency-free adapter.
	MaxKafkaTableMessageBytes = 16 << 20
	// MaxKafkaTableSourceNameBytes bounds source, table, topic, and partition
	// labels retained in checkpoint metadata.
	MaxKafkaTableSourceNameBytes = 256
	// MaxKafkaTableSourceKeyBytes bounds one primary key retained in the
	// in-memory table.
	MaxKafkaTableSourceKeyBytes = 4096
	// MaxKafkaTableTransactionIDBytes bounds a caller-supplied transaction ID.
	MaxKafkaTableTransactionIDBytes = 512
)

var (
	// ErrKafkaTableSourceNil reports a nil source receiver.
	ErrKafkaTableSourceNil = errors.New("Kafka table source is nil")
	// ErrKafkaTableSourceInvalid reports malformed source configuration or
	// batches.
	ErrKafkaTableSourceInvalid = errors.New("Kafka table source is invalid")
	// ErrKafkaTableSourceDecoderRequired reports a missing record decoder.
	ErrKafkaTableSourceDecoderRequired = errors.New("Kafka table source decoder is required")
	// ErrKafkaTableSourceBatchTooLarge reports a batch over the configured
	// message limit.
	ErrKafkaTableSourceBatchTooLarge = errors.New("Kafka table source batch is too large")
	// ErrKafkaTableSourceConsumerRequired reports a missing poll/commit adapter.
	ErrKafkaTableSourceConsumerRequired = errors.New("Kafka table source consumer is required")
	// ErrKafkaTableSourceMessageInvalid reports a message that cannot be safely
	// associated with this table.
	ErrKafkaTableSourceMessageInvalid = errors.New("Kafka table source message is invalid")
	// ErrKafkaTableSourceSnapshotInvalid reports a snapshot for another source
	// or a malformed snapshot payload.
	ErrKafkaTableSourceSnapshotInvalid = errors.New("Kafka table source snapshot is invalid")
)

// KafkaTableOperation identifies the row mutation represented by a record.
type KafkaTableOperation uint8

const (
	KafkaTableUpsert KafkaTableOperation = iota + 1
	KafkaTableDelete
)

// KafkaTableMessage is the dependency-free subset of a Kafka record needed by
// the table adapter. Partition is a string so callers can preserve a broker's
// partition identity without importing a Kafka client.
type KafkaTableMessage struct {
	Topic     string
	Partition string
	Offset    uint64
	Key       string
	Value     []byte
	Timestamp time.Time
}

// KafkaTableChange is the decoded primary-key mutation for one record.
type KafkaTableChange struct {
	Key       string
	Operation KafkaTableOperation
	Row       Row
}

// KafkaTableDecoder converts a broker record into an upsert or tombstone.
// The decoder is called only for records newer than the stored partition
// checkpoint.
type KafkaTableDecoder func(KafkaTableMessage) (KafkaTableChange, error)

// KafkaTableBatch is one polled source batch. TransactionID is optional; when
// omitted, a deterministic ID derived from the batch's partition offsets is
// used for replay protection.
type KafkaTableBatch struct {
	TransactionID string
	Messages      []KafkaTableMessage
}

// KafkaTableSourceOptions configures a primary-key table backed by a Kafka
// topic. Source is the durable checkpoint identity; Table and Topic identify
// the SQL resolver and broker stream respectively.
type KafkaTableSourceOptions struct {
	Source           string
	Table            string
	Topic            string
	SourceKind       string
	Decoder          KafkaTableDecoder
	MaxBatchMessages int
}

// KafkaTableConsumer is the narrow polling/commit contract an actual Kafka
// client adapter must implement. Offsets passed to Commit are the last
// successfully applied offsets, not Kafka's next-offset convention.
type KafkaTableConsumer interface {
	Poll(context.Context) (KafkaTableBatch, error)
	Commit(context.Context, []SQLSourceOffset) error
}

// KafkaTableIngestResult reports local application and broker-commit state.
type KafkaTableIngestResult struct {
	TransactionID     string
	Offsets           []SQLSourceOffset
	AppliedMessages   int
	SkippedMessages   int
	Committed         bool
	Duplicate         bool
	ConsumerCommitted bool
}

// KafkaTableSourceStats reports cumulative source activity.
type KafkaTableSourceStats struct {
	Rows             int
	AppliedBatches   uint64
	DuplicateBatches uint64
	AppliedMessages  uint64
	SkippedMessages  uint64
}

// KafkaTableSourceSnapshot is a self-contained restore point for table rows,
// partition checkpoints, and replay markers. Callers can serialize it with
// their existing backup format.
type KafkaTableSourceSnapshot struct {
	Source     string                         `json:"source"`
	Table      string                         `json:"table"`
	Topic      string                         `json:"topic"`
	SourceKind string                         `json:"source_kind"`
	Lifecycle  KafkaTableSourceLifecycle      `json:"lifecycle"`
	Rows       map[string]Row                 `json:"rows"`
	Offsets    []SQLSourceOffset              `json:"offsets"`
	Ingestions []SQLSourceTransactionEnvelope `json:"ingestions"`
}

// KafkaTableSource stores the latest row for each Kafka key and its durable
// source progress. It implements SourceResolver for SQL table reads.
type KafkaTableSource struct {
	mu               sync.RWMutex
	source           string
	table            string
	topic            string
	sourceKind       string
	decoder          KafkaTableDecoder
	maxBatchMessages int
	rows             map[string]Row
	offsets          *SQLSourceOffsetTracker
	ingestions       *SQLSourceIngestionCoordinator
	lifecycle        KafkaTableSourceLifecycle
	stats            KafkaTableSourceStats
}

var _ SourceResolver = (*KafkaTableSource)(nil)

// NewKafkaTableSource creates an empty Kafka-backed SQL table source.
func NewKafkaTableSource(options KafkaTableSourceOptions) (*KafkaTableSource, error) {
	source, err := normalizeKafkaTableSourceName(options.Source, "source")
	if err != nil {
		return nil, err
	}
	table, err := normalizeKafkaTableSourceName(options.Table, "table")
	if err != nil {
		return nil, err
	}
	topic, err := normalizeKafkaTableSourceName(options.Topic, "topic")
	if err != nil {
		return nil, err
	}
	sourceKind := options.SourceKind
	if strings.TrimSpace(sourceKind) == "" {
		sourceKind = DefaultKafkaTableSourceKind
	}
	sourceKind, err = normalizeKafkaTableSourceName(sourceKind, "source kind")
	if err != nil {
		return nil, err
	}
	if options.Decoder == nil {
		return nil, ErrKafkaTableSourceDecoderRequired
	}
	maxBatchMessages := options.MaxBatchMessages
	if maxBatchMessages == 0 {
		maxBatchMessages = DefaultKafkaTableSourceMaxBatchMessages
	}
	if maxBatchMessages < 1 || maxBatchMessages > MaxKafkaTableSourceMaxBatchMessages {
		return nil, fmt.Errorf("%w: max batch messages %d", ErrKafkaTableSourceInvalid, maxBatchMessages)
	}
	return &KafkaTableSource{
		source:           source,
		table:            table,
		topic:            topic,
		sourceKind:       sourceKind,
		decoder:          options.Decoder,
		maxBatchMessages: maxBatchMessages,
		offsets:          NewSQLSourceOffsetTracker(),
		ingestions:       NewSQLSourceIngestionCoordinator(),
		lifecycle:        KafkaTableSourceLifecycle{State: KafkaTableSourceLifecycleRunning},
	}, nil
}

// ResolveSQLSource exposes a deterministic row snapshot to the SQL executor.
func (source *KafkaTableSource) ResolveSQLSource(name, key string) ([]Row, error) {
	if source == nil {
		return nil, ErrKafkaTableSourceNil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	if !strings.EqualFold(strings.TrimSpace(name), source.sourceKind) || strings.TrimSpace(key) != source.table {
		return nil, fmt.Errorf("%w: resolver %q/%q does not match %q/%q", ErrKafkaTableSourceInvalid, name, key, source.sourceKind, source.table)
	}
	keys := make([]string, 0, len(source.rows))
	for rowKey := range source.rows {
		keys = append(keys, rowKey)
	}
	sort.Strings(keys)
	rows := make([]Row, 0, len(keys))
	for _, rowKey := range keys {
		rows = append(rows, cloneKafkaTableRow(source.rows[rowKey]))
	}
	return rows, nil
}

// ApplyBatch decodes and applies a batch as one atomic source transaction.
// Checkpoints advance only after every new record decodes successfully.
func (source *KafkaTableSource) ApplyBatch(batch KafkaTableBatch) (KafkaTableIngestResult, error) {
	if source == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceNil
	}
	normalizedMessages, offsets, transactionID, err := source.normalizeBatch(batch)
	if err != nil {
		return KafkaTableIngestResult{}, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.lifecycle.State == KafkaTableSourceLifecyclePaused {
		return KafkaTableIngestResult{}, ErrKafkaTableSourcePaused
	}
	return source.applyKafkaTableBatchLocked(normalizedMessages, offsets, transactionID)
}

// ConsumeOnce polls, applies, and then commits one source batch. A consumer
// commit failure is returned after local application; replay remains safe.
func (source *KafkaTableSource) ConsumeOnce(ctx context.Context, consumer KafkaTableConsumer) (KafkaTableIngestResult, error) {
	if source == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceNil
	}
	if consumer == nil {
		return KafkaTableIngestResult{}, ErrKafkaTableSourceConsumerRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if source.isPaused() {
		return KafkaTableIngestResult{}, ErrKafkaTableSourcePaused
	}
	batch, err := consumer.Poll(ctx)
	if err != nil {
		return KafkaTableIngestResult{}, err
	}
	if len(batch.Messages) == 0 {
		return KafkaTableIngestResult{}, nil
	}
	result, err := source.ApplyBatch(batch)
	if err != nil {
		return KafkaTableIngestResult{}, err
	}
	if err := consumer.Commit(ctx, cloneKafkaTableOffsets(result.Offsets)); err != nil {
		return result, err
	}
	result.ConsumerCommitted = true
	return result, nil
}

// Offsets returns deterministic copies of the current partition checkpoints.
func (source *KafkaTableSource) Offsets() []SQLSourceOffset {
	if source == nil {
		return nil
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	return source.offsets.Snapshot()
}

// Stats returns a point-in-time source activity summary.
func (source *KafkaTableSource) Stats() KafkaTableSourceStats {
	if source == nil {
		return KafkaTableSourceStats{}
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	stats := source.stats
	stats.Rows = len(source.rows)
	return stats
}

// Snapshot returns rows, checkpoints, and committed transaction metadata with
// independently owned values.
func (source *KafkaTableSource) Snapshot() KafkaTableSourceSnapshot {
	if source == nil {
		return KafkaTableSourceSnapshot{}
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	rows := make(map[string]Row, len(source.rows))
	for key, row := range source.rows {
		rows[key] = cloneKafkaTableRow(row)
	}
	return KafkaTableSourceSnapshot{
		Source:     source.source,
		Table:      source.table,
		Topic:      source.topic,
		SourceKind: source.sourceKind,
		Lifecycle:  source.lifecycle,
		Rows:       rows,
		Offsets:    source.offsets.Snapshot(),
		Ingestions: source.ingestions.SnapshotEnvelopes(),
	}
}

// Restore atomically replaces rows, offsets, and replay metadata after
// validating the complete snapshot.
func (source *KafkaTableSource) Restore(snapshot KafkaTableSourceSnapshot) error {
	if source == nil {
		return ErrKafkaTableSourceNil
	}
	if snapshot.Source != source.source || snapshot.Table != source.table || snapshot.Topic != source.topic || snapshot.SourceKind != source.sourceKind {
		return ErrKafkaTableSourceSnapshotInvalid
	}
	replacementRows := make(map[string]Row, len(snapshot.Rows))
	for key, row := range snapshot.Rows {
		if err := validateKafkaTableKey(key); err != nil || row == nil {
			return fmt.Errorf("%w: row %q", ErrKafkaTableSourceSnapshotInvalid, key)
		}
		replacementRows[key] = cloneKafkaTableRow(row)
	}
	replacementOffsets := NewSQLSourceOffsetTracker()
	if err := replacementOffsets.Restore(snapshot.Offsets); err != nil {
		return fmt.Errorf("%w: offsets: %w", ErrKafkaTableSourceSnapshotInvalid, err)
	}
	replacementIngestions := NewSQLSourceIngestionCoordinator()
	if err := replacementIngestions.RestoreEnvelopes(snapshot.Ingestions); err != nil {
		return fmt.Errorf("%w: ingestions: %w", ErrKafkaTableSourceSnapshotInvalid, err)
	}
	lifecycle, err := normalizeKafkaTableSourceLifecycle(snapshot.Lifecycle)
	if err != nil {
		return fmt.Errorf("%w: lifecycle: %v", ErrKafkaTableSourceSnapshotInvalid, err)
	}
	source.mu.Lock()
	source.rows = replacementRows
	source.offsets = replacementOffsets
	source.ingestions = replacementIngestions
	source.lifecycle = lifecycle
	source.stats = KafkaTableSourceStats{}
	source.mu.Unlock()
	return nil
}

func (source *KafkaTableSource) normalizeBatch(batch KafkaTableBatch) ([]KafkaTableMessage, []SQLSourceOffset, string, error) {
	if len(batch.Messages) == 0 {
		return nil, nil, "", fmt.Errorf("%w: empty batch", ErrKafkaTableSourceInvalid)
	}
	if len(batch.Messages) > source.maxBatchMessages {
		return nil, nil, "", ErrKafkaTableSourceBatchTooLarge
	}
	normalizedMessages := batch.Messages
	clonedMessages := false
	partitionOffsets := make(map[string]uint64)
	for index, message := range batch.Messages {
		message.Topic = strings.TrimSpace(message.Topic)
		message.Partition = strings.TrimSpace(message.Partition)
		message.Key = strings.TrimSpace(message.Key)
		if message.Topic != source.topic || !validKafkaTableLabel(message.Partition, MaxKafkaTableSourceNameBytes) || len(message.Value) > MaxKafkaTableMessageBytes {
			return nil, nil, "", fmt.Errorf("%w: topic=%q partition=%q", ErrKafkaTableSourceMessageInvalid, message.Topic, message.Partition)
		}
		if clonedMessages || message.Topic != batch.Messages[index].Topic || message.Partition != batch.Messages[index].Partition || message.Key != batch.Messages[index].Key {
			if !clonedMessages {
				normalizedMessages = append([]KafkaTableMessage(nil), batch.Messages...)
				clonedMessages = true
			}
			normalizedMessages[index] = message
		}
		if current, found := partitionOffsets[message.Partition]; !found || message.Offset > current {
			partitionOffsets[message.Partition] = message.Offset
		}
	}
	offsets := make([]SQLSourceOffset, 0, len(partitionOffsets))
	for partition, offset := range partitionOffsets {
		offsets = append(offsets, SQLSourceOffset{Source: source.source, Partition: partition, Offset: offset})
	}
	sort.Slice(offsets, func(left, right int) bool { return offsets[left].Partition < offsets[right].Partition })
	transactionID := strings.TrimSpace(batch.TransactionID)
	if transactionID == "" {
		transactionID = kafkaTableDerivedTransactionID(offsets)
	}
	if !validKafkaTableLabel(transactionID, MaxKafkaTableTransactionIDBytes) || strings.IndexByte(transactionID, 0) >= 0 {
		return nil, nil, "", fmt.Errorf("%w: transaction ID", ErrKafkaTableSourceInvalid)
	}
	return normalizedMessages, offsets, transactionID, nil
}

type kafkaTablePartitionProgress struct {
	offset uint64
	found  bool
}

func (source *KafkaTableSource) applyMessagesLocked(messages []KafkaTableMessage, offsets []SQLSourceOffset) (int, int, error) {
	changes := make([]KafkaTableChange, 0, len(messages))
	partitionProgress := make(map[string]kafkaTablePartitionProgress, len(offsets))
	for _, offset := range offsets {
		current, found := source.offsets.Offset(offset.Source, offset.Partition)
		partitionProgress[offset.Partition] = kafkaTablePartitionProgress{offset: current, found: found}
	}
	skipped := 0
	for _, message := range messages {
		progress := partitionProgress[message.Partition]
		if progress.found && message.Offset <= progress.offset {
			skipped++
			continue
		}
		change, err := source.decoder(message)
		if err != nil {
			return 0, 0, fmt.Errorf("%w: partition=%q offset=%d: %v", ErrKafkaTableSourceMessageInvalid, message.Partition, message.Offset, err)
		}
		if strings.TrimSpace(change.Key) == "" {
			change.Key = message.Key
		}
		change.Key = strings.TrimSpace(change.Key)
		if err := validateKafkaTableKey(change.Key); err != nil {
			return 0, 0, err
		}
		switch change.Operation {
		case KafkaTableUpsert:
			if change.Row == nil {
				return 0, 0, fmt.Errorf("%w: upsert row is nil", ErrKafkaTableSourceMessageInvalid)
			}
			change.Row = cloneKafkaTableRow(change.Row)
		case KafkaTableDelete:
			change.Row = nil
		default:
			return 0, 0, fmt.Errorf("%w: unknown operation %d", ErrKafkaTableSourceMessageInvalid, change.Operation)
		}
		changes = append(changes, change)
		partitionProgress[message.Partition] = kafkaTablePartitionProgress{offset: message.Offset, found: true}
	}
	if source.rows == nil {
		source.rows = make(map[string]Row, len(messages))
	}
	for _, change := range changes {
		if change.Operation == KafkaTableDelete {
			delete(source.rows, change.Key)
			continue
		}
		source.rows[change.Key] = change.Row
	}
	if _, err := source.offsets.AdvanceBatch(offsets); err != nil {
		return 0, 0, err
	}
	source.stats.AppliedMessages += uint64(len(changes))
	source.stats.SkippedMessages += uint64(skipped)
	return len(changes), skipped, nil
}

func (source *KafkaTableSource) effectiveOffsetsLocked(offsets []SQLSourceOffset) []SQLSourceOffset {
	effective := cloneKafkaTableOffsets(offsets)
	for index := range effective {
		if current, found := source.offsets.Offset(effective[index].Source, effective[index].Partition); found && current > effective[index].Offset {
			effective[index].Offset = current
		}
	}
	return effective
}

func kafkaTableDerivedTransactionID(offsets []SQLSourceOffset) string {
	var builder strings.Builder
	builder.WriteString("offsets:")
	for index, offset := range offsets {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(offset.Partition)
		builder.WriteByte('=')
		builder.WriteString(strconv.FormatUint(offset.Offset, 10))
	}
	return builder.String()
}

func normalizeKafkaTableSourceName(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if !validKafkaTableLabel(value, MaxKafkaTableSourceNameBytes) || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: %s", ErrKafkaTableSourceInvalid, label)
	}
	return value, nil
}

func validKafkaTableLabel(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes && strings.IndexByte(value, 0) < 0
}

func validateKafkaTableKey(key string) error {
	if !validKafkaTableLabel(key, MaxKafkaTableSourceKeyBytes) {
		return fmt.Errorf("%w: primary key", ErrKafkaTableSourceMessageInvalid)
	}
	return nil
}

func cloneKafkaTableOffsets(offsets []SQLSourceOffset) []SQLSourceOffset {
	if len(offsets) == 0 {
		return nil
	}
	cloned := append([]SQLSourceOffset(nil), offsets...)
	return cloned
}

func cloneKafkaTableRow(row Row) Row {
	if row == nil {
		return nil
	}
	cloned := make(Row, len(row))
	for key, value := range row {
		cloned[key] = cloneKafkaTableValue(value)
	}
	return cloned
}

func cloneKafkaTableValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case Row:
		return cloneKafkaTableRow(typed)
	case map[string]interface{}:
		cloned := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			cloned[key] = cloneKafkaTableValue(nested)
		}
		return cloned
	case []interface{}:
		cloned := make([]interface{}, len(typed))
		for index, nested := range typed {
			cloned[index] = cloneKafkaTableValue(nested)
		}
		return cloned
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}

// KafkaTableJSONDecoder interprets a JSON object as an upsert and a Kafka
// tombstone (nil Value) as a delete. The Kafka record key is the table key.
func KafkaTableJSONDecoder(message KafkaTableMessage) (KafkaTableChange, error) {
	if message.Value == nil {
		return KafkaTableChange{Key: message.Key, Operation: KafkaTableDelete}, nil
	}
	var row Row
	if err := json.Unmarshal(message.Value, &row); err != nil {
		return KafkaTableChange{}, err
	}
	if row == nil {
		return KafkaTableChange{}, errors.New("JSON value must be an object")
	}
	return KafkaTableChange{Key: message.Key, Operation: KafkaTableUpsert, Row: row}, nil
}
