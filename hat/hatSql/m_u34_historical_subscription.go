package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
)

const (
	querySubscriptionCheckpointMagic                       = "HQS1"
	querySubscriptionCheckpointVersion                byte = 1
	DefaultQuerySubscriptionCheckpointMaxEncodedBytes      = 64 << 20
	DefaultQuerySubscriptionCheckpointMaxRows              = 1 << 20
	DefaultQuerySubscriptionCheckpointMaxFieldsPerRow      = 1024
	DefaultQuerySubscriptionCheckpointMaxValueDepth        = 16
	querySubscriptionCheckpointComplete                    = 1 << 0
	querySubscriptionCheckpointCanceled                    = 1 << 1
	querySubscriptionCheckpointHasMore                     = 1 << 2
)

var (
	// ErrQuerySubscriptionCheckpointInvalid reports invalid checkpoint state
	// or codec options.
	ErrQuerySubscriptionCheckpointInvalid = errors.New("query subscription checkpoint is invalid")
	// ErrQuerySubscriptionCheckpointCorrupt reports a malformed or checksum-
	// invalid checkpoint payload.
	ErrQuerySubscriptionCheckpointCorrupt = errors.New("query subscription checkpoint is corrupt")
	// ErrQuerySubscriptionCheckpointTooLarge reports a configured checkpoint
	// bound.
	ErrQuerySubscriptionCheckpointTooLarge = errors.New("query subscription checkpoint is too large")
	// ErrQuerySubscriptionCheckpointSequence reports a non-monotone resume or
	// acknowledgement boundary.
	ErrQuerySubscriptionCheckpointSequence = errors.New("query subscription checkpoint sequence is invalid")
	// ErrQuerySubscriptionCheckpointTerminal reports a completed or canceled
	// checkpoint that cannot be resumed.
	ErrQuerySubscriptionCheckpointTerminal = errors.New("query subscription checkpoint is terminal")
)

// QuerySubscriptionCheckpoint is the durable consumer boundary for a
// HistoricalQuerySubscription. Result is the last acknowledged full result;
// retaining it lets the next refresh produce a delta-free full snapshot
// without replaying the already acknowledged frontier.
type QuerySubscriptionCheckpoint struct {
	ID       uint64
	Revision uint64
	Frontier uint64
	Complete bool
	Canceled bool
	Result   QueryResult
}

// QuerySubscriptionCheckpointCodecOptions bounds binary checkpoint encoding
// and decoding. Zero fields use the documented defaults.
type QuerySubscriptionCheckpointCodecOptions struct {
	MaxEncodedBytes int
	MaxRows         int
	MaxFieldsPerRow int
	MaxValueDepth   int
}

// HistoricalQuerySubscription adds explicit acknowledgement and durable
// cancellation to a normal query subscription. It is opt-in; ordinary
// QuerySubscription instances retain their existing behavior and cost.
type HistoricalQuerySubscription struct {
	subscription *QuerySubscription

	mu         sync.Mutex
	checkpoint QuerySubscriptionCheckpoint
}

// SubscribeHistorical evaluates a historical query once and creates an
// acknowledgement-aware subscription. The initial result is the baseline
// checkpoint; subsequent updates must be acknowledged before checkpointing.
func (registry *QuerySubscriptions) SubscribeHistorical(ctx context.Context, definition QuerySubscriptionDefinition, resolver SourceResolver, options QueryOptions) (*HistoricalQuerySubscription, error) {
	subscription, err := registry.Subscribe(ctx, definition, resolver, options)
	if err != nil {
		return nil, err
	}
	snapshot, ok := subscription.checkpointSnapshot()
	if !ok {
		subscription.Close()
		return nil, ErrQuerySubscriptionCheckpointInvalid
	}
	return &HistoricalQuerySubscription{
		subscription: subscription,
		checkpoint:   querySubscriptionCheckpointFromSnapshot(snapshot),
	}, nil
}

// ResumeHistorical restores an acknowledged historical subscription without
// rereading its source. The caller supplies the resolver on later
// NotifyChangedAt calls, so restart can proceed even while a source is down.
func (registry *QuerySubscriptions) ResumeHistorical(definition QuerySubscriptionDefinition, checkpoint QuerySubscriptionCheckpoint) (*HistoricalQuerySubscription, error) {
	if registry == nil {
		return nil, ErrQuerySubscriptionCheckpointInvalid
	}
	normalized, err := normalizeQuerySubscriptionDefinition(definition)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrQuerySubscriptionCheckpointInvalid, err)
	}
	if err := validateQuerySubscriptionCheckpoint(normalized, checkpoint); err != nil {
		return nil, err
	}
	if checkpoint.Canceled || checkpoint.Complete {
		return nil, ErrQuerySubscriptionCheckpointTerminal
	}
	subscription, err := registry.registerHistoricalSnapshot(normalized, checkpoint)
	if err != nil {
		return nil, err
	}
	return &HistoricalQuerySubscription{
		subscription: subscription,
		checkpoint:   cloneQuerySubscriptionCheckpoint(checkpoint),
	}, nil
}

// ResumeHistoricalBinary decodes and resumes one binary checkpoint.
func (registry *QuerySubscriptions) ResumeHistoricalBinary(definition QuerySubscriptionDefinition, encoded []byte) (*HistoricalQuerySubscription, error) {
	checkpoint, err := DecodeQuerySubscriptionCheckpoint(encoded)
	if err != nil {
		return nil, err
	}
	return registry.ResumeHistorical(definition, checkpoint)
}

// Updates returns the underlying bounded snapshot channel.
func (subscription *HistoricalQuerySubscription) Updates() <-chan QuerySubscriptionSnapshot {
	if subscription == nil || subscription.subscription == nil {
		return nil
	}
	return subscription.subscription.Updates()
}

// Snapshot returns the current live snapshot. A completed or canceled
// subscription returns false, while its last acknowledged checkpoint remains
// available through Checkpoint.
func (subscription *HistoricalQuerySubscription) Snapshot() (QuerySubscriptionSnapshot, bool) {
	if subscription == nil || subscription.subscription == nil {
		return QuerySubscriptionSnapshot{}, false
	}
	return subscription.subscription.Snapshot()
}

// Ack records a snapshot after its downstream side effect is durable. A
// progress-only snapshot retains the previously acknowledged result.
func (subscription *HistoricalQuerySubscription) Ack(snapshot QuerySubscriptionSnapshot) error {
	if subscription == nil || subscription.subscription == nil {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.checkpoint.Canceled {
		return ErrQuerySubscriptionCheckpointTerminal
	}
	if subscription.checkpoint.Complete {
		if snapshot.Complete && snapshot.ID == subscription.checkpoint.ID &&
			snapshot.Revision == subscription.checkpoint.Revision && snapshot.Frontier == subscription.checkpoint.Frontier {
			return nil
		}
		return ErrQuerySubscriptionCheckpointTerminal
	}
	current, ok := subscription.subscription.checkpointSnapshot()
	if !ok {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	if snapshot.ID == 0 || snapshot.ID != subscription.checkpoint.ID || snapshot.ID != current.ID {
		return fmt.Errorf("%w: snapshot ID", ErrQuerySubscriptionCheckpointInvalid)
	}
	if snapshot.Revision != current.Revision || snapshot.Frontier != current.Frontier {
		return fmt.Errorf("%w: snapshot is not the current subscription boundary", ErrQuerySubscriptionCheckpointSequence)
	}
	if snapshot.Complete != current.Complete {
		return fmt.Errorf("%w: snapshot completion state differs", ErrQuerySubscriptionCheckpointSequence)
	}
	if !snapshot.Progress && !sameQuerySubscriptionResult(snapshot.Result, current.Result) {
		return fmt.Errorf("%w: snapshot result differs", ErrQuerySubscriptionCheckpointSequence)
	}
	if snapshot.Revision < subscription.checkpoint.Revision ||
		(snapshot.Revision == subscription.checkpoint.Revision && snapshot.Frontier < subscription.checkpoint.Frontier) {
		return fmt.Errorf("%w: acknowledgement moved backwards", ErrQuerySubscriptionCheckpointSequence)
	}
	next := subscription.checkpoint
	next.Revision = snapshot.Revision
	next.Frontier = snapshot.Frontier
	next.Complete = snapshot.Complete
	if !snapshot.Progress {
		next.Result = cloneQuerySubscriptionCheckpointResult(snapshot.Result)
	}
	subscription.checkpoint = next
	return nil
}

// Checkpoint returns a detached copy of the last acknowledged boundary.
func (subscription *HistoricalQuerySubscription) Checkpoint() QuerySubscriptionCheckpoint {
	if subscription == nil {
		return QuerySubscriptionCheckpoint{}
	}
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	return cloneQuerySubscriptionCheckpoint(subscription.checkpoint)
}

// Cancel marks the last acknowledged boundary terminal and then closes the
// live subscription. Repeated calls return the same durable cancellation.
func (subscription *HistoricalQuerySubscription) Cancel() (QuerySubscriptionCheckpoint, error) {
	if subscription == nil || subscription.subscription == nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointInvalid
	}
	subscription.mu.Lock()
	checkpoint := cloneQuerySubscriptionCheckpoint(subscription.checkpoint)
	if !checkpoint.Complete {
		checkpoint.Canceled = true
		subscription.checkpoint = checkpoint
	}
	subscription.mu.Unlock()
	subscription.subscription.Close()
	return checkpoint, nil
}

// Close removes the live subscription without changing its durable
// checkpoint. Use Cancel when a restart must not resume it.
func (subscription *HistoricalQuerySubscription) Close() {
	if subscription == nil || subscription.subscription == nil {
		return
	}
	subscription.subscription.Close()
}

// MarshalBinary encodes a deterministic, CRC32C-protected HQS1 checkpoint.
func (checkpoint QuerySubscriptionCheckpoint) MarshalBinary() ([]byte, error) {
	return EncodeQuerySubscriptionCheckpoint(checkpoint)
}

// UnmarshalBinary validates and decodes one complete HQS1 checkpoint.
func (checkpoint *QuerySubscriptionCheckpoint) UnmarshalBinary(encoded []byte) error {
	if checkpoint == nil {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	decoded, err := DecodeQuerySubscriptionCheckpoint(encoded)
	if err != nil {
		return err
	}
	*checkpoint = decoded
	return nil
}

// EncodeQuerySubscriptionCheckpoint encodes a checkpoint with default bounds.
func EncodeQuerySubscriptionCheckpoint(checkpoint QuerySubscriptionCheckpoint) ([]byte, error) {
	return EncodeQuerySubscriptionCheckpointWithOptions(checkpoint, QuerySubscriptionCheckpointCodecOptions{})
}

// EncodeQuerySubscriptionCheckpointWithOptions encodes deterministic HQS1
// bytes. Map fields inside rows use the existing canonical typed row codec.
func EncodeQuerySubscriptionCheckpointWithOptions(checkpoint QuerySubscriptionCheckpoint, options QuerySubscriptionCheckpointCodecOptions) ([]byte, error) {
	normalized, err := normalizeQuerySubscriptionCheckpointCodecOptions(options)
	if err != nil {
		return nil, err
	}
	if err := validateQuerySubscriptionCheckpointIdentity(checkpoint); err != nil {
		return nil, err
	}
	if len(checkpoint.Result.Columns) > normalized.MaxFieldsPerRow || len(checkpoint.Result.Rows) > normalized.MaxRows {
		return nil, ErrQuerySubscriptionCheckpointTooLarge
	}
	rowOptions := DifferentialCheckpointCodecOptions{
		MaxEncodedBytes: normalized.MaxEncodedBytes,
		MaxRows:         normalized.MaxRows,
		MaxFieldsPerRow: normalized.MaxFieldsPerRow,
		MaxValueDepth:   normalized.MaxValueDepth,
	}
	rowData := make([]byte, 0, len(checkpoint.Result.Rows)*32)
	for index, row := range checkpoint.Result.Rows {
		rowData, err = appendDifferentialCheckpointRow(rowData, row, rowOptions)
		if err != nil {
			return nil, fmt.Errorf("%w: row %d: %v", ErrQuerySubscriptionCheckpointInvalid, index, err)
		}
		if len(rowData)+len(checkpoint.Result.Columns)*16+64 > normalized.MaxEncodedBytes {
			return nil, ErrQuerySubscriptionCheckpointTooLarge
		}
	}

	flags := byte(0)
	if checkpoint.Complete {
		flags |= querySubscriptionCheckpointComplete
	}
	if checkpoint.Canceled {
		flags |= querySubscriptionCheckpointCanceled
	}
	if checkpoint.Result.HasMore {
		flags |= querySubscriptionCheckpointHasMore
	}
	encoded := make([]byte, 0, len(rowData)+len(checkpoint.Result.Columns)*16+64)
	encoded = append(encoded, querySubscriptionCheckpointMagic...)
	encoded = append(encoded, querySubscriptionCheckpointVersion, flags)
	encoded = appendDifferentialCheckpointUvarint(encoded, checkpoint.ID)
	encoded = appendDifferentialCheckpointUvarint(encoded, checkpoint.Revision)
	encoded = appendDifferentialCheckpointUvarint(encoded, checkpoint.Frontier)
	encoded = appendDifferentialCheckpointUvarint(encoded, uint64(len(checkpoint.Result.Columns)))
	for _, column := range checkpoint.Result.Columns {
		encoded = appendDifferentialCheckpointString(encoded, column)
	}
	encoded = appendDifferentialCheckpointString(encoded, checkpoint.Result.QueryID)
	encoded = appendDifferentialCheckpointString(encoded, checkpoint.Result.NextCursor)
	encoded = appendDifferentialCheckpointUvarint(encoded, uint64(len(checkpoint.Result.Rows)))
	encoded = append(encoded, rowData...)
	if len(encoded)+4 > normalized.MaxEncodedBytes {
		return nil, ErrQuerySubscriptionCheckpointTooLarge
	}
	checksum := crc32.Checksum(encoded, differentialCheckpointCRCTable)
	var checksumBytes [4]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	return append(encoded, checksumBytes[:]...), nil
}

// DecodeQuerySubscriptionCheckpoint validates and decodes bounded HQS1 bytes.
func DecodeQuerySubscriptionCheckpoint(encoded []byte) (QuerySubscriptionCheckpoint, error) {
	return DecodeQuerySubscriptionCheckpointWithOptions(encoded, QuerySubscriptionCheckpointCodecOptions{})
}

// DecodeQuerySubscriptionCheckpointWithOptions decodes a bounded HQS1 payload
// atomically; malformed input never returns a partial checkpoint.
func DecodeQuerySubscriptionCheckpointWithOptions(encoded []byte, options QuerySubscriptionCheckpointCodecOptions) (QuerySubscriptionCheckpoint, error) {
	normalized, err := normalizeQuerySubscriptionCheckpointCodecOptions(options)
	if err != nil {
		return QuerySubscriptionCheckpoint{}, err
	}
	if len(encoded) > normalized.MaxEncodedBytes {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointTooLarge
	}
	if len(encoded) < len(querySubscriptionCheckpointMagic)+2+4 {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	body := encoded[:len(encoded)-4]
	if string(body[:len(querySubscriptionCheckpointMagic)]) != querySubscriptionCheckpointMagic ||
		body[len(querySubscriptionCheckpointMagic)] != querySubscriptionCheckpointVersion {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	if crc32.Checksum(body, differentialCheckpointCRCTable) != binary.LittleEndian.Uint32(encoded[len(encoded)-4:]) {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}

	rowOptions := DifferentialCheckpointCodecOptions{
		MaxEncodedBytes: normalized.MaxEncodedBytes,
		MaxRows:         normalized.MaxRows,
		MaxFieldsPerRow: normalized.MaxFieldsPerRow,
		MaxValueDepth:   normalized.MaxValueDepth,
	}
	reader := differentialCheckpointReader{
		data:    body,
		offset:  len(querySubscriptionCheckpointMagic) + 1,
		options: rowOptions,
	}
	flags, err := reader.readByte()
	if err != nil || flags&^(querySubscriptionCheckpointComplete|querySubscriptionCheckpointCanceled|querySubscriptionCheckpointHasMore) != 0 {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	id, err := reader.readUvarint()
	if err != nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	revision, err := reader.readUvarint()
	if err != nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	frontier, err := reader.readUvarint()
	if err != nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	columnCount, err := reader.readUvarint()
	if err != nil || columnCount > uint64(normalized.MaxFieldsPerRow) {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointTooLarge
	}
	columns := make([]string, int(columnCount))
	for index := range columns {
		columns[index], err = reader.readString()
		if err != nil {
			return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
		}
	}
	queryID, err := reader.readString()
	if err != nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	nextCursor, err := reader.readString()
	if err != nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	rowCount, err := reader.readUvarint()
	if err != nil || rowCount > uint64(normalized.MaxRows) {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointTooLarge
	}
	rows := make([]Row, int(rowCount))
	for index := range rows {
		rows[index], err = reader.readRowValue()
		if err != nil {
			return QuerySubscriptionCheckpoint{}, fmt.Errorf("%w: row %d: %v", ErrQuerySubscriptionCheckpointCorrupt, index, err)
		}
	}
	if reader.offset != len(body) {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointCorrupt
	}
	checkpoint := QuerySubscriptionCheckpoint{
		ID:       id,
		Revision: revision,
		Frontier: frontier,
		Complete: flags&querySubscriptionCheckpointComplete != 0,
		Canceled: flags&querySubscriptionCheckpointCanceled != 0,
		Result: QueryResult{
			QueryID:    queryID,
			Columns:    columns,
			Rows:       rows,
			HasMore:    flags&querySubscriptionCheckpointHasMore != 0,
			NextCursor: nextCursor,
		},
	}
	if err := validateQuerySubscriptionCheckpointIdentity(checkpoint); err != nil {
		return QuerySubscriptionCheckpoint{}, err
	}
	return checkpoint, nil
}

func normalizeQuerySubscriptionCheckpointCodecOptions(options QuerySubscriptionCheckpointCodecOptions) (QuerySubscriptionCheckpointCodecOptions, error) {
	if options.MaxEncodedBytes < 0 || options.MaxRows < 0 || options.MaxFieldsPerRow < 0 || options.MaxValueDepth < 0 {
		return QuerySubscriptionCheckpointCodecOptions{}, ErrQuerySubscriptionCheckpointInvalid
	}
	if options.MaxEncodedBytes == 0 {
		options.MaxEncodedBytes = DefaultQuerySubscriptionCheckpointMaxEncodedBytes
	}
	if options.MaxRows == 0 {
		options.MaxRows = DefaultQuerySubscriptionCheckpointMaxRows
	}
	if options.MaxFieldsPerRow == 0 {
		options.MaxFieldsPerRow = DefaultQuerySubscriptionCheckpointMaxFieldsPerRow
	}
	if options.MaxValueDepth == 0 {
		options.MaxValueDepth = DefaultQuerySubscriptionCheckpointMaxValueDepth
	}
	return options, nil
}

func validateQuerySubscriptionCheckpointIdentity(checkpoint QuerySubscriptionCheckpoint) error {
	if checkpoint.ID == 0 {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	if checkpoint.Complete && !checkpoint.Canceled && checkpoint.Frontier == 0 {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	return nil
}

func validateQuerySubscriptionCheckpoint(definition QuerySubscriptionDefinition, checkpoint QuerySubscriptionCheckpoint) error {
	if err := validateQuerySubscriptionCheckpointIdentity(checkpoint); err != nil {
		return err
	}
	if checkpoint.Revision == 0 && !definition.StartLive {
		return fmt.Errorf("%w: zero revision", ErrQuerySubscriptionCheckpointSequence)
	}
	if checkpoint.Frontier < definition.AsOf {
		return fmt.Errorf("%w: frontier before AS OF", ErrQuerySubscriptionCheckpointSequence)
	}
	if definition.UpTo > 0 && checkpoint.Frontier > definition.UpTo {
		return fmt.Errorf("%w: frontier after UP TO", ErrQuerySubscriptionCheckpointSequence)
	}
	if checkpoint.Complete && (definition.UpTo == 0 || checkpoint.Frontier != definition.UpTo) {
		return fmt.Errorf("%w: invalid completion frontier", ErrQuerySubscriptionCheckpointSequence)
	}
	return nil
}

func (registry *QuerySubscriptions) registerHistoricalSnapshot(definition QuerySubscriptionDefinition, checkpoint QuerySubscriptionCheckpoint) (*QuerySubscription, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.subs == nil {
		registry.subs = make(map[uint64]*QuerySubscription)
	}
	if _, exists := registry.subs[checkpoint.ID]; exists {
		return nil, fmt.Errorf("%w: subscription ID %d is already active", ErrQuerySubscriptionCheckpointSequence, checkpoint.ID)
	}
	if checkpoint.ID > registry.nextID {
		registry.nextID = checkpoint.ID
	}
	subscription := &QuerySubscription{
		registry:   registry,
		id:         checkpoint.ID,
		definition: definition,
		snapshot: QuerySubscriptionSnapshot{
			ID:       checkpoint.ID,
			Revision: checkpoint.Revision,
			Frontier: checkpoint.Frontier,
			Complete: checkpoint.Complete,
			Result:   cloneQuerySubscriptionCheckpointResult(checkpoint.Result),
		},
		updates: make(chan QuerySubscriptionSnapshot, registry.buffer),
	}
	registry.subs[subscription.id] = subscription
	return subscription, nil
}

func (subscription *QuerySubscription) checkpointSnapshot() (QuerySubscriptionSnapshot, bool) {
	if subscription == nil {
		return QuerySubscriptionSnapshot{}, false
	}
	subscription.mu.RLock()
	defer subscription.mu.RUnlock()
	if subscription.snapshot.ID == 0 {
		return QuerySubscriptionSnapshot{}, false
	}
	return cloneQuerySubscriptionSnapshot(subscription.snapshot), true
}

func querySubscriptionCheckpointFromSnapshot(snapshot QuerySubscriptionSnapshot) QuerySubscriptionCheckpoint {
	return QuerySubscriptionCheckpoint{
		ID:       snapshot.ID,
		Revision: snapshot.Revision,
		Frontier: snapshot.Frontier,
		Complete: snapshot.Complete,
		Result:   cloneQuerySubscriptionCheckpointResult(snapshot.Result),
	}
}

func cloneQuerySubscriptionCheckpoint(checkpoint QuerySubscriptionCheckpoint) QuerySubscriptionCheckpoint {
	checkpoint.Result = cloneQuerySubscriptionCheckpointResult(checkpoint.Result)
	return checkpoint
}

func cloneQuerySubscriptionCheckpointResult(result QueryResult) QueryResult {
	cloned := cloneQueryResult(result)
	cloned.Plan = nil
	cloned.PlanSnapshot = nil
	cloned.Stats = nil
	return cloned
}
