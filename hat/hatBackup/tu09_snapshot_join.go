package hatBackup

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"unicode/utf8"
)

const (
	SnapshotJoinProtocolVersion uint16 = 1

	DefaultSnapshotJoinMaxBatchRecords = 1024
	MaxSnapshotJoinBatchRecords        = 65536
	DefaultSnapshotJoinMaxRecordBytes  = 16 << 20
	MaxSnapshotJoinRecordBytes         = 64 << 20
	MaxSnapshotJoinCheckpointBytes     = 4096
	MaxSnapshotJoinComponentBytes      = 256
	MaxSnapshotJoinFailureBytes        = 1024
)

var (
	ErrSnapshotJoinNil               = errors.New("hatriecache: snapshot join bootstrap is nil")
	ErrSnapshotJoinManifestInvalid   = errors.New("hatriecache: snapshot join manifest is invalid")
	ErrSnapshotJoinOptionsInvalid    = errors.New("hatriecache: snapshot join options are invalid")
	ErrSnapshotJoinCallbackRequired  = errors.New("hatriecache: snapshot join callback is required")
	ErrSnapshotJoinContextRequired   = errors.New("hatriecache: snapshot join context is required")
	ErrSnapshotJoinPhase             = errors.New("hatriecache: snapshot join phase does not allow this operation")
	ErrSnapshotJoinChecksum          = errors.New("hatriecache: snapshot join checksum does not match")
	ErrSnapshotJoinSequenceGap       = errors.New("hatriecache: snapshot join WAL sequence has a gap")
	ErrSnapshotJoinSequenceRange     = errors.New("hatriecache: snapshot join WAL sequence is outside the manifest range")
	ErrSnapshotJoinFence             = errors.New("hatriecache: snapshot join fencing token is stale")
	ErrSnapshotJoinFailed            = errors.New("hatriecache: snapshot join bootstrap failed")
	ErrSnapshotJoinCheckpointCorrupt = errors.New("hatriecache: snapshot join checkpoint is corrupt")
	ErrSnapshotJoinCheckpointInvalid = errors.New("hatriecache: snapshot join checkpoint is invalid")
)

// SnapshotJoinPhase is the durable lifecycle state of a joining replica.
type SnapshotJoinPhase uint8

const (
	SnapshotJoinPhasePending SnapshotJoinPhase = iota + 1
	SnapshotJoinPhaseSnapshotApplying
	SnapshotJoinPhaseSnapshotApplied
	SnapshotJoinPhaseWALApplying
	SnapshotJoinPhaseReady
	SnapshotJoinPhaseActivating
	SnapshotJoinPhaseActive
	SnapshotJoinPhaseFailed
)

func (phase SnapshotJoinPhase) String() string {
	switch phase {
	case SnapshotJoinPhasePending:
		return "pending"
	case SnapshotJoinPhaseSnapshotApplying:
		return "snapshot_applying"
	case SnapshotJoinPhaseSnapshotApplied:
		return "snapshot_applied"
	case SnapshotJoinPhaseWALApplying:
		return "wal_applying"
	case SnapshotJoinPhaseReady:
		return "ready"
	case SnapshotJoinPhaseActivating:
		return "activating"
	case SnapshotJoinPhaseActive:
		return "active"
	case SnapshotJoinPhaseFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// SnapshotJoinManifest binds a snapshot and its following WAL interval to a
// source, joiner, and fencing generation. WALLastSequence is inclusive.
type SnapshotJoinManifest struct {
	ProtocolVersion  uint16
	SourceID         string
	JoinerID         string
	FencingToken     uint64
	SnapshotSequence uint64
	WALLastSequence  uint64
	SnapshotChecksum [32]byte
}

// SnapshotJoinRecord is one immutable-after-validation WAL record supplied to
// the caller's atomic apply callback.
type SnapshotJoinRecord struct {
	Sequence uint64
	Payload  []byte
}

// SnapshotJoinOptions bounds one bootstrap coordinator. Zero values select
// conservative defaults and do not enable any background work.
type SnapshotJoinOptions struct {
	MaxBatchRecords int
	MaxRecordBytes  int
}

// SnapshotJoinStatus is a point-in-time progress report.
type SnapshotJoinStatus struct {
	Phase           SnapshotJoinPhase
	SourceID        string
	JoinerID        string
	FencingToken    uint64
	AppliedSequence uint64
	WALRemaining    uint64
	Failure         string
}

// SnapshotJoinCheckpoint is the stable state needed to resume a join. It is
// safe to persist only after SnapshotJoinBootstrap.Checkpoint returns it.
type SnapshotJoinCheckpoint struct {
	Manifest        SnapshotJoinManifest
	Phase           SnapshotJoinPhase
	AppliedSequence uint64
	Failure         string
}

// SnapshotJoinBootstrap coordinates a snapshot, contiguous WAL catch-up, and
// fenced activation. The data transfer and durable writes remain caller-owned.
type SnapshotJoinBootstrap struct {
	mu             sync.Mutex
	manifest       SnapshotJoinManifest
	maxBatch       int
	maxRecordBytes int
	phase          SnapshotJoinPhase
	applied        uint64
	failure        error
}

// NewSnapshotJoinBootstrap creates an idle, opt-in join coordinator.
func NewSnapshotJoinBootstrap(manifest SnapshotJoinManifest, options SnapshotJoinOptions) (*SnapshotJoinBootstrap, error) {
	if err := validateSnapshotJoinManifest(manifest); err != nil {
		return nil, err
	}
	maxBatch := options.MaxBatchRecords
	if maxBatch == 0 {
		maxBatch = DefaultSnapshotJoinMaxBatchRecords
	}
	if maxBatch < 1 || maxBatch > MaxSnapshotJoinBatchRecords {
		return nil, fmt.Errorf("%w: max batch records %d", ErrSnapshotJoinOptionsInvalid, maxBatch)
	}
	maxRecordBytes := options.MaxRecordBytes
	if maxRecordBytes == 0 {
		maxRecordBytes = DefaultSnapshotJoinMaxRecordBytes
	}
	if maxRecordBytes < 1 || maxRecordBytes > MaxSnapshotJoinRecordBytes {
		return nil, fmt.Errorf("%w: max record bytes %d", ErrSnapshotJoinOptionsInvalid, maxRecordBytes)
	}
	return &SnapshotJoinBootstrap{
		manifest:       manifest,
		maxBatch:       maxBatch,
		maxRecordBytes: maxRecordBytes,
		phase:          SnapshotJoinPhasePending,
	}, nil
}

// ApplySnapshot validates the transferred snapshot and invokes apply exactly
// once. A failed callback makes the coordinator terminally failed because the
// caller may have partially written its destination.
func (bootstrap *SnapshotJoinBootstrap) ApplySnapshot(ctx context.Context, checksum [32]byte, apply func() error) error {
	if bootstrap == nil {
		return ErrSnapshotJoinNil
	}
	if apply == nil {
		return ErrSnapshotJoinCallbackRequired
	}
	if err := snapshotJoinContextError(ctx); err != nil {
		return err
	}

	bootstrap.mu.Lock()
	if bootstrap.phase == SnapshotJoinPhaseFailed {
		err := bootstrap.failedErrorLocked()
		bootstrap.mu.Unlock()
		return err
	}
	if bootstrap.phase != SnapshotJoinPhasePending {
		err := fmt.Errorf("%w: apply snapshot in %s", ErrSnapshotJoinPhase, bootstrap.phase)
		bootstrap.mu.Unlock()
		return err
	}
	if checksum != bootstrap.manifest.SnapshotChecksum {
		bootstrap.mu.Unlock()
		return ErrSnapshotJoinChecksum
	}
	bootstrap.phase = SnapshotJoinPhaseSnapshotApplying
	bootstrap.mu.Unlock()

	err := apply()
	if err == nil {
		err = ctx.Err()
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if err != nil {
		bootstrap.markFailedLocked(err)
		return err
	}
	bootstrap.applied = bootstrap.manifest.SnapshotSequence
	bootstrap.phase = SnapshotJoinPhaseSnapshotApplied
	if bootstrap.applied == bootstrap.manifest.WALLastSequence {
		bootstrap.phase = SnapshotJoinPhaseReady
	}
	return nil
}

// ApplyWAL validates and atomically hands one contiguous WAL batch to apply.
// The callback receives owned record and payload copies. It must make the
// destination changes atomically with respect to the batch.
func (bootstrap *SnapshotJoinBootstrap) ApplyWAL(ctx context.Context, records []SnapshotJoinRecord, apply func([]SnapshotJoinRecord) error) error {
	if bootstrap == nil {
		return ErrSnapshotJoinNil
	}
	if apply == nil {
		return ErrSnapshotJoinCallbackRequired
	}
	if err := snapshotJoinContextError(ctx); err != nil {
		return err
	}

	bootstrap.mu.Lock()
	if bootstrap.phase == SnapshotJoinPhaseFailed {
		err := bootstrap.failedErrorLocked()
		bootstrap.mu.Unlock()
		return err
	}
	if bootstrap.phase != SnapshotJoinPhaseSnapshotApplied {
		err := fmt.Errorf("%w: apply WAL in %s", ErrSnapshotJoinPhase, bootstrap.phase)
		bootstrap.mu.Unlock()
		return err
	}
	if len(records) == 0 || len(records) > bootstrap.maxBatch {
		err := fmt.Errorf("%w: batch has %d records, maximum is %d", ErrSnapshotJoinSequenceRange, len(records), bootstrap.maxBatch)
		bootstrap.mu.Unlock()
		return err
	}
	expected := bootstrap.applied + 1
	batch := make([]SnapshotJoinRecord, len(records))
	for index, record := range records {
		if record.Sequence != expected {
			err := fmt.Errorf("%w: expected %d at index %d, got %d", ErrSnapshotJoinSequenceGap, expected, index, record.Sequence)
			bootstrap.mu.Unlock()
			return err
		}
		if record.Sequence > bootstrap.manifest.WALLastSequence {
			err := fmt.Errorf("%w: sequence %d exceeds manifest end %d", ErrSnapshotJoinSequenceRange, record.Sequence, bootstrap.manifest.WALLastSequence)
			bootstrap.mu.Unlock()
			return err
		}
		if len(record.Payload) > bootstrap.maxRecordBytes {
			err := fmt.Errorf("%w: record %d has %d bytes, maximum is %d", ErrSnapshotJoinOptionsInvalid, record.Sequence, len(record.Payload), bootstrap.maxRecordBytes)
			bootstrap.mu.Unlock()
			return err
		}
		batch[index] = SnapshotJoinRecord{
			Sequence: record.Sequence,
			Payload:  append([]byte(nil), record.Payload...),
		}
		expected++
	}
	bootstrap.phase = SnapshotJoinPhaseWALApplying
	bootstrap.mu.Unlock()

	err := apply(batch)
	if err == nil {
		err = ctx.Err()
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if err != nil {
		bootstrap.markFailedLocked(err)
		return err
	}
	bootstrap.applied = batch[len(batch)-1].Sequence
	bootstrap.phase = SnapshotJoinPhaseSnapshotApplied
	if bootstrap.applied == bootstrap.manifest.WALLastSequence {
		bootstrap.phase = SnapshotJoinPhaseReady
	}
	return nil
}

// Activate fences the joiner generation and invokes the caller's atomic
// publication callback only after the complete manifest WAL interval is
// applied.
func (bootstrap *SnapshotJoinBootstrap) Activate(ctx context.Context, fencingToken uint64, activate func() error) error {
	if bootstrap == nil {
		return ErrSnapshotJoinNil
	}
	if activate == nil {
		return ErrSnapshotJoinCallbackRequired
	}
	if err := snapshotJoinContextError(ctx); err != nil {
		return err
	}

	bootstrap.mu.Lock()
	if bootstrap.phase == SnapshotJoinPhaseFailed {
		err := bootstrap.failedErrorLocked()
		bootstrap.mu.Unlock()
		return err
	}
	if bootstrap.phase != SnapshotJoinPhaseReady {
		err := fmt.Errorf("%w: activate in %s", ErrSnapshotJoinPhase, bootstrap.phase)
		bootstrap.mu.Unlock()
		return err
	}
	if fencingToken != bootstrap.manifest.FencingToken {
		bootstrap.mu.Unlock()
		return ErrSnapshotJoinFence
	}
	bootstrap.phase = SnapshotJoinPhaseActivating
	bootstrap.mu.Unlock()

	err := activate()
	if err == nil {
		err = ctx.Err()
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if err != nil {
		bootstrap.markFailedLocked(err)
		return err
	}
	bootstrap.phase = SnapshotJoinPhaseActive
	return nil
}

// Abort makes a non-active join terminally failed and records a bounded
// diagnostic string for operators.
func (bootstrap *SnapshotJoinBootstrap) Abort(err error) error {
	if bootstrap == nil {
		return ErrSnapshotJoinNil
	}
	if err == nil {
		return ErrSnapshotJoinFailed
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if bootstrap.phase == SnapshotJoinPhaseActive {
		return fmt.Errorf("%w: active join cannot be aborted", ErrSnapshotJoinPhase)
	}
	if bootstrap.phase == SnapshotJoinPhaseFailed {
		return bootstrap.failedErrorLocked()
	}
	bootstrap.markFailedLocked(err)
	return err
}

// Status returns an immutable progress snapshot.
func (bootstrap *SnapshotJoinBootstrap) Status() SnapshotJoinStatus {
	if bootstrap == nil {
		return SnapshotJoinStatus{Phase: SnapshotJoinPhaseFailed, Failure: ErrSnapshotJoinNil.Error()}
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	status := SnapshotJoinStatus{
		Phase:           bootstrap.phase,
		SourceID:        bootstrap.manifest.SourceID,
		JoinerID:        bootstrap.manifest.JoinerID,
		FencingToken:    bootstrap.manifest.FencingToken,
		AppliedSequence: bootstrap.applied,
	}
	if bootstrap.applied < bootstrap.manifest.WALLastSequence {
		status.WALRemaining = bootstrap.manifest.WALLastSequence - bootstrap.applied
	}
	if bootstrap.failure != nil {
		status.Failure = bootstrap.failure.Error()
	}
	return status
}

// Checkpoint returns the current state without retaining mutable buffers.
func (bootstrap *SnapshotJoinBootstrap) Checkpoint() SnapshotJoinCheckpoint {
	if bootstrap == nil {
		return SnapshotJoinCheckpoint{Phase: SnapshotJoinPhaseFailed, Failure: ErrSnapshotJoinNil.Error()}
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	checkpoint := SnapshotJoinCheckpoint{
		Manifest:        bootstrap.manifest,
		Phase:           bootstrap.phase,
		AppliedSequence: bootstrap.applied,
	}
	if bootstrap.failure != nil {
		checkpoint.Failure = bootstrap.failure.Error()
	}
	return checkpoint
}

// RestoreCheckpoint restores a stable checkpoint into a fresh coordinator.
func (bootstrap *SnapshotJoinBootstrap) RestoreCheckpoint(checkpoint SnapshotJoinCheckpoint) error {
	if bootstrap == nil {
		return ErrSnapshotJoinNil
	}
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	if bootstrap.phase == SnapshotJoinPhaseFailed {
		return bootstrap.failedErrorLocked()
	}
	if bootstrap.phase != SnapshotJoinPhasePending {
		return fmt.Errorf("%w: restore in %s", ErrSnapshotJoinPhase, bootstrap.phase)
	}
	if checkpoint.Manifest != bootstrap.manifest {
		return fmt.Errorf("%w: manifest does not match coordinator", ErrSnapshotJoinCheckpointInvalid)
	}
	if err := validateSnapshotJoinCheckpoint(checkpoint); err != nil {
		return err
	}
	bootstrap.phase = checkpoint.Phase
	bootstrap.applied = checkpoint.AppliedSequence
	if checkpoint.Failure != "" {
		bootstrap.failure = errors.New(checkpoint.Failure)
	}
	return nil
}

func (bootstrap *SnapshotJoinBootstrap) markFailedLocked(err error) {
	bootstrap.phase = SnapshotJoinPhaseFailed
	bootstrap.failure = boundedSnapshotJoinError(err)
}

func (bootstrap *SnapshotJoinBootstrap) failedErrorLocked() error {
	if bootstrap.failure == nil {
		return ErrSnapshotJoinFailed
	}
	return fmt.Errorf("%w: %v", ErrSnapshotJoinFailed, bootstrap.failure)
}

func snapshotJoinContextError(ctx context.Context) error {
	if ctx == nil {
		return ErrSnapshotJoinContextRequired
	}
	return ctx.Err()
}

func boundedSnapshotJoinError(err error) error {
	if err == nil {
		return ErrSnapshotJoinFailed
	}
	message := err.Error()
	if len(message) > MaxSnapshotJoinFailureBytes {
		message = message[:MaxSnapshotJoinFailureBytes]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return errors.New(message)
}

func validateSnapshotJoinManifest(manifest SnapshotJoinManifest) error {
	if manifest.ProtocolVersion != SnapshotJoinProtocolVersion {
		return fmt.Errorf("%w: protocol version %d", ErrSnapshotJoinManifestInvalid, manifest.ProtocolVersion)
	}
	if err := validateSnapshotJoinComponent(manifest.SourceID); err != nil {
		return fmt.Errorf("%w: source ID: %v", ErrSnapshotJoinManifestInvalid, err)
	}
	if err := validateSnapshotJoinComponent(manifest.JoinerID); err != nil {
		return fmt.Errorf("%w: joiner ID: %v", ErrSnapshotJoinManifestInvalid, err)
	}
	if manifest.FencingToken == 0 {
		return fmt.Errorf("%w: fencing token must be positive", ErrSnapshotJoinManifestInvalid)
	}
	if manifest.WALLastSequence < manifest.SnapshotSequence {
		return fmt.Errorf("%w: WAL end %d precedes snapshot %d", ErrSnapshotJoinManifestInvalid, manifest.WALLastSequence, manifest.SnapshotSequence)
	}
	return nil
}

func validateSnapshotJoinComponent(value string) error {
	if value == "" || len(value) > MaxSnapshotJoinComponentBytes || !utf8.ValidString(value) {
		return errors.New("component is empty, too long, or invalid UTF-8")
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return errors.New("component contains a control character")
		}
	}
	return nil
}

func validateSnapshotJoinCheckpoint(checkpoint SnapshotJoinCheckpoint) error {
	if err := validateSnapshotJoinManifest(checkpoint.Manifest); err != nil {
		return fmt.Errorf("%w: manifest: %v", ErrSnapshotJoinCheckpointInvalid, err)
	}
	if checkpoint.Failure != "" {
		if len(checkpoint.Failure) > MaxSnapshotJoinFailureBytes || !utf8.ValidString(checkpoint.Failure) {
			return fmt.Errorf("%w: failure is too long or invalid UTF-8", ErrSnapshotJoinCheckpointInvalid)
		}
	}
	snapshot := checkpoint.Manifest.SnapshotSequence
	last := checkpoint.Manifest.WALLastSequence
	switch checkpoint.Phase {
	case SnapshotJoinPhasePending:
		if checkpoint.AppliedSequence != 0 || checkpoint.Failure != "" {
			return fmt.Errorf("%w: pending checkpoint has progress or failure", ErrSnapshotJoinCheckpointInvalid)
		}
	case SnapshotJoinPhaseSnapshotApplied:
		if checkpoint.AppliedSequence < snapshot || checkpoint.AppliedSequence >= last || checkpoint.Failure != "" {
			return fmt.Errorf("%w: snapshot-applied checkpoint sequence is invalid", ErrSnapshotJoinCheckpointInvalid)
		}
	case SnapshotJoinPhaseReady, SnapshotJoinPhaseActive:
		if checkpoint.AppliedSequence != last || checkpoint.Failure != "" {
			return fmt.Errorf("%w: ready/active checkpoint sequence is invalid", ErrSnapshotJoinCheckpointInvalid)
		}
	case SnapshotJoinPhaseFailed:
		if checkpoint.AppliedSequence > last || checkpoint.Failure == "" {
			return fmt.Errorf("%w: failed checkpoint is invalid", ErrSnapshotJoinCheckpointInvalid)
		}
	default:
		return fmt.Errorf("%w: transient or unknown phase %d", ErrSnapshotJoinCheckpointInvalid, checkpoint.Phase)
	}
	return nil
}

var snapshotJoinCheckpointCRCTable = crc32.MakeTable(crc32.Castagnoli)

// MarshalBinary encodes a deterministic bounded SJC1 checkpoint.
func (checkpoint SnapshotJoinCheckpoint) MarshalBinary() ([]byte, error) {
	if err := validateSnapshotJoinCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	data := make([]byte, 0, 256)
	data = append(data, 'S', 'J', 'C', '1')
	data = appendUint16(data, SnapshotJoinProtocolVersion)
	data = append(data, byte(checkpoint.Phase), 0)
	data = appendUint16(data, checkpoint.Manifest.ProtocolVersion)
	data = appendSnapshotJoinString(data, checkpoint.Manifest.SourceID)
	data = appendSnapshotJoinString(data, checkpoint.Manifest.JoinerID)
	data = appendUint64(data, checkpoint.Manifest.FencingToken)
	data = appendUint64(data, checkpoint.Manifest.SnapshotSequence)
	data = appendUint64(data, checkpoint.Manifest.WALLastSequence)
	data = append(data, checkpoint.Manifest.SnapshotChecksum[:]...)
	data = appendUint64(data, checkpoint.AppliedSequence)
	data = appendSnapshotJoinString(data, checkpoint.Failure)
	checksum := crc32.Checksum(data, snapshotJoinCheckpointCRCTable)
	data = appendUint32(data, checksum)
	if len(data) > MaxSnapshotJoinCheckpointBytes {
		return nil, fmt.Errorf("%w: encoded size %d", ErrSnapshotJoinCheckpointInvalid, len(data))
	}
	return data, nil
}

// UnmarshalSnapshotJoinCheckpoint decodes and validates an SJC1 checkpoint.
func UnmarshalSnapshotJoinCheckpoint(data []byte) (SnapshotJoinCheckpoint, error) {
	if len(data) > MaxSnapshotJoinCheckpointBytes || len(data) < 4+2+2+2+2+8+8+8+32+8+2+4 {
		return SnapshotJoinCheckpoint{}, ErrSnapshotJoinCheckpointCorrupt
	}
	content := data[:len(data)-4]
	want := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(content, snapshotJoinCheckpointCRCTable) != want {
		return SnapshotJoinCheckpoint{}, ErrSnapshotJoinCheckpointCorrupt
	}
	reader := snapshotJoinCheckpointReader{data: content}
	if string(reader.bytes(4)) != "SJC1" {
		return SnapshotJoinCheckpoint{}, ErrSnapshotJoinCheckpointCorrupt
	}
	if reader.uint16() != SnapshotJoinProtocolVersion {
		return SnapshotJoinCheckpoint{}, ErrSnapshotJoinCheckpointCorrupt
	}
	phase := SnapshotJoinPhase(reader.byte())
	if reader.byte() != 0 {
		return SnapshotJoinCheckpoint{}, ErrSnapshotJoinCheckpointCorrupt
	}
	manifest := SnapshotJoinManifest{ProtocolVersion: reader.uint16()}
	manifest.SourceID = reader.string(MaxSnapshotJoinComponentBytes)
	manifest.JoinerID = reader.string(MaxSnapshotJoinComponentBytes)
	manifest.FencingToken = reader.uint64()
	manifest.SnapshotSequence = reader.uint64()
	manifest.WALLastSequence = reader.uint64()
	checksum := reader.bytes(32)
	if len(checksum) != 32 {
		return SnapshotJoinCheckpoint{}, ErrSnapshotJoinCheckpointCorrupt
	}
	copy(manifest.SnapshotChecksum[:], checksum)
	checkpoint := SnapshotJoinCheckpoint{
		Manifest:        manifest,
		Phase:           phase,
		AppliedSequence: reader.uint64(),
		Failure:         reader.string(MaxSnapshotJoinFailureBytes),
	}
	if reader.bad || reader.offset != len(content) {
		return SnapshotJoinCheckpoint{}, ErrSnapshotJoinCheckpointCorrupt
	}
	if err := validateSnapshotJoinCheckpoint(checkpoint); err != nil {
		return SnapshotJoinCheckpoint{}, fmt.Errorf("%w: %v", ErrSnapshotJoinCheckpointCorrupt, err)
	}
	return checkpoint, nil
}

type snapshotJoinCheckpointReader struct {
	data   []byte
	offset int
	bad    bool
}

func (reader *snapshotJoinCheckpointReader) bytes(size int) []byte {
	if size < 0 || reader.offset+size > len(reader.data) {
		reader.bad = true
		return nil
	}
	value := reader.data[reader.offset : reader.offset+size]
	reader.offset += size
	return value
}

func (reader *snapshotJoinCheckpointReader) byte() byte {
	value := reader.bytes(1)
	if len(value) != 1 {
		return 0
	}
	return value[0]
}

func (reader *snapshotJoinCheckpointReader) uint16() uint16 {
	value := reader.bytes(2)
	if len(value) != 2 {
		return 0
	}
	return binary.LittleEndian.Uint16(value)
}

func (reader *snapshotJoinCheckpointReader) uint64() uint64 {
	value := reader.bytes(8)
	if len(value) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(value)
}

func (reader *snapshotJoinCheckpointReader) string(maxBytes int) string {
	length := int(reader.uint16())
	if length > maxBytes {
		reader.bad = true
		return ""
	}
	value := reader.bytes(length)
	if reader.bad || !utf8.Valid(value) {
		reader.bad = true
		return ""
	}
	return string(value)
}

func appendUint16(data []byte, value uint16) []byte {
	var encoded [2]byte
	binary.LittleEndian.PutUint16(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendUint32(data []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendUint64(data []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendSnapshotJoinString(data []byte, value string) []byte {
	data = appendUint16(data, uint16(len(value)))
	return append(data, value...)
}
