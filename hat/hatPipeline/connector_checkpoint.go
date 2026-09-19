package hatPipeline

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	connectorCheckpointMagic         = "HCP1"
	connectorCheckpointHeaderBytes   = 4 + 2 + 8 + 8 + 4 + 4
	connectorCheckpointChecksumBytes = 4
	maxConnectorCheckpointIDBytes    = 256
	maxConnectorCheckpointFieldBytes = 512 << 10
	maxConnectorCheckpointBytes      = 1 << 20
)

var (
	// ErrConnectorCheckpointStoreRequired indicates that no checkpoint store
	// was supplied to a checkpoint operation.
	ErrConnectorCheckpointStoreRequired = errors.New("hatPipeline: connector checkpoint store is required")
	// ErrConnectorCheckpointNotFound indicates that a store has no checkpoint.
	ErrConnectorCheckpointNotFound = errors.New("hatPipeline: connector checkpoint was not found")
	// ErrConnectorCheckpointInvalid indicates a malformed or corrupt payload.
	ErrConnectorCheckpointInvalid = errors.New("hatPipeline: connector checkpoint is invalid")
	// ErrConnectorCheckpointPayloadTooLarge indicates a bounded field or
	// complete payload exceeded the checkpoint format limit.
	ErrConnectorCheckpointPayloadTooLarge = errors.New("hatPipeline: connector checkpoint payload is too large")
	// ErrConnectorCheckpointSequenceInvalid indicates a zero checkpoint
	// sequence, which cannot identify an operator-visible checkpoint.
	ErrConnectorCheckpointSequenceInvalid = errors.New("hatPipeline: connector checkpoint sequence is invalid")
	// ErrConnectorCheckpointGenerationInvalid indicates an invalid lifecycle
	// generation in an encoded checkpoint or checkpoint input.
	ErrConnectorCheckpointGenerationInvalid = errors.New("hatPipeline: connector checkpoint generation is invalid")
	// ErrConnectorCheckpointIDMismatch indicates that a checkpoint belongs to
	// another connector.
	ErrConnectorCheckpointIDMismatch = errors.New("hatPipeline: connector checkpoint ID does not match")
	// ErrConnectorCheckpointStateInvalid indicates that the connector is not
	// paused and therefore cannot be restored from a checkpoint.
	ErrConnectorCheckpointStateInvalid = errors.New("hatPipeline: connector is not paused for checkpoint restore")
	// ErrConnectorCheckpointStale indicates that a lifecycle transition changed
	// the connector generation after the checkpoint was written.
	ErrConnectorCheckpointStale = errors.New("hatPipeline: connector checkpoint is stale")
	// ErrConnectorCheckpointApplierRequired indicates that a restore callback
	// was not supplied to apply opaque source position data.
	ErrConnectorCheckpointApplierRequired = errors.New("hatPipeline: connector checkpoint applier is required")
)

var connectorCheckpointCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ConnectorCheckpointStore persists and loads one encoded connector
// checkpoint. FrontierSnapshotFileStore implements this interface and can be
// reused for local durable checkpoints.
type ConnectorCheckpointStore interface {
	Load(context.Context) ([]byte, error)
	Save(context.Context, []byte) error
}

// ConnectorCheckpoint contains a source position captured at one connector
// lifecycle generation. Offset and Frontier are opaque source-specific bytes;
// callers decode and apply them in ConnectorCheckpointApplier.
type ConnectorCheckpoint struct {
	ConnectorID string
	Sequence    uint64
	Generation  uint64
	Offset      []byte
	Frontier    []byte
}

// ConnectorCheckpointApplier restores source-specific offset and frontier
// state. It runs while the connector remains paused and before Resume is
// called. Implementations must not call lifecycle methods on the same
// registry from the callback.
type ConnectorCheckpointApplier func(context.Context, ConnectorCheckpoint) error

// EncodeConnectorCheckpoint returns a deterministic, bounded HCP1 payload.
func EncodeConnectorCheckpoint(checkpoint ConnectorCheckpoint) ([]byte, error) {
	if err := validateConnectorCheckpoint(checkpoint, true); err != nil {
		return nil, err
	}
	total := connectorCheckpointHeaderBytes + len(checkpoint.ConnectorID) + len(checkpoint.Frontier) + len(checkpoint.Offset) + connectorCheckpointChecksumBytes
	if total > maxConnectorCheckpointBytes {
		return nil, ErrConnectorCheckpointPayloadTooLarge
	}
	payload := make([]byte, total)
	copy(payload[:4], connectorCheckpointMagic)
	binary.BigEndian.PutUint16(payload[4:6], uint16(len(checkpoint.ConnectorID)))
	binary.BigEndian.PutUint64(payload[6:14], checkpoint.Sequence)
	binary.BigEndian.PutUint64(payload[14:22], checkpoint.Generation)
	binary.BigEndian.PutUint32(payload[22:26], uint32(len(checkpoint.Frontier)))
	binary.BigEndian.PutUint32(payload[26:30], uint32(len(checkpoint.Offset)))
	position := connectorCheckpointHeaderBytes
	position += copy(payload[position:], checkpoint.ConnectorID)
	position += copy(payload[position:], checkpoint.Frontier)
	position += copy(payload[position:], checkpoint.Offset)
	binary.BigEndian.PutUint32(payload[position:], crc32.Checksum(payload[:position], connectorCheckpointCRCTable))
	return payload, nil
}

// DecodeConnectorCheckpoint validates and decodes one HCP1 payload without
// allocating field buffers until its size, lengths, and checksum are valid.
func DecodeConnectorCheckpoint(payload []byte) (ConnectorCheckpoint, error) {
	if len(payload) > maxConnectorCheckpointBytes {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointPayloadTooLarge
	}
	minimum := connectorCheckpointHeaderBytes + connectorCheckpointChecksumBytes
	if len(payload) < minimum || string(payload[:4]) != connectorCheckpointMagic {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointInvalid
	}
	idLength := int(binary.BigEndian.Uint16(payload[4:6]))
	frontierLength := uint64(binary.BigEndian.Uint32(payload[22:26]))
	offsetLength := uint64(binary.BigEndian.Uint32(payload[26:30]))
	if idLength == 0 || idLength > maxConnectorCheckpointIDBytes || frontierLength > maxConnectorCheckpointFieldBytes || offsetLength > maxConnectorCheckpointFieldBytes {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointInvalid
	}
	bodyLength := uint64(connectorCheckpointHeaderBytes) + uint64(idLength) + frontierLength + offsetLength
	if bodyLength+connectorCheckpointChecksumBytes != uint64(len(payload)) {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointInvalid
	}
	if crc32.Checksum(payload[:bodyLength], connectorCheckpointCRCTable) != binary.BigEndian.Uint32(payload[bodyLength:]) {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointInvalid
	}
	checkpoint := ConnectorCheckpoint{
		Sequence:   binary.BigEndian.Uint64(payload[6:14]),
		Generation: binary.BigEndian.Uint64(payload[14:22]),
	}
	position := connectorCheckpointHeaderBytes
	checkpoint.ConnectorID = string(payload[position : position+idLength])
	position += idLength
	checkpoint.Frontier = append([]byte(nil), payload[position:position+int(frontierLength)]...)
	position += int(frontierLength)
	checkpoint.Offset = append([]byte(nil), payload[position:position+int(offsetLength)]...)
	if err := validateConnectorCheckpoint(checkpoint, true); err != nil {
		return ConnectorCheckpoint{}, err
	}
	return checkpoint, nil
}

// PauseWithCheckpoint pauses a running connector and durably stores its
// source position. The connector remains paused when storing fails, so an
// operator cannot accidentally resume without a durable checkpoint.
func (r *ConnectorRegistry) PauseWithCheckpoint(ctx context.Context, id string, checkpoint ConnectorCheckpoint, store ConnectorCheckpointStore) (ConnectorCheckpoint, error) {
	if store == nil {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointStoreRequired
	}
	if id == "" {
		return ConnectorCheckpoint{}, ErrConnectorIDEmpty
	}
	if checkpoint.ConnectorID == "" {
		checkpoint.ConnectorID = id
	}
	if checkpoint.ConnectorID != id {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointIDMismatch
	}
	if err := validateConnectorCheckpoint(checkpoint, false); err != nil {
		return ConnectorCheckpoint{}, err
	}
	if checkpoint.Generation != 0 {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointGenerationInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entry, err := r.connectorCheckpointEntry(id)
	if err != nil {
		return ConnectorCheckpoint{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.removed {
		return ConnectorCheckpoint{}, ErrConnectorNotFound
	}
	if err := r.transitionEntryLocked(ctx, entry, connectorPause); err != nil {
		return ConnectorCheckpoint{}, err
	}
	checkpoint.Generation = entry.status.Generation
	payload, err := EncodeConnectorCheckpoint(checkpoint)
	if err != nil {
		return ConnectorCheckpoint{}, err
	}
	if err := store.Save(ctx, payload); err != nil {
		return ConnectorCheckpoint{}, err
	}
	return cloneConnectorCheckpoint(checkpoint), nil
}

// ResumeFromCheckpoint loads a checkpoint, verifies its connector identity
// and lifecycle generation, applies source state, and resumes the connector.
func (r *ConnectorRegistry) ResumeFromCheckpoint(ctx context.Context, id string, store ConnectorCheckpointStore, apply ConnectorCheckpointApplier) (ConnectorCheckpoint, error) {
	if store == nil {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointStoreRequired
	}
	if apply == nil {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointApplierRequired
	}
	if id == "" {
		return ConnectorCheckpoint{}, ErrConnectorIDEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entry, err := r.connectorCheckpointEntry(id)
	if err != nil {
		return ConnectorCheckpoint{}, err
	}
	payload, err := store.Load(ctx)
	if err != nil {
		return ConnectorCheckpoint{}, err
	}
	if payload == nil {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointNotFound
	}
	checkpoint, err := DecodeConnectorCheckpoint(payload)
	if err != nil {
		return ConnectorCheckpoint{}, err
	}
	if checkpoint.ConnectorID != id {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointIDMismatch
	}
	if err := ctx.Err(); err != nil {
		return ConnectorCheckpoint{}, err
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.removed {
		return ConnectorCheckpoint{}, ErrConnectorNotFound
	}
	if entry.status.State != ConnectorPaused {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointStateInvalid
	}
	if entry.status.Generation != checkpoint.Generation {
		return ConnectorCheckpoint{}, ErrConnectorCheckpointStale
	}
	if err := apply(ctx, cloneConnectorCheckpoint(checkpoint)); err != nil {
		return ConnectorCheckpoint{}, err
	}
	if err := r.transitionEntryLocked(ctx, entry, connectorResume); err != nil {
		return ConnectorCheckpoint{}, err
	}
	return cloneConnectorCheckpoint(checkpoint), nil
}

func (r *ConnectorRegistry) connectorCheckpointEntry(id string) (*managedConnector, error) {
	if r == nil {
		return nil, ErrConnectorRegistryClosed
	}
	r.mu.RLock()
	closed := r.closed
	entry := r.connectors[id]
	r.mu.RUnlock()
	if closed {
		return nil, ErrConnectorRegistryClosed
	}
	if entry == nil {
		return nil, ErrConnectorNotFound
	}
	return entry, nil
}

func validateConnectorCheckpoint(checkpoint ConnectorCheckpoint, requireGeneration bool) error {
	if len(checkpoint.ConnectorID) == 0 || len(checkpoint.ConnectorID) > maxConnectorCheckpointIDBytes {
		return ErrConnectorCheckpointInvalid
	}
	if checkpoint.Sequence == 0 {
		return ErrConnectorCheckpointSequenceInvalid
	}
	if requireGeneration && checkpoint.Generation == 0 {
		return ErrConnectorCheckpointGenerationInvalid
	}
	if len(checkpoint.Frontier) > maxConnectorCheckpointFieldBytes || len(checkpoint.Offset) > maxConnectorCheckpointFieldBytes {
		return ErrConnectorCheckpointPayloadTooLarge
	}
	total := connectorCheckpointHeaderBytes + len(checkpoint.ConnectorID) + len(checkpoint.Frontier) + len(checkpoint.Offset) + connectorCheckpointChecksumBytes
	if total > maxConnectorCheckpointBytes {
		return ErrConnectorCheckpointPayloadTooLarge
	}
	return nil
}

func cloneConnectorCheckpoint(checkpoint ConnectorCheckpoint) ConnectorCheckpoint {
	checkpoint.Offset = append([]byte(nil), checkpoint.Offset...)
	checkpoint.Frontier = append([]byte(nil), checkpoint.Frontier...)
	return checkpoint
}
