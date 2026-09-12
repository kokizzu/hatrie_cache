package hatPipeline

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"
)

var (
	// ErrFrontierSnapshotInvalid indicates malformed, duplicate, or
	// semantically invalid snapshot data.
	ErrFrontierSnapshotInvalid = errors.New("hatPipeline: frontier snapshot is invalid")
	// ErrFrontierSnapshotNotEmpty indicates that restore would overwrite
	// already-registered frontier state.
	ErrFrontierSnapshotNotEmpty = errors.New("hatPipeline: frontier snapshot requires an empty registry")
)

const (
	frontierSnapshotHeaderSize = 5
	maxFrontierSnapshotBytes   = 64 << 20
	maxFrontierSnapshotIDBytes = 1 << 20
)

var frontierSnapshotMagic = [4]byte{'H', 'F', 'R', '1'}

// MarshalSnapshot encodes all named frontiers in deterministic ID order. The
// versioned binary format is suitable for a caller-managed checkpoint or
// backup object and contains no pointers or runtime-only channels.
func (registry *FrontierRegistry) MarshalSnapshot() ([]byte, error) {
	if registry == nil {
		return nil, ErrFrontierClosed
	}
	snapshots := registry.SnapshotAll()
	if len(snapshots) > maxFrontierObjects {
		return nil, ErrFrontierObjectLimit
	}
	encoded := make([]byte, 0, frontierSnapshotHeaderSize+binary.MaxVarintLen64+len(snapshots)*48)
	encoded = append(encoded, frontierSnapshotMagic[:]...)
	encoded = append(encoded, 1)
	encoded = appendFrontierSnapshotUvarint(encoded, uint64(len(snapshots)))
	for _, snapshot := range snapshots {
		if snapshot.ID == "" || len(snapshot.ID) > maxFrontierSnapshotIDBytes || snapshot.Lower > snapshot.Upper {
			return nil, ErrFrontierSnapshotInvalid
		}
		encoded = appendFrontierSnapshotUvarint(encoded, uint64(len(snapshot.ID)))
		encoded = append(encoded, snapshot.ID...)
		encoded = appendFrontierSnapshotUvarint(encoded, snapshot.Lower)
		encoded = appendFrontierSnapshotUvarint(encoded, snapshot.Upper)
		encoded = appendFrontierSnapshotUvarint(encoded, snapshot.Generation)
		var timestamp [binary.MaxVarintLen64]byte
		size := binary.PutVarint(timestamp[:], snapshot.UpdatedAt.UnixNano())
		encoded = append(encoded, timestamp[:size]...)
		if len(encoded) > maxFrontierSnapshotBytes {
			return nil, ErrFrontierSnapshotInvalid
		}
	}
	return encoded, nil
}

// RestoreSnapshot atomically restores a validated snapshot into an empty
// registry. It never overwrites existing frontier state, and restored values
// continue to obey the normal monotone Advance contract.
func (registry *FrontierRegistry) RestoreSnapshot(payload []byte) error {
	if registry == nil {
		return ErrFrontierClosed
	}
	registry.mu.RLock()
	maxObjects := registry.maxObjects
	if maxObjects == 0 {
		maxObjects = DefaultFrontierMaxObjects
	}
	registry.mu.RUnlock()
	snapshots, err := decodeFrontierSnapshot(payload, maxObjects)
	if err != nil {
		return err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierClosed
	}
	if len(registry.objects) != 0 {
		return ErrFrontierSnapshotNotEmpty
	}
	if registry.maxObjects == 0 {
		registry.maxObjects = DefaultFrontierMaxObjects
	}
	if len(snapshots) > registry.maxObjects {
		return ErrFrontierObjectLimit
	}
	if registry.objects == nil {
		registry.objects = make(map[string]*frontierObject, len(snapshots))
	}
	for _, snapshot := range snapshots {
		registry.objects[snapshot.ID] = &frontierObject{
			id:         snapshot.ID,
			lower:      snapshot.Lower,
			upper:      snapshot.Upper,
			generation: snapshot.Generation,
			updatedAt:  snapshot.UpdatedAt,
		}
	}
	return nil
}

func decodeFrontierSnapshot(payload []byte, maxObjects int) ([]FrontierSnapshot, error) {
	if len(payload) < frontierSnapshotHeaderSize || len(payload) > maxFrontierSnapshotBytes || !bytes.Equal(payload[:4], frontierSnapshotMagic[:]) || payload[4] != 1 {
		return nil, ErrFrontierSnapshotInvalid
	}
	offset := frontierSnapshotHeaderSize
	count, size := readFrontierSnapshotUvarint(payload[offset:])
	if size <= 0 {
		return nil, ErrFrontierSnapshotInvalid
	}
	offset += size
	if count > uint64(maxFrontierObjects) || count > uint64(maxObjects) {
		return nil, ErrFrontierObjectLimit
	}
	snapshots := make([]FrontierSnapshot, 0, int(count))
	seen := make(map[string]struct{}, int(count))
	for index := uint64(0); index < count; index++ {
		idLength, size := readFrontierSnapshotUvarint(payload[offset:])
		if size <= 0 || idLength == 0 || idLength > maxFrontierSnapshotIDBytes || idLength > uint64(len(payload)-offset-size) {
			return nil, ErrFrontierSnapshotInvalid
		}
		offset += size
		idEnd := offset + int(idLength)
		id := string(payload[offset:idEnd])
		offset = idEnd
		if _, exists := seen[id]; exists {
			return nil, ErrFrontierSnapshotInvalid
		}
		seen[id] = struct{}{}

		lower, size := readFrontierSnapshotUvarint(payload[offset:])
		if size <= 0 {
			return nil, ErrFrontierSnapshotInvalid
		}
		offset += size
		upper, size := readFrontierSnapshotUvarint(payload[offset:])
		if size <= 0 || lower > upper {
			return nil, ErrFrontierSnapshotInvalid
		}
		offset += size
		generation, size := readFrontierSnapshotUvarint(payload[offset:])
		if size <= 0 {
			return nil, ErrFrontierSnapshotInvalid
		}
		offset += size
		updatedAt, size := readFrontierSnapshotVarint(payload[offset:])
		if size <= 0 {
			return nil, ErrFrontierSnapshotInvalid
		}
		offset += size
		snapshots = append(snapshots, FrontierSnapshot{
			ID:         id,
			Lower:      lower,
			Upper:      upper,
			Generation: generation,
			UpdatedAt:  time.Unix(0, updatedAt).UTC(),
		})
	}
	if offset != len(payload) {
		return nil, ErrFrontierSnapshotInvalid
	}
	return snapshots, nil
}

func appendFrontierSnapshotUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func readFrontierSnapshotUvarint(payload []byte) (uint64, int) {
	if len(payload) == 0 {
		return 0, 0
	}
	return binary.Uvarint(payload)
}

func readFrontierSnapshotVarint(payload []byte) (int64, int) {
	if len(payload) == 0 {
		return 0, 0
	}
	return binary.Varint(payload)
}
