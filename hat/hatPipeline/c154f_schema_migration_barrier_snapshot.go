package hatPipeline

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

var (
	// ErrSchemaMigrationBarrierSnapshotInvalid indicates malformed or unsafe
	// snapshot input, including invalid barrier state or bounds.
	ErrSchemaMigrationBarrierSnapshotInvalid = errors.New("schema migration barrier snapshot is invalid")
	// ErrSchemaMigrationBarrierSnapshotChecksum indicates a corrupt payload.
	ErrSchemaMigrationBarrierSnapshotChecksum = errors.New("schema migration barrier snapshot checksum mismatch")
)

const (
	schemaMigrationBarrierSnapshotMagic         = "SMB1"
	schemaMigrationBarrierSnapshotVersion       = byte(1)
	schemaMigrationBarrierSnapshotHeaderBytes   = 4 + 1 + 4
	schemaMigrationBarrierSnapshotRecordBytes   = 2 + 8 + 1 + 2 + 2
	schemaMigrationBarrierSnapshotChecksumBytes = 4
	schemaMigrationBarrierSnapshotMaxBytes      = 64 << 20
)

// MarshalSnapshot returns a deterministic, bounded, CRC-protected snapshot of
// all pending and terminal schema barriers. It is opt-in and does not alter
// the normal barrier protocol.
func (barrier *SchemaMigrationBarrier) MarshalSnapshot() ([]byte, error) {
	if barrier == nil {
		return nil, ErrSchemaMigrationBarrierNil
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	ids := make([]string, 0, len(barrier.barriers))
	for id := range barrier.barriers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > MaxSchemaMigrationBarrierMaxBarriers {
		return nil, fmt.Errorf("%w: barrier count %d", ErrSchemaMigrationBarrierSnapshotInvalid, len(ids))
	}
	totalBytes := schemaMigrationBarrierSnapshotHeaderBytes + schemaMigrationBarrierSnapshotChecksumBytes
	for _, id := range ids {
		entry := barrier.barriers[id]
		if err := validateSchemaMigrationBarrierSnapshotStatus(entry.status); err != nil {
			return nil, err
		}
		recordBytes, err := schemaMigrationBarrierSnapshotRecordSize(entry.status)
		if err != nil {
			return nil, err
		}
		if totalBytes > schemaMigrationBarrierSnapshotMaxBytes-recordBytes {
			return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrSchemaMigrationBarrierSnapshotInvalid, schemaMigrationBarrierSnapshotMaxBytes)
		}
		totalBytes += recordBytes
	}

	payload := make([]byte, totalBytes)
	copy(payload, schemaMigrationBarrierSnapshotMagic)
	payload[4] = schemaMigrationBarrierSnapshotVersion
	binary.BigEndian.PutUint32(payload[5:9], uint32(len(ids)))
	offset := schemaMigrationBarrierSnapshotHeaderBytes
	for _, id := range ids {
		status := barrier.barriers[id].status
		binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(len(status.ID)))
		offset += 2
		binary.BigEndian.PutUint64(payload[offset:offset+8], status.Version)
		offset += 8
		payload[offset] = schemaMigrationBarrierSnapshotStateByte(status.State)
		offset++
		binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(len(status.Dependencies)))
		offset += 2
		binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(len(status.AcknowledgedDependencies)))
		offset += 2
		copy(payload[offset:], status.ID)
		offset += len(status.ID)
		for _, dependency := range status.Dependencies {
			binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(len(dependency)))
			offset += 2
			copy(payload[offset:], dependency)
			offset += len(dependency)
		}
		for _, acknowledged := range status.AcknowledgedDependencies {
			dependencyIndex := sort.SearchStrings(status.Dependencies, acknowledged)
			binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(dependencyIndex))
			offset += 2
		}
	}
	binary.BigEndian.PutUint32(payload[offset:], crc32.ChecksumIEEE(payload[:offset]))
	return payload, nil
}

// RestoreSnapshot validates a complete snapshot and atomically replaces the
// barrier registry. Invalid or corrupt input leaves all existing barriers
// untouched.
func (barrier *SchemaMigrationBarrier) RestoreSnapshot(payload []byte) error {
	if barrier == nil {
		return ErrSchemaMigrationBarrierNil
	}
	entries, err := decodeSchemaMigrationBarrierSnapshot(payload, barrier.maxBarriers, barrier.maxDependencies)
	if err != nil {
		return err
	}
	barrier.mu.Lock()
	barrier.barriers = entries
	barrier.mu.Unlock()
	return nil
}

func decodeSchemaMigrationBarrierSnapshot(payload []byte, maxBarriers, maxDependencies int) (map[string]*schemaMigrationBarrierEntry, error) {
	if len(payload) < schemaMigrationBarrierSnapshotHeaderBytes+schemaMigrationBarrierSnapshotChecksumBytes || len(payload) > schemaMigrationBarrierSnapshotMaxBytes {
		return nil, fmt.Errorf("%w: payload length %d", ErrSchemaMigrationBarrierSnapshotInvalid, len(payload))
	}
	if string(payload[:4]) != schemaMigrationBarrierSnapshotMagic || payload[4] != schemaMigrationBarrierSnapshotVersion {
		return nil, fmt.Errorf("%w: unsupported magic or version", ErrSchemaMigrationBarrierSnapshotInvalid)
	}
	checksumOffset := len(payload) - schemaMigrationBarrierSnapshotChecksumBytes
	expected := binary.BigEndian.Uint32(payload[checksumOffset:])
	actual := crc32.ChecksumIEEE(payload[:checksumOffset])
	if expected != actual {
		return nil, ErrSchemaMigrationBarrierSnapshotChecksum
	}
	count := binary.BigEndian.Uint32(payload[5:9])
	if count > uint32(maxBarriers) || count > MaxSchemaMigrationBarrierMaxBarriers {
		return nil, fmt.Errorf("%w: barrier count %d", ErrSchemaMigrationBarrierSnapshotInvalid, count)
	}
	entries := make(map[string]*schemaMigrationBarrierEntry, int(count))
	offset := schemaMigrationBarrierSnapshotHeaderBytes
	for index := uint32(0); index < count; index++ {
		if checksumOffset-offset < schemaMigrationBarrierSnapshotRecordBytes {
			return nil, fmt.Errorf("%w: truncated record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
		}
		idLength := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		version := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8
		state, ok := schemaMigrationBarrierSnapshotState(payload[offset])
		offset++
		dependencyCount := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		acknowledgedCount := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if !ok || idLength == 0 || idLength > MaxSchemaMigrationBarrierTextBytes || dependencyCount == 0 || dependencyCount > maxDependencies || acknowledgedCount > dependencyCount || idLength > checksumOffset-offset {
			return nil, fmt.Errorf("%w: invalid header in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
		}
		id := string(payload[offset : offset+idLength])
		offset += idLength
		if strings.TrimSpace(id) != id {
			return nil, fmt.Errorf("%w: non-canonical barrier ID in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
		}
		if _, exists := entries[id]; exists {
			return nil, fmt.Errorf("%w: duplicate barrier ID %q", ErrSchemaMigrationBarrierSnapshotInvalid, id)
		}
		dependencies := make([]string, dependencyCount)
		dependencySet := make(map[string]struct{}, dependencyCount)
		for dependencyIndex := range dependencies {
			if checksumOffset-offset < 2 {
				return nil, fmt.Errorf("%w: truncated dependency length in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
			}
			length := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
			offset += 2
			if length == 0 || length > MaxSchemaMigrationBarrierTextBytes || length > checksumOffset-offset {
				return nil, fmt.Errorf("%w: invalid dependency length in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
			}
			dependency := string(payload[offset : offset+length])
			offset += length
			if strings.TrimSpace(dependency) != dependency || (dependencyIndex > 0 && dependencies[dependencyIndex-1] >= dependency) {
				return nil, fmt.Errorf("%w: dependencies are not canonical in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
			}
			if _, exists := dependencySet[dependency]; exists {
				return nil, fmt.Errorf("%w: duplicate dependency in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
			}
			dependencies[dependencyIndex] = dependency
			dependencySet[dependency] = struct{}{}
		}
		acknowledged := make([]string, acknowledgedCount)
		acknowledgedSet := make(map[string]struct{}, acknowledgedCount)
		for acknowledgedIndex := range acknowledged {
			if checksumOffset-offset < 2 {
				return nil, fmt.Errorf("%w: truncated acknowledgement in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
			}
			dependencyIndex := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
			offset += 2
			if dependencyIndex >= len(dependencies) || (acknowledgedIndex > 0 && acknowledged[acknowledgedIndex-1] >= dependencies[dependencyIndex]) {
				return nil, fmt.Errorf("%w: acknowledgement order is invalid in record %d", ErrSchemaMigrationBarrierSnapshotInvalid, index)
			}
			acknowledged[acknowledgedIndex] = dependencies[dependencyIndex]
			acknowledgedSet[dependencies[dependencyIndex]] = struct{}{}
		}
		status := SchemaMigrationBarrierStatus{
			ID:                       id,
			Version:                  version,
			State:                    state,
			Dependencies:             dependencies,
			AcknowledgedDependencies: acknowledged,
			Acknowledged:             acknowledgedCount,
			Remaining:                dependencyCount - acknowledgedCount,
		}
		if err := validateSchemaMigrationBarrierSnapshotStatus(status); err != nil {
			return nil, err
		}
		entries[id] = &schemaMigrationBarrierEntry{status: status, dependencies: dependencySet, acknowledged: acknowledgedSet}
	}
	if offset != checksumOffset {
		return nil, fmt.Errorf("%w: trailing bytes after records", ErrSchemaMigrationBarrierSnapshotInvalid)
	}
	return entries, nil
}

func schemaMigrationBarrierSnapshotRecordSize(status SchemaMigrationBarrierStatus) (int, error) {
	if err := validateSchemaMigrationBarrierSnapshotStatus(status); err != nil {
		return 0, err
	}
	size := schemaMigrationBarrierSnapshotRecordBytes + len(status.ID)
	for _, dependency := range status.Dependencies {
		size += 2 + len(dependency)
	}
	size += 2 * len(status.AcknowledgedDependencies)
	return size, nil
}

func validateSchemaMigrationBarrierSnapshotStatus(status SchemaMigrationBarrierStatus) error {
	if status.ID == "" || len(status.ID) > MaxSchemaMigrationBarrierTextBytes || strings.TrimSpace(status.ID) != status.ID || status.Version == 0 || len(status.Dependencies) == 0 || len(status.Dependencies) > MaxSchemaMigrationBarrierMaxDependencies || status.Acknowledged < 0 || status.Acknowledged != len(status.AcknowledgedDependencies) || status.Remaining != len(status.Dependencies)-status.Acknowledged {
		return fmt.Errorf("%w: barrier %q has invalid identity, version, or counts", ErrSchemaMigrationBarrierSnapshotInvalid, status.ID)
	}
	if status.State != SchemaMigrationBarrierPrepared && status.State != SchemaMigrationBarrierCommitted && status.State != SchemaMigrationBarrierAborted {
		return fmt.Errorf("%w: barrier %q has unknown state %q", ErrSchemaMigrationBarrierSnapshotInvalid, status.ID, status.State)
	}
	for index, dependency := range status.Dependencies {
		if dependency == "" || len(dependency) > MaxSchemaMigrationBarrierTextBytes || strings.TrimSpace(dependency) != dependency || (index > 0 && status.Dependencies[index-1] >= dependency) {
			return fmt.Errorf("%w: barrier %q has non-canonical dependencies", ErrSchemaMigrationBarrierSnapshotInvalid, status.ID)
		}
	}
	for index, acknowledged := range status.AcknowledgedDependencies {
		if index > 0 && status.AcknowledgedDependencies[index-1] >= acknowledged {
			return fmt.Errorf("%w: barrier %q has non-canonical acknowledgements", ErrSchemaMigrationBarrierSnapshotInvalid, status.ID)
		}
		position := sort.SearchStrings(status.Dependencies, acknowledged)
		if position >= len(status.Dependencies) || status.Dependencies[position] != acknowledged {
			return fmt.Errorf("%w: barrier %q acknowledges an unknown dependency", ErrSchemaMigrationBarrierSnapshotInvalid, status.ID)
		}
	}
	if status.State == SchemaMigrationBarrierCommitted && status.Remaining != 0 {
		return fmt.Errorf("%w: committed barrier %q is not fully acknowledged", ErrSchemaMigrationBarrierSnapshotInvalid, status.ID)
	}
	return nil
}

func schemaMigrationBarrierSnapshotStateByte(state SchemaMigrationBarrierState) byte {
	switch state {
	case SchemaMigrationBarrierPrepared:
		return 1
	case SchemaMigrationBarrierCommitted:
		return 2
	case SchemaMigrationBarrierAborted:
		return 3
	default:
		return 0
	}
}

func schemaMigrationBarrierSnapshotState(value byte) (SchemaMigrationBarrierState, bool) {
	switch value {
	case 1:
		return SchemaMigrationBarrierPrepared, true
	case 2:
		return SchemaMigrationBarrierCommitted, true
	case 3:
		return SchemaMigrationBarrierAborted, true
	default:
		return "", false
	}
}
