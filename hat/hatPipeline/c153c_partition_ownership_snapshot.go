package hatPipeline

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var (
	// ErrQueuePartitionOwnershipSnapshotInvalid indicates malformed or unsafe
	// snapshot input, including invalid assignment state or bounds.
	ErrQueuePartitionOwnershipSnapshotInvalid = errors.New("hatPipeline: queue partition ownership snapshot is invalid")
	// ErrQueuePartitionOwnershipSnapshotChecksum indicates a corrupted payload.
	ErrQueuePartitionOwnershipSnapshotChecksum = errors.New("hatPipeline: queue partition ownership snapshot checksum mismatch")
)

const (
	queuePartitionOwnershipSnapshotMagic         = "QPO1"
	queuePartitionOwnershipSnapshotVersion       = byte(1)
	queuePartitionOwnershipSnapshotHeaderBytes   = 4 + 1 + 4
	queuePartitionOwnershipSnapshotRecordBytes   = 4 + 8 + 8 + 1 + 1 + 2 + 2
	queuePartitionOwnershipSnapshotChecksumBytes = 4
	queuePartitionOwnershipSnapshotMaxString     = int(^uint16(0))
	queuePartitionOwnershipSnapshotMaxBytes      = 64 << 20
)

// MarshalSnapshot returns a compact, deterministic, CRC-protected snapshot of
// the current ownership metadata. The snapshot is safe to persist or transfer
// to a caller-owned control plane; it does not alter the routing hot path.
func (ownership *QueuePartitionOwnership) MarshalSnapshot() ([]byte, error) {
	if ownership == nil {
		return nil, ErrQueuePartitionOwnershipNil
	}
	snapshot := ownership.snapshot.Load()
	if snapshot == nil {
		return nil, ErrQueuePartitionOwnershipSnapshotInvalid
	}
	if err := validateQueuePartitionOwnershipAssignments(snapshot.assignments); err != nil {
		return nil, err
	}

	totalBytes := queuePartitionOwnershipSnapshotHeaderBytes + queuePartitionOwnershipSnapshotChecksumBytes
	for _, assignment := range snapshot.assignments {
		if len(assignment.Owner) > queuePartitionOwnershipSnapshotMaxString || len(assignment.Target) > queuePartitionOwnershipSnapshotMaxString {
			return nil, fmt.Errorf("%w: owner or target exceeds %d bytes", ErrQueuePartitionOwnershipSnapshotInvalid, queuePartitionOwnershipSnapshotMaxString)
		}
		recordBytes := queuePartitionOwnershipSnapshotRecordBytes + len(assignment.Owner) + len(assignment.Target)
		if totalBytes > queuePartitionOwnershipSnapshotMaxBytes-recordBytes {
			return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrQueuePartitionOwnershipSnapshotInvalid, queuePartitionOwnershipSnapshotMaxBytes)
		}
		totalBytes += recordBytes
	}

	payload := make([]byte, totalBytes)
	copy(payload, queuePartitionOwnershipSnapshotMagic)
	payload[4] = queuePartitionOwnershipSnapshotVersion
	binary.BigEndian.PutUint32(payload[5:9], uint32(len(snapshot.assignments)))
	offset := queuePartitionOwnershipSnapshotHeaderBytes
	for _, assignment := range snapshot.assignments {
		binary.BigEndian.PutUint32(payload[offset:offset+4], uint32(assignment.Partition))
		offset += 4
		binary.BigEndian.PutUint64(payload[offset:offset+8], assignment.Generation)
		offset += 8
		binary.BigEndian.PutUint64(payload[offset:offset+8], assignment.Fence)
		offset += 8
		payload[offset] = byte(assignment.State)
		offset++
		if assignment.MigrationReady {
			payload[offset] = 1
		}
		offset++
		binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(len(assignment.Owner)))
		offset += 2
		binary.BigEndian.PutUint16(payload[offset:offset+2], uint16(len(assignment.Target)))
		offset += 2
		copy(payload[offset:], assignment.Owner)
		offset += len(assignment.Owner)
		copy(payload[offset:], assignment.Target)
		offset += len(assignment.Target)
	}
	binary.BigEndian.PutUint32(payload[offset:], crc32.ChecksumIEEE(payload[:offset]))
	return payload, nil
}

// RestoreSnapshot validates a complete snapshot before atomically publishing
// it. A malformed or corrupt payload leaves the current routing state intact.
func (ownership *QueuePartitionOwnership) RestoreSnapshot(payload []byte) error {
	if ownership == nil {
		return ErrQueuePartitionOwnershipNil
	}
	assignments, err := decodeQueuePartitionOwnershipSnapshot(payload)
	if err != nil {
		return err
	}
	owners := make([]string, len(assignments))
	for index, assignment := range assignments {
		owners[index] = assignment.Owner
	}
	ownership.controlMu.Lock()
	ownership.snapshot.Store(&queuePartitionOwnershipSnapshot{assignments: assignments, owners: owners})
	ownership.controlMu.Unlock()
	return nil
}

func decodeQueuePartitionOwnershipSnapshot(payload []byte) ([]QueuePartitionAssignment, error) {
	if len(payload) < queuePartitionOwnershipSnapshotHeaderBytes+queuePartitionOwnershipSnapshotChecksumBytes || len(payload) > queuePartitionOwnershipSnapshotMaxBytes {
		return nil, fmt.Errorf("%w: payload length %d", ErrQueuePartitionOwnershipSnapshotInvalid, len(payload))
	}
	if string(payload[:4]) != queuePartitionOwnershipSnapshotMagic || payload[4] != queuePartitionOwnershipSnapshotVersion {
		return nil, fmt.Errorf("%w: unsupported magic or version", ErrQueuePartitionOwnershipSnapshotInvalid)
	}
	checksumOffset := len(payload) - queuePartitionOwnershipSnapshotChecksumBytes
	expected := binary.BigEndian.Uint32(payload[checksumOffset:])
	actual := crc32.ChecksumIEEE(payload[:checksumOffset])
	if expected != actual {
		return nil, ErrQueuePartitionOwnershipSnapshotChecksum
	}
	count := binary.BigEndian.Uint32(payload[5:9])
	if count == 0 || count > MaxQueuePartitionOwnershipPartitions {
		return nil, fmt.Errorf("%w: partition count %d", ErrQueuePartitionOwnershipSnapshotInvalid, count)
	}
	assignments := make([]QueuePartitionAssignment, int(count))
	offset := queuePartitionOwnershipSnapshotHeaderBytes
	for index := range assignments {
		if checksumOffset-offset < queuePartitionOwnershipSnapshotRecordBytes {
			return nil, fmt.Errorf("%w: truncated record %d", ErrQueuePartitionOwnershipSnapshotInvalid, index)
		}
		partition := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		generation := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8
		fence := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8
		state := QueuePartitionOwnershipState(payload[offset])
		offset++
		ready := payload[offset]
		offset++
		ownerLength := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		targetLength := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if ownerLength > queuePartitionOwnershipSnapshotMaxString || targetLength > queuePartitionOwnershipSnapshotMaxString || ownerLength+targetLength > checksumOffset-offset {
			return nil, fmt.Errorf("%w: invalid string lengths in record %d", ErrQueuePartitionOwnershipSnapshotInvalid, index)
		}
		assignment := QueuePartitionAssignment{
			Partition:      int(partition),
			Owner:          string(payload[offset : offset+ownerLength]),
			Target:         string(payload[offset+ownerLength : offset+ownerLength+targetLength]),
			Fence:          fence,
			Generation:     generation,
			State:          state,
			MigrationReady: ready == 1,
		}
		offset += ownerLength + targetLength
		if ready > 1 || assignment.Partition != index {
			return nil, fmt.Errorf("%w: record %d identity or readiness is invalid", ErrQueuePartitionOwnershipSnapshotInvalid, index)
		}
		assignments[index] = assignment
	}
	if offset != checksumOffset {
		return nil, fmt.Errorf("%w: trailing bytes after records", ErrQueuePartitionOwnershipSnapshotInvalid)
	}
	if err := validateQueuePartitionOwnershipAssignments(assignments); err != nil {
		return nil, err
	}
	return assignments, nil
}

func validateQueuePartitionOwnershipAssignments(assignments []QueuePartitionAssignment) error {
	if len(assignments) == 0 || len(assignments) > MaxQueuePartitionOwnershipPartitions {
		return fmt.Errorf("%w: partition count %d", ErrQueuePartitionOwnershipSnapshotInvalid, len(assignments))
	}
	for index, assignment := range assignments {
		if assignment.Partition != index || assignment.Generation == 0 {
			return fmt.Errorf("%w: record %d identity or generation is invalid", ErrQueuePartitionOwnershipSnapshotInvalid, index)
		}
		if len(assignment.Owner) > queuePartitionOwnershipSnapshotMaxString || len(assignment.Target) > queuePartitionOwnershipSnapshotMaxString {
			return fmt.Errorf("%w: record %d owner or target is too long", ErrQueuePartitionOwnershipSnapshotInvalid, index)
		}
		switch assignment.State {
		case QueuePartitionOwnershipStable:
			if assignment.Target != "" || assignment.Fence != 0 || assignment.MigrationReady {
				return fmt.Errorf("%w: stable record %d contains migration state", ErrQueuePartitionOwnershipSnapshotInvalid, index)
			}
		case QueuePartitionOwnershipMigrating:
			if assignment.Owner == "" || assignment.Target == "" || assignment.Owner == assignment.Target {
				return fmt.Errorf("%w: migrating record %d has invalid owners", ErrQueuePartitionOwnershipSnapshotInvalid, index)
			}
		default:
			return fmt.Errorf("%w: record %d has unknown state %d", ErrQueuePartitionOwnershipSnapshotInvalid, index, assignment.State)
		}
	}
	return nil
}
