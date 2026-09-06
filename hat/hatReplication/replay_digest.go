package hatReplication

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrReplaySequenceInvalid = errors.New("hatriecache: replay sequence is invalid")
	ErrReplayMismatch        = errors.New("hatriecache: replay records differ")
)

// ReplayRecord is the canonical input to a deterministic replay check. The
// payload is opaque to this package and is framed with its length.
type ReplayRecord struct {
	Sequence uint64
	Payload  []byte
}

// ReplayDigest summarizes a validated replay stream.
type ReplayDigest struct {
	FirstSequence uint64
	LastSequence  uint64
	Records       int
	SHA256        [sha256.Size]byte
}

// DigestReplayRecords computes a deterministic digest for records in replay
// order. Sequence numbers must be positive and strictly increasing.
func DigestReplayRecords(records []ReplayRecord) (ReplayDigest, error) {
	digest := ReplayDigest{Records: len(records)}
	hash := sha256.New()
	var scratch [binary.MaxVarintLen64]byte
	var previous uint64
	for index, record := range records {
		if record.Sequence == 0 || index > 0 && record.Sequence <= previous {
			return ReplayDigest{}, fmt.Errorf("%w at record %d: sequence=%d previous=%d", ErrReplaySequenceInvalid, index, record.Sequence, previous)
		}
		if index == 0 {
			digest.FirstSequence = record.Sequence
		}
		digest.LastSequence = record.Sequence
		previous = record.Sequence
		n := binary.PutUvarint(scratch[:], record.Sequence)
		_, _ = hash.Write(scratch[:n])
		n = binary.PutUvarint(scratch[:], uint64(len(record.Payload)))
		_, _ = hash.Write(scratch[:n])
		_, _ = hash.Write(record.Payload)
	}
	copy(digest.SHA256[:], hash.Sum(nil))
	return digest, nil
}

// VerifyReplayRecords checks that two validated replay streams have identical
// sequence and payload content.
func VerifyReplayRecords(expected, actual []ReplayRecord) error {
	expectedDigest, err := DigestReplayRecords(expected)
	if err != nil {
		return err
	}
	actualDigest, err := DigestReplayRecords(actual)
	if err != nil {
		return err
	}
	if expectedDigest != actualDigest {
		return fmt.Errorf("%w: expected=%x actual=%x", ErrReplayMismatch, expectedDigest.SHA256, actualDigest.SHA256)
	}
	return nil
}
