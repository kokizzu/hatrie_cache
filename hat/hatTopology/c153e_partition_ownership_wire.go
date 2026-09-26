package hatTopology

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	partitionOwnershipConsensusWireMagic = "POV1"
	// MaxPartitionOwnershipConsensusVoteWireBytes bounds one received vote
	// before any variable-length field is decoded.
	MaxPartitionOwnershipConsensusVoteWireBytes = 64 << 10
	maxPartitionOwnershipConsensusWireString    = 16 << 10
	maxPartitionOwnershipConsensusWireReplicas  = 1 << 10
	partitionOwnershipConsensusWireAccepted     = 1 << 0
	partitionOwnershipConsensusWireSigned       = 1 << 1
)

var (
	// ErrPartitionOwnershipConsensusWireInvalid indicates malformed or
	// semantically invalid wire data.
	ErrPartitionOwnershipConsensusWireInvalid = errors.New("hatriecache: invalid partition ownership consensus wire data")
	// ErrPartitionOwnershipConsensusWireTooLarge indicates that a payload or
	// one of its bounded fields exceeds the wire limit.
	ErrPartitionOwnershipConsensusWireTooLarge = errors.New("hatriecache: partition ownership consensus wire data is too large")
)

// MarshalPartitionOwnershipConsensusVote encodes one ownership vote in a
// deterministic bounded binary format. The format carries both legacy
// unsigned votes and the optional C153d HMAC key ID/signature fields, so it is
// suitable for HTTP, gRPC, or another caller-owned transport.
func MarshalPartitionOwnershipConsensusVote(vote PartitionOwnershipConsensusVote) ([]byte, error) {
	if err := validatePartitionOwnershipConsensusWireVote(vote); err != nil {
		return nil, err
	}
	if len(vote.Ownership.Replicas) > maxPartitionOwnershipConsensusWireReplicas {
		return nil, ErrPartitionOwnershipConsensusWireTooLarge
	}
	encodedSize := len(partitionOwnershipConsensusWireMagic) + 1 + 4 + 8
	encodedSize += partitionOwnershipConsensusWireStringSize(vote.NodeID)
	encodedSize += partitionOwnershipConsensusWireStringSize(vote.Ownership.Primary)
	encodedSize += partitionOwnershipConsensusWireStringSize(vote.Ownership.TopologyFingerprint)
	encodedSize += partitionOwnershipConsensusWireStringSize(vote.KeyID)
	encodedSize += partitionOwnershipConsensusWireUvarintSize(uint64(len(vote.Ownership.Replicas)))
	for _, replica := range vote.Ownership.Replicas {
		encodedSize += partitionOwnershipConsensusWireStringSize(replica)
	}
	encodedSize += partitionOwnershipConsensusWireUvarintSize(uint64(len(vote.Signature))) + len(vote.Signature)
	if encodedSize > MaxPartitionOwnershipConsensusVoteWireBytes {
		return nil, ErrPartitionOwnershipConsensusWireTooLarge
	}

	flags := byte(0)
	if vote.Accepted {
		flags |= partitionOwnershipConsensusWireAccepted
	}
	if len(vote.Signature) > 0 {
		flags |= partitionOwnershipConsensusWireSigned
	}
	payload := make([]byte, 0, encodedSize)
	payload = append(payload, partitionOwnershipConsensusWireMagic...)
	payload = append(payload, flags)
	var fixed [8]byte
	binary.BigEndian.PutUint32(fixed[:4], vote.Ownership.ShardID)
	payload = append(payload, fixed[:4]...)
	binary.BigEndian.PutUint64(fixed[:], vote.Ownership.FencingToken)
	payload = append(payload, fixed[:]...)
	payload = appendPartitionOwnershipConsensusWireString(payload, vote.NodeID)
	payload = appendPartitionOwnershipConsensusWireString(payload, vote.Ownership.Primary)
	payload = appendPartitionOwnershipConsensusWireString(payload, vote.Ownership.TopologyFingerprint)
	payload = appendPartitionOwnershipConsensusWireString(payload, vote.KeyID)
	payload = appendPartitionOwnershipConsensusWireUvarint(payload, uint64(len(vote.Ownership.Replicas)))
	for _, replica := range vote.Ownership.Replicas {
		payload = appendPartitionOwnershipConsensusWireString(payload, replica)
	}
	payload = appendPartitionOwnershipConsensusWireUvarint(payload, uint64(len(vote.Signature)))
	payload = append(payload, vote.Signature...)
	if len(payload) > MaxPartitionOwnershipConsensusVoteWireBytes {
		return nil, ErrPartitionOwnershipConsensusWireTooLarge
	}
	return payload, nil
}

// UnmarshalPartitionOwnershipConsensusVote decodes and validates one bounded
// binary vote. All returned variable-length fields own their memory and no
// caller payload slice is retained.
func UnmarshalPartitionOwnershipConsensusVote(payload []byte) (PartitionOwnershipConsensusVote, error) {
	if len(payload) > MaxPartitionOwnershipConsensusVoteWireBytes {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireTooLarge
	}
	if len(payload) < len(partitionOwnershipConsensusWireMagic)+1+4+8 || string(payload[:len(partitionOwnershipConsensusWireMagic)]) != partitionOwnershipConsensusWireMagic {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	offset := len(partitionOwnershipConsensusWireMagic)
	flags := payload[offset]
	offset++
	if flags & ^byte(partitionOwnershipConsensusWireAccepted|partitionOwnershipConsensusWireSigned) != 0 {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	if len(payload)-offset < 12 {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	shardID := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	fencingToken := binary.BigEndian.Uint64(payload[offset : offset+8])
	offset += 8
	nodeID, ok := readPartitionOwnershipConsensusWireString(payload, &offset)
	if !ok {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	primary, ok := readPartitionOwnershipConsensusWireString(payload, &offset)
	if !ok {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	fingerprint, ok := readPartitionOwnershipConsensusWireString(payload, &offset)
	if !ok {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	keyID, ok := readPartitionOwnershipConsensusWireString(payload, &offset)
	if !ok {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	replicaCount, ok := readPartitionOwnershipConsensusWireUvarint(payload, &offset)
	if !ok || replicaCount > maxPartitionOwnershipConsensusWireReplicas {
		if ok && replicaCount > maxPartitionOwnershipConsensusWireReplicas {
			return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireTooLarge
		}
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	var replicas []string
	if replicaCount > 0 {
		replicas = make([]string, int(replicaCount))
	}
	for index := range replicas {
		replicas[index], ok = readPartitionOwnershipConsensusWireString(payload, &offset)
		if !ok {
			return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
		}
	}
	signatureLength, ok := readPartitionOwnershipConsensusWireUvarint(payload, &offset)
	if !ok {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	if signatureLength > sha256.Size {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	if signatureLength > uint64(len(payload)-offset) {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	signature := append([]byte(nil), payload[offset:offset+int(signatureLength)]...)
	offset += int(signatureLength)
	if offset != len(payload) {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	vote := PartitionOwnershipConsensusVote{
		NodeID: nodeID,
		Ownership: PartitionOwnership{
			ShardID:             shardID,
			Primary:             primary,
			Replicas:            replicas,
			TopologyFingerprint: fingerprint,
			FencingToken:        fencingToken,
		},
		Accepted:  flags&partitionOwnershipConsensusWireAccepted != 0,
		KeyID:     keyID,
		Signature: signature,
	}
	if signatureLength > 0 != (flags&partitionOwnershipConsensusWireSigned != 0) {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusWireInvalid
	}
	if err := validatePartitionOwnershipConsensusWireVote(vote); err != nil {
		return PartitionOwnershipConsensusVote{}, err
	}
	return vote, nil
}

func validatePartitionOwnershipConsensusWireVote(vote PartitionOwnershipConsensusVote) error {
	if vote.NodeID == "" || vote.NodeID != strings.TrimSpace(vote.NodeID) || len(vote.NodeID) > maxPartitionOwnershipConsensusWireString {
		return fmt.Errorf("%w: node id", ErrPartitionOwnershipConsensusWireInvalid)
	}
	if vote.KeyID != strings.TrimSpace(vote.KeyID) || len(vote.KeyID) > maxPartitionOwnershipConsensusWireString {
		return fmt.Errorf("%w: key id", ErrPartitionOwnershipConsensusWireInvalid)
	}
	if len(vote.Signature) != 0 && len(vote.Signature) != sha256.Size {
		return fmt.Errorf("%w: signature length", ErrPartitionOwnershipConsensusWireInvalid)
	}
	if len(vote.Signature) == 0 && vote.KeyID != "" {
		return fmt.Errorf("%w: key id without signature", ErrPartitionOwnershipConsensusWireInvalid)
	}
	if len(vote.Ownership.Replicas) > maxPartitionOwnershipConsensusWireReplicas {
		return ErrPartitionOwnershipConsensusWireTooLarge
	}
	if len(vote.Ownership.Primary) > maxPartitionOwnershipConsensusWireString || len(vote.Ownership.TopologyFingerprint) > maxPartitionOwnershipConsensusWireString {
		return ErrPartitionOwnershipConsensusWireTooLarge
	}
	if err := validatePartitionOwnershipConsensusMetadata(vote.Ownership); err != nil {
		return fmt.Errorf("%w: ownership: %v", ErrPartitionOwnershipConsensusWireInvalid, err)
	}
	for _, replica := range vote.Ownership.Replicas {
		if len(replica) > maxPartitionOwnershipConsensusWireString {
			return ErrPartitionOwnershipConsensusWireTooLarge
		}
	}
	return nil
}

func appendPartitionOwnershipConsensusWireUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(payload, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func partitionOwnershipConsensusWireUvarintSize(value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutUvarint(encoded[:], value)
}

func partitionOwnershipConsensusWireStringSize(value string) int {
	return partitionOwnershipConsensusWireUvarintSize(uint64(len(value))) + len(value)
}

func appendPartitionOwnershipConsensusWireString(payload []byte, value string) []byte {
	payload = appendPartitionOwnershipConsensusWireUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readPartitionOwnershipConsensusWireUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	var encoded [binary.MaxVarintLen64]byte
	if binary.PutUvarint(encoded[:], value) != size {
		return 0, false
	}
	*offset += size
	return value, true
}

func readPartitionOwnershipConsensusWireString(payload []byte, offset *int) (string, bool) {
	length, ok := readPartitionOwnershipConsensusWireUvarint(payload, offset)
	if !ok || length > maxPartitionOwnershipConsensusWireString || length > uint64(len(payload)-*offset) {
		return "", false
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), true
}
