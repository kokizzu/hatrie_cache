package hatDataStructure

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	tdigestAggregateStateVersion  uint64 = 1
	tdigestAggregateStateHeader          = 16
	tdigestAggregateStateCentroid        = 16
)

// MarshalAggregateState encodes the digest as a bounded HAG1 envelope with a
// fixed-width centroid payload. It avoids JSON object and float formatting
// overhead while retaining the envelope's kind, version, length, and CRC.
func (digest TDigest) MarshalAggregateState() ([]byte, error) {
	snapshot := digest.Snapshot()
	if err := ValidateTDigestSnapshot(snapshot); err != nil {
		return nil, err
	}
	if len(snapshot.Centroids) > (MaxAggregateStateEnvelopeBytes-tdigestAggregateStateHeader)/tdigestAggregateStateCentroid {
		return nil, fmt.Errorf("hatriecache: t-digest aggregate state is too large")
	}
	payloadLength := tdigestAggregateStateHeader + len(snapshot.Centroids)*tdigestAggregateStateCentroid
	return MarshalAggregateStateEnvelopeWithPayload(
		AggregateStateKindTDigest,
		tdigestAggregateStateVersion,
		payloadLength,
		func(encoded []byte) []byte {
			var header [tdigestAggregateStateHeader]byte
			binary.LittleEndian.PutUint32(header[0:4], snapshot.Compression)
			binary.LittleEndian.PutUint64(header[4:12], snapshot.Count)
			binary.LittleEndian.PutUint32(header[12:16], uint32(len(snapshot.Centroids)))
			encoded = append(encoded, header[:]...)
			for _, centroid := range snapshot.Centroids {
				var item [tdigestAggregateStateCentroid]byte
				binary.LittleEndian.PutUint64(item[0:8], math.Float64bits(centroid.Mean))
				binary.LittleEndian.PutUint64(item[8:16], centroid.Count)
				encoded = append(encoded, item[:]...)
			}
			return encoded
		},
	)
}

// NewTDigestFromAggregateState decodes and validates one compact HAG1 digest.
func NewTDigestFromAggregateState(data []byte) (TDigest, error) {
	envelope, err := UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return TDigest{}, err
	}
	if envelope.Kind != AggregateStateKindTDigest {
		return TDigest{}, fmt.Errorf("%w: got %q want %q", ErrAggregateStateKindMismatch, envelope.Kind, AggregateStateKindTDigest)
	}
	if envelope.Version != tdigestAggregateStateVersion {
		return TDigest{}, fmt.Errorf("%w: got %d want %d", ErrAggregateStateVersionUnsupported, envelope.Version, tdigestAggregateStateVersion)
	}
	payload := envelope.Payload
	if len(payload) < tdigestAggregateStateHeader || (len(payload)-tdigestAggregateStateHeader)%tdigestAggregateStateCentroid != 0 {
		return TDigest{}, fmt.Errorf("%w: invalid t-digest payload length %d", ErrAggregateStateEnvelopeWire, len(payload))
	}
	centroidCount := binary.LittleEndian.Uint32(payload[12:16])
	available := (len(payload) - tdigestAggregateStateHeader) / tdigestAggregateStateCentroid
	if uint64(centroidCount) != uint64(available) {
		return TDigest{}, fmt.Errorf("%w: centroid count %d does not match payload", ErrAggregateStateEnvelopeWire, centroidCount)
	}
	snapshot := TDigestSnapshot{
		Compression: binary.LittleEndian.Uint32(payload[0:4]),
		Count:       binary.LittleEndian.Uint64(payload[4:12]),
		Centroids:   make([]TDigestCentroid, available),
	}
	for index := range snapshot.Centroids {
		offset := tdigestAggregateStateHeader + index*tdigestAggregateStateCentroid
		snapshot.Centroids[index] = TDigestCentroid{
			Mean:  math.Float64frombits(binary.LittleEndian.Uint64(payload[offset : offset+8])),
			Count: binary.LittleEndian.Uint64(payload[offset+8 : offset+16]),
		}
	}
	digest, err := NewTDigestFromSnapshot(snapshot)
	if err != nil {
		return TDigest{}, fmt.Errorf("%w: %v", ErrAggregateStateEnvelopeInvalid, err)
	}
	return digest, nil
}

// MergeAggregateState decodes and merges one compact digest atomically. The
// receiver is unchanged when decoding or compression validation fails.
func (digest *TDigest) MergeAggregateState(data []byte) error {
	other, err := NewTDigestFromAggregateState(data)
	if err != nil {
		return err
	}
	return digest.Merge(other)
}
