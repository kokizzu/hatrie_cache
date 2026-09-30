package hatDataStructure

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func marshalAggregateStateEnvelopeInto(dst []byte, kind string, version uint64, payload []byte) ([]byte, error) {
	if err := validateAggregateStateMetadata(kind, version); err != nil {
		return nil, err
	}
	if len(payload) > MaxAggregateStatePayloadBytes {
		return nil, ErrAggregateStateEnvelopeLimit
	}
	capacity := len(aggregateStateEnvelopeMagic) + 1 + aggregateStateUvarintLen(uint64(len(kind))) + len(kind) + aggregateStateUvarintLen(version) + aggregateStateUvarintLen(uint64(len(payload))) + len(payload) + 4
	if capacity > MaxAggregateStateEnvelopeBytes {
		return nil, ErrAggregateStateEnvelopeLimit
	}
	if cap(dst) < capacity {
		dst = make([]byte, 0, capacity)
	} else {
		dst = dst[:0]
	}
	dst = append(dst, aggregateStateEnvelopeMagic[:]...)
	dst = append(dst, AggregateStateEnvelopeWireVersion)
	dst = appendAggregateStateUvarint(dst, uint64(len(kind)))
	dst = append(dst, kind...)
	dst = appendAggregateStateUvarint(dst, version)
	dst = appendAggregateStateUvarint(dst, uint64(len(payload)))
	dst = append(dst, payload...)
	if len(dst)+4 > MaxAggregateStateEnvelopeBytes {
		return nil, ErrAggregateStateEnvelopeLimit
	}
	var checksum [4]byte
	binary.LittleEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(dst))
	dst = append(dst, checksum[:]...)
	if len(dst) != capacity {
		return nil, fmt.Errorf("%w: encoded envelope length %d, want %d", ErrAggregateStateEnvelopeInvalid, len(dst), capacity)
	}
	return dst, nil
}
