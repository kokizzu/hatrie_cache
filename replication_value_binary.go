package hatriecache

import (
	"bytes"
	"errors"
	"time"
)

var replicationValueBinaryMagic = []byte{'h', 'c', 'r', 'v', 1}

func replicationValueDataIsBinary(data []byte) bool {
	return bytes.HasPrefix(data, replicationValueBinaryMagic)
}

func marshalReplicationValueBinary(entry snapshotEntry) ([]byte, error) {
	return appendReplicationValueBinary(nil, entry)
}

func appendReplicationValueBinary(destination []byte, entry snapshotEntry) ([]byte, error) {
	switch entry.Type {
	case "string":
		return appendReplicationStringEntryBinary(destination, entry)
	case "map":
		return appendReplicationCollectionValueBinary(destination, entry, entry.Map)
	case "slice":
		return appendReplicationCollectionValueBinary(destination, entry, entry.Slice)
	case "set":
		return appendReplicationCollectionValueBinary(destination, entry, entry.Set)
	case "priority_queue":
		return appendReplicationPriorityQueueValueBinary(destination, entry)
	case "top_k":
		if entry.TopK == nil {
			return destination, errors.New("hatriecache: top-k snapshot is required")
		}
		return appendReplicationTopKValueBinary(destination, entry, *entry.TopK)
	case "radix_tree":
		if entry.RadixTree == nil {
			return destination, errors.New("hatriecache: radix tree snapshot is required")
		}
		return appendReplicationRadixTreeValueBinary(destination, entry, *entry.RadixTree)
	case "reservoir_sample":
		if entry.ReservoirSample == nil {
			return destination, errors.New("hatriecache: reservoir sample snapshot is required")
		}
		return appendReplicationReservoirSampleValueBinary(destination, entry, *entry.ReservoirSample)
	case "xor_filter":
		if entry.XorFilter == nil {
			return destination, errors.New("hatriecache: xor filter snapshot is required")
		}
		if !entry.XorFilter.Built {
			return appendReplicationStagedXorFilterValueBinary(destination, entry, *entry.XorFilter)
		}
		return appendReplicationBuiltXorFilterValueBinary(destination, entry, *entry.XorFilter)
	case "bloom_filter":
		if entry.BloomFilter == nil {
			return destination, errors.New("hatriecache: bloom filter snapshot is required")
		}
		return appendReplicationBloomFilterValueBinary(destination, entry, *entry.BloomFilter)
	case "count_min_sketch":
		if entry.CountMinSketch == nil {
			return destination, errors.New("hatriecache: count-min sketch snapshot is required")
		}
		return appendReplicationCountMinSketchValueBinary(destination, entry, *entry.CountMinSketch)
	case "hyperloglog":
		if entry.HyperLogLog == nil {
			return destination, errors.New("hatriecache: hyperloglog snapshot is required")
		}
		return appendReplicationHyperLogLogValueBinary(destination, entry, *entry.HyperLogLog)
	case "cuckoo_filter":
		if entry.CuckooFilter == nil {
			return destination, errors.New("hatriecache: cuckoo filter snapshot is required")
		}
		return appendReplicationCuckooFilterValueBinary(destination, entry, *entry.CuckooFilter)
	case "roaring_bitmap":
		if entry.RoaringBitmap == nil {
			return destination, errors.New("hatriecache: roaring bitmap snapshot is required")
		}
		return appendReplicationRoaringBitmapValueBinary(destination, entry, *entry.RoaringBitmap)
	case "sparse_bitset":
		if entry.SparseBitset == nil {
			return destination, errors.New("hatriecache: sparse bitset snapshot is required")
		}
		return appendReplicationSparseBitsetValueBinary(destination, entry, *entry.SparseBitset)
	case "fenwick_tree":
		if entry.FenwickTree == nil {
			return destination, errors.New("hatriecache: fenwick tree snapshot is required")
		}
		return appendReplicationFenwickTreeValueBinary(destination, entry, *entry.FenwickTree)
	case "quantile_sketch":
		if entry.QuantileSketch == nil {
			return destination, errors.New("hatriecache: quantile sketch snapshot is required")
		}
		return appendReplicationQuantileSketchValueBinary(destination, entry, *entry.QuantileSketch)
	}
	value, err := prepareLevelDBBinaryEntryValue(entry)
	if err != nil {
		return destination, err
	}
	capacity, err := replicationValueBinaryCapacity(entry.Type, value.encodedSize, entry.ExpiresAt)
	if err != nil {
		return destination, err
	}
	destination = growBinaryAppendBuffer(destination, capacity)
	writer := levelDBBinaryWriter{binaryFieldWriter: binaryFieldWriter{buf: destination}}
	writer.buf = append(writer.buf, replicationValueBinaryMagic...)
	writer.writeString(entry.Type)
	writer.writePreparedSnapshotEntryValue(value)
	writer.writeTimePtr(entry.ExpiresAt)
	return writer.bytes(), nil
}

func appendCanonicalReplicationValueBinary(destination []byte, entry snapshotEntry) ([]byte, error) {
	return appendCanonicalReplicationValueBinaryDirect(destination, entry)
}

func appendReplicationStringEntryBinary(destination []byte, entry snapshotEntry) ([]byte, error) {
	valueSize, err := binaryLengthPrefixedSize(int64(len(entry.String)))
	if err != nil {
		return destination, err
	}
	capacity, err := replicationValueBinaryCapacity(entry.Type, valueSize, entry.ExpiresAt)
	if err != nil {
		return destination, err
	}
	destination = growBinaryAppendBuffer(destination, capacity)
	writer := levelDBBinaryWriter{binaryFieldWriter: binaryFieldWriter{buf: destination}}
	writer.buf = append(writer.buf, replicationValueBinaryMagic...)
	writer.writeString(entry.Type)
	writer.writeString(entry.String)
	writer.writeTimePtr(entry.ExpiresAt)
	return writer.bytes(), nil
}

func appendReplicationCollectionValueBinary(destination []byte, entry snapshotEntry, value interface{}) ([]byte, error) {
	prepared, _, err := prepareSnapshotDynamicValueBinary(value)
	if err != nil {
		return destination, err
	}
	size, ok, err := snapshotValueBinarySize(prepared)
	if err != nil {
		return destination, err
	}
	if !ok {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinary(&writer.binaryFieldWriter, prepared) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationPriorityQueueValueBinary(destination []byte, entry snapshotEntry) ([]byte, error) {
	prepared, err := prepareSnapshotPriorityQueueItemsBinary(entry.PriorityQueue)
	if err != nil {
		return destination, err
	}
	size, ok, err := snapshotValueBinaryPriorityQueueSize(prepared)
	if err != nil {
		return destination, err
	}
	if !ok {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinaryPriorityQueue(&writer.binaryFieldWriter, prepared) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationTopKValueBinary(destination []byte, entry snapshotEntry, snapshot topKSnapshot) ([]byte, error) {
	prepared, err := prepareSnapshotTopKBinary(snapshot)
	if err != nil {
		return destination, err
	}
	size, ok, err := snapshotValueBinaryTopKSize(prepared)
	if err != nil {
		return destination, err
	}
	if !ok {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinaryTopK(&writer.binaryFieldWriter, prepared) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationRadixTreeValueBinary(destination []byte, entry snapshotEntry, snapshot radixTreeSnapshot) ([]byte, error) {
	prepared, err := prepareSnapshotRadixTreeBinary(snapshot)
	if err != nil {
		return destination, err
	}
	size, ok, err := snapshotValueBinaryRadixTreeSize(prepared)
	if err != nil {
		return destination, err
	}
	if !ok {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinaryRadixTree(&writer.binaryFieldWriter, prepared) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationReservoirSampleValueBinary(destination []byte, entry snapshotEntry, snapshot reservoirSampleSnapshot) ([]byte, error) {
	prepared, err := prepareSnapshotReservoirSampleBinary(snapshot)
	if err != nil {
		return destination, err
	}
	size, ok, err := snapshotValueBinaryReservoirSampleSize(prepared)
	if err != nil {
		return destination, err
	}
	if !ok {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinaryReservoirSample(&writer.binaryFieldWriter, prepared) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationStagedXorFilterValueBinary(destination []byte, entry snapshotEntry, snapshot xorFilterSnapshot) ([]byte, error) {
	prepared, err := prepareSnapshotStagedXorFilterBinary(snapshot)
	if err != nil {
		return destination, err
	}
	size, ok, err := snapshotValueBinaryStagedXorFilterSize(prepared)
	if err != nil {
		return destination, err
	}
	if !ok {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinaryStagedXorFilter(&writer.binaryFieldWriter, prepared) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationBloomFilterValueBinary(destination []byte, entry snapshotEntry, snapshot bloomFilterSnapshot) ([]byte, error) {
	bits, err := snapshotBloomFilterRawBits(snapshot)
	if err != nil {
		return destination, err
	}
	size, err := snapshotValueBinaryBloomFilterSize(snapshot, len(bits))
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryBloomFilter(&writer.binaryFieldWriter, snapshot, bits)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationCountMinSketchValueBinary(destination []byte, entry snapshotEntry, snapshot countMinSketchSnapshot) ([]byte, error) {
	counters, err := snapshotCountMinSketchRawCounters(snapshot)
	if err != nil {
		return destination, err
	}
	size, err := snapshotValueBinaryCountMinSketchSize(snapshot, len(counters))
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryCountMinSketch(&writer.binaryFieldWriter, snapshot, counters)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationHyperLogLogValueBinary(destination []byte, entry snapshotEntry, snapshot hyperLogLogSnapshot) ([]byte, error) {
	registers, err := snapshotHyperLogLogRawRegisters(snapshot)
	if err != nil {
		return destination, err
	}
	size, err := snapshotValueBinaryHyperLogLogSize(snapshot, len(registers))
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryHyperLogLog(&writer.binaryFieldWriter, snapshot, registers)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationCuckooFilterValueBinary(destination []byte, entry snapshotEntry, snapshot cuckooFilterSnapshot) ([]byte, error) {
	fingerprints, err := snapshotCuckooFilterRawFingerprints(snapshot)
	if err != nil {
		return destination, err
	}
	size, err := snapshotValueBinaryCuckooFilterSize(snapshot, len(fingerprints))
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryCuckooFilter(&writer.binaryFieldWriter, snapshot, fingerprints)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationBuiltXorFilterValueBinary(destination []byte, entry snapshotEntry, snapshot xorFilterSnapshot) ([]byte, error) {
	fingerprints, err := snapshotXorFilterRawFingerprints(snapshot)
	if err != nil {
		return destination, err
	}
	size, err := snapshotValueBinaryXorFilterSize(snapshot, len(fingerprints))
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryXorFilter(&writer.binaryFieldWriter, snapshot, fingerprints)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationRoaringBitmapValueBinary(destination []byte, entry snapshotEntry, snapshot roaringBitmapSnapshot) ([]byte, error) {
	containers, err := prepareSnapshotRoaringBitmapBinaryContainers(snapshot)
	if err != nil {
		return destination, err
	}
	size, err := snapshotValueBinaryRoaringBitmapSize(snapshot, containers)
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryRoaringBitmap(&writer.binaryFieldWriter, snapshot, containers)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationSparseBitsetValueBinary(destination []byte, entry snapshotEntry, snapshot sparseBitsetSnapshot) ([]byte, error) {
	containers, err := prepareSnapshotSparseBitsetBinaryContainers(snapshot)
	if err != nil {
		return destination, err
	}
	size, err := snapshotValueBinarySparseBitsetSize(snapshot, containers)
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinarySparseBitset(&writer.binaryFieldWriter, snapshot, containers)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationFenwickTreeValueBinary(destination []byte, entry snapshotEntry, snapshot fenwickTreeSnapshot) ([]byte, error) {
	size, err := snapshotValueBinaryFenwickTreeSize(snapshot)
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryFenwickTree(&writer.binaryFieldWriter, snapshot)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendReplicationQuantileSketchValueBinary(destination []byte, entry snapshotEntry, snapshot quantileSketchSnapshot) ([]byte, error) {
	size, err := snapshotValueBinaryQuantileSketchSize(snapshot)
	if err != nil {
		return destination, err
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryQuantileSketch(&writer.binaryFieldWriter, snapshot)
	return finishReplicationDirectPayload(writer, entry), nil
}

func newReplicationDirectPayloadWriter(destination []byte, entry snapshotEntry, valueSize int) (levelDBBinaryWriter, error) {
	payloadSize, err := snapshotValueBinaryAdd(len(snapshotValueBinaryMagic), valueSize)
	if err != nil {
		return levelDBBinaryWriter{}, err
	}
	encodedSize, err := binaryLengthPrefixedSize(int64(payloadSize))
	if err != nil {
		return levelDBBinaryWriter{}, err
	}
	capacity, err := replicationValueBinaryCapacity(entry.Type, encodedSize, entry.ExpiresAt)
	if err != nil {
		return levelDBBinaryWriter{}, err
	}
	destination = growBinaryAppendBuffer(destination, capacity)
	writer := levelDBBinaryWriter{binaryFieldWriter: binaryFieldWriter{buf: destination}}
	writer.buf = append(writer.buf, replicationValueBinaryMagic...)
	writer.writeString(entry.Type)
	writer.writeUvarint(uint64(payloadSize))
	writer.buf = append(writer.buf, snapshotValueBinaryMagic...)
	return writer, nil
}

func finishReplicationDirectPayload(writer levelDBBinaryWriter, entry snapshotEntry) []byte {
	writer.writeTimePtr(entry.ExpiresAt)
	return writer.bytes()
}

func growBinaryAppendBuffer(destination []byte, additional int) []byte {
	if additional <= cap(destination)-len(destination) {
		return destination
	}
	needed := len(destination) + additional
	capacity := cap(destination) * 2
	if capacity < needed {
		capacity = needed
	}
	if capacity == 0 {
		capacity = additional
	}
	grown := make([]byte, len(destination), capacity)
	copy(grown, destination)
	return grown
}

func marshalReplicationStringValueBinary(value string) ([]byte, error) {
	return marshalReplicationValueBinary(snapshotEntry{Type: "string", String: value})
}

func replicationValueBinaryCapacity(entryType string, encodedValueBytes int64, expiresAt *time.Time) (int, error) {
	if encodedValueBytes < 0 {
		return 0, errLevelDBBinaryRecordTooLarge
	}
	typeSize, err := binaryLengthPrefixedSize(int64(len(entryType)))
	if err != nil {
		return 0, err
	}
	total := int64(len(replicationValueBinaryMagic))
	for _, size := range []int64{typeSize, encodedValueBytes, int64(levelDBBinaryTimePtrSize(expiresAt))} {
		total, err = addLevelDBBinaryRecordSize(total, size)
		if err != nil {
			return 0, err
		}
	}
	return int(total), nil
}

func unmarshalReplicationValueBinary(key string, data []byte) (snapshotEntry, error) {
	if !replicationValueDataIsBinary(data) {
		return snapshotEntry{}, errors.New("hatriecache: invalid binary replication value")
	}
	reader := levelDBBinaryReader{binaryFieldReader: newBinaryFieldReader(data[len(replicationValueBinaryMagic):])}
	entryType, err := reader.readString()
	if err != nil {
		return snapshotEntry{}, err
	}
	entry := snapshotEntry{Key: key, Type: entryType}
	if err := reader.readSnapshotEntryValue(&entry); err != nil {
		return snapshotEntry{}, err
	}
	entry.ExpiresAt, err = reader.readTimePtr()
	if err != nil {
		return snapshotEntry{}, err
	}
	if !reader.done() {
		return snapshotEntry{}, errors.New("hatriecache: invalid trailing binary replication value data")
	}
	return entry, nil
}
