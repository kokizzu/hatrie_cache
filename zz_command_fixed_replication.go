package hatriecache

import (
	"encoding/binary"
	"time"
)

func (ht *HatTrie) appendCommandDumpFixedEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	switch entry.Value.Type() {
	case DATAVALUE_TYPE_BLOOM_FILTER:
		data, err := appendCommandDumpBloomFilterBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.bloomFilters.array[entry.Value.Index])
		return data, true, err
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		data, err := appendCommandDumpCountMinSketchBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.countMinSketches.array[entry.Value.Index])
		return data, true, err
	case DATAVALUE_TYPE_HYPERLOGLOG:
		data, err := appendCommandDumpHyperLogLogBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.hyperLogLogs.array[entry.Value.Index])
		return data, true, err
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		data, err := appendCommandDumpCuckooFilterBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.cuckooFilters.array[entry.Value.Index])
		return data, true, err
	case DATAVALUE_TYPE_XOR_FILTER:
		filter := ht.xorFilters.array[entry.Value.Index]
		if filter.built {
			data, err := appendCommandDumpBuiltXorFilterBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), filter)
			return data, true, err
		}
	case DATAVALUE_TYPE_ROARING_BITMAP:
		data, err := appendCommandDumpRoaringBitmapBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.roaringBitmaps.array[entry.Value.Index])
		return data, true, err
	case DATAVALUE_TYPE_SPARSE_BITSET:
		data, err := appendCommandDumpSparseBitsetBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.sparseBitsets.array[entry.Value.Index])
		return data, true, err
	}
	return destination, false, nil
}

func appendCommandDumpBloomFilterBinary(destination []byte, expiresAt *time.Time, filter bloomFilterData) ([]byte, error) {
	snapshot := bloomFilterSnapshot{
		BitCount:   uint64(filter.bitCount),
		HashCount:  filter.hashCount,
		Insertions: filter.insertions,
	}
	rawSize := len(filter.words) * 8
	size, err := snapshotValueBinaryBloomFilterSize(snapshot, rawSize)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "bloom_filter", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryBloomFilter)
	writer.writeUvarint(snapshot.BitCount)
	writer.writeUvarint(uint64(snapshot.HashCount))
	writer.writeUvarint(snapshot.Insertions)
	writeSnapshotUint64Bytes(&writer.binaryFieldWriter, filter.words)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCommandDumpCountMinSketchBinary(destination []byte, expiresAt *time.Time, sketch countMinSketchData) ([]byte, error) {
	snapshot := countMinSketchSnapshot{
		Width:      sketch.width,
		Depth:      sketch.depth,
		TotalCount: sketch.total,
	}
	rawSize := len(sketch.counters) * 4
	size, err := snapshotValueBinaryCountMinSketchSize(snapshot, rawSize)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "count_min_sketch", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryCountMinSketch)
	writer.writeUvarint(snapshot.Width)
	writer.writeUvarint(uint64(snapshot.Depth))
	writer.writeUvarint(snapshot.TotalCount)
	writeSnapshotUint32Bytes(&writer.binaryFieldWriter, sketch.counters)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCommandDumpHyperLogLogBinary(destination []byte, expiresAt *time.Time, hll hyperLogLogData) ([]byte, error) {
	snapshot := hyperLogLogSnapshot{
		Precision:    hll.precision,
		Observations: hll.observations,
	}
	size, err := snapshotValueBinaryHyperLogLogSize(snapshot, len(hll.registers))
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "hyperloglog", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryHyperLogLog(&writer.binaryFieldWriter, snapshot, hll.registers)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCommandDumpCuckooFilterBinary(destination []byte, expiresAt *time.Time, filter cuckooFilterData) ([]byte, error) {
	snapshot := cuckooFilterSnapshot{
		BucketCount:     filter.bucketCount,
		BucketSize:      cuckooFilterBucketSize,
		FingerprintBits: filter.fingerprintBits,
		Count:           filter.count,
	}
	rawSize := len(filter.fingerprints) * 2
	size, err := snapshotValueBinaryCuckooFilterSize(snapshot, rawSize)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "cuckoo_filter", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryCuckooFilter)
	writer.writeUvarint(snapshot.BucketCount)
	writer.writeUvarint(uint64(snapshot.BucketSize))
	writer.writeUvarint(uint64(snapshot.FingerprintBits))
	writer.writeUvarint(snapshot.Count)
	writeSnapshotUint16Bytes(&writer.binaryFieldWriter, filter.fingerprints)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCommandDumpBuiltXorFilterBinary(destination []byte, expiresAt *time.Time, filter xorFilterData) ([]byte, error) {
	snapshot := xorFilterSnapshot{
		ExpectedItems: filter.expectedItems,
		Built:         true,
		Items:         filter.items,
		Seed:          filter.seed,
		BlockLength:   filter.blockLength,
	}
	size, err := snapshotValueBinaryXorFilterSize(snapshot, len(filter.fingerprints))
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "xor_filter", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryXorFilter(&writer.binaryFieldWriter, snapshot, filter.fingerprints)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCommandDumpRoaringBitmapBinary(destination []byte, expiresAt *time.Time, bitmap roaringBitmapData) ([]byte, error) {
	size, err := liveRoaringBitmapBinarySize(bitmap)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "roaring_bitmap", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryRoaringBitmap)
	writer.writeUvarint(bitmap.count)
	writer.writeUvarint(uint64(len(bitmap.containers)))
	for index := range bitmap.containers {
		container := &bitmap.containers[index]
		writer.writeUvarint(uint64(container.key))
		if container.isBitmap() {
			writer.buf = append(writer.buf, snapshotRoaringBitmapBinaryBits)
			writer.writeUvarint(uint64(container.cardinality))
			writeSnapshotUint64Bytes(&writer.binaryFieldWriter, container.bits[:])
			continue
		}
		writer.buf = append(writer.buf, snapshotRoaringBitmapBinaryArray)
		writer.writeUvarint(uint64(container.cardinality))
		writeSnapshotUint16Bytes(&writer.binaryFieldWriter, container.values)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func liveRoaringBitmapBinarySize(bitmap roaringBitmapData) (int, error) {
	total := 1 + binaryUvarintSize(bitmap.count) + binaryUvarintSize(uint64(len(bitmap.containers)))
	for index := range bitmap.containers {
		container := &bitmap.containers[index]
		rawSize := len(container.values) * 2
		if container.isBitmap() {
			rawSize = len(container.bits) * 8
		}
		payloadSize, err := snapshotValueBinaryBytesSize(rawSize)
		if err != nil {
			return 0, err
		}
		itemSize := binaryUvarintSize(uint64(container.key)) +
			1 +
			binaryUvarintSize(uint64(container.cardinality)) +
			payloadSize
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func appendCommandDumpSparseBitsetBinary(destination []byte, expiresAt *time.Time, bitset sparseBitsetData) ([]byte, error) {
	size, err := liveSparseBitsetBinarySize(bitset)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "sparse_bitset", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writer.buf = append(writer.buf, snapshotValueBinarySparseBitset)
	writer.writeUvarint(bitset.count)
	writer.writeUvarint(uint64(len(bitset.containers)))
	for index := range bitset.containers {
		container := &bitset.containers[index]
		writer.writeUvarint(container.key)
		if container.isBitmap() {
			writer.buf = append(writer.buf, snapshotSparseBitsetBinaryBits)
			writer.writeUvarint(uint64(container.cardinality))
			writeSnapshotUint64Bytes(&writer.binaryFieldWriter, container.bits[:])
			continue
		}
		writer.buf = append(writer.buf, snapshotSparseBitsetBinaryArray)
		writer.writeUvarint(uint64(container.cardinality))
		values := container.values
		if values == nil {
			values = container.inline[:container.cardinality]
		}
		writeSnapshotUint16Bytes(&writer.binaryFieldWriter, values)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func liveSparseBitsetBinarySize(bitset sparseBitsetData) (int, error) {
	total := 1 + binaryUvarintSize(bitset.count) + binaryUvarintSize(uint64(len(bitset.containers)))
	for index := range bitset.containers {
		container := &bitset.containers[index]
		rawSize := len(container.values) * 2
		if container.isBitmap() {
			rawSize = len(container.bits) * 8
		} else if container.values == nil {
			rawSize = int(container.cardinality) * 2
		}
		payloadSize, err := snapshotValueBinaryBytesSize(rawSize)
		if err != nil {
			return 0, err
		}
		itemSize := binaryUvarintSize(container.key) +
			1 +
			binaryUvarintSize(uint64(container.cardinality)) +
			payloadSize
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func writeSnapshotUint64Bytes(writer *binaryFieldWriter, values []uint64) {
	writer.writeUvarint(uint64(len(values) * 8))
	start := len(writer.buf)
	writer.buf = writer.buf[:start+len(values)*8]
	for index, value := range values {
		binary.LittleEndian.PutUint64(writer.buf[start+index*8:], value)
	}
}

func writeSnapshotUint32Bytes(writer *binaryFieldWriter, values []uint32) {
	writer.writeUvarint(uint64(len(values) * 4))
	start := len(writer.buf)
	writer.buf = writer.buf[:start+len(values)*4]
	for index, value := range values {
		binary.LittleEndian.PutUint32(writer.buf[start+index*4:], value)
	}
}

func writeSnapshotUint16Bytes(writer *binaryFieldWriter, values []uint16) {
	writer.writeUvarint(uint64(len(values) * 2))
	start := len(writer.buf)
	writer.buf = writer.buf[:start+len(values)*2]
	for index, value := range values {
		binary.LittleEndian.PutUint16(writer.buf[start+index*2:], value)
	}
}
