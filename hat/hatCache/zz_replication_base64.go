package hatCache

func appendCanonicalReplicationValueBinaryDirect(destination []byte, entry snapshotEntry) ([]byte, error) {
	switch entry.Type {
	case "bloom_filter":
		if entry.BloomFilter != nil {
			return appendCanonicalReplicationBloomFilterValueBinary(destination, entry, *entry.BloomFilter)
		}
	case "count_min_sketch":
		if entry.CountMinSketch != nil {
			return appendCanonicalReplicationCountMinSketchValueBinary(destination, entry, *entry.CountMinSketch)
		}
	case "hyperloglog":
		if entry.HyperLogLog != nil {
			return appendCanonicalReplicationHyperLogLogValueBinary(destination, entry, *entry.HyperLogLog)
		}
	case "cuckoo_filter":
		if entry.CuckooFilter != nil {
			return appendCanonicalReplicationCuckooFilterValueBinary(destination, entry, *entry.CuckooFilter)
		}
	case "xor_filter":
		if entry.XorFilter != nil && entry.XorFilter.Built {
			return appendCanonicalReplicationBuiltXorFilterValueBinary(destination, entry, *entry.XorFilter)
		}
	case "roaring_bitmap":
		if entry.RoaringBitmap != nil {
			return appendCanonicalReplicationRoaringBitmapValueBinary(destination, entry, *entry.RoaringBitmap)
		}
	case "sparse_bitset":
		if entry.SparseBitset != nil {
			return appendCanonicalReplicationSparseBitsetValueBinary(destination, entry, *entry.SparseBitset)
		}
	}
	return appendReplicationValueBinary(destination, entry)
}

func appendCanonicalReplicationBloomFilterValueBinary(destination []byte, entry snapshotEntry, snapshot bloomFilterSnapshot) ([]byte, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Bits)
	if !ok {
		return appendReplicationValueBinary(destination, entry)
	}
	size, err := snapshotValueBinaryBloomFilterSize(snapshot, decodedSize)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	written, err := writeSnapshotValueBinaryBloomFilterBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil {
		return destination, err
	}
	if !written {
		return appendReplicationValueBinary(destination, entry)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCanonicalReplicationCountMinSketchValueBinary(destination []byte, entry snapshotEntry, snapshot countMinSketchSnapshot) ([]byte, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Counters)
	if !ok {
		return appendReplicationValueBinary(destination, entry)
	}
	size, err := snapshotValueBinaryCountMinSketchSize(snapshot, decodedSize)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	written, err := writeSnapshotValueBinaryCountMinSketchBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil {
		return destination, err
	}
	if !written {
		return appendReplicationValueBinary(destination, entry)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCanonicalReplicationHyperLogLogValueBinary(destination []byte, entry snapshotEntry, snapshot hyperLogLogSnapshot) ([]byte, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Registers)
	if !ok {
		return appendReplicationValueBinary(destination, entry)
	}
	size, err := snapshotValueBinaryHyperLogLogSize(snapshot, decodedSize)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	written, err := writeSnapshotValueBinaryHyperLogLogBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil {
		return destination, err
	}
	if !written {
		return appendReplicationValueBinary(destination, entry)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCanonicalReplicationCuckooFilterValueBinary(destination []byte, entry snapshotEntry, snapshot cuckooFilterSnapshot) ([]byte, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Fingerprints)
	if !ok {
		return appendReplicationValueBinary(destination, entry)
	}
	size, err := snapshotValueBinaryCuckooFilterSize(snapshot, decodedSize)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	written, err := writeSnapshotValueBinaryCuckooFilterBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil {
		return destination, err
	}
	if !written {
		return appendReplicationValueBinary(destination, entry)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCanonicalReplicationBuiltXorFilterValueBinary(destination []byte, entry snapshotEntry, snapshot xorFilterSnapshot) ([]byte, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Fingerprints)
	if !ok {
		return appendReplicationValueBinary(destination, entry)
	}
	size, err := snapshotValueBinaryXorFilterSize(snapshot, decodedSize)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	written, err := writeSnapshotValueBinaryXorFilterBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil {
		return destination, err
	}
	if !written {
		return appendReplicationValueBinary(destination, entry)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCanonicalReplicationRoaringBitmapValueBinary(destination []byte, entry snapshotEntry, snapshot roaringBitmapSnapshot) ([]byte, error) {
	size, ok := snapshotValueBinaryRoaringBitmapBase64Size(snapshot)
	if !ok {
		return appendReplicationValueBinary(destination, entry)
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	written, err := writeSnapshotValueBinaryRoaringBitmapBase64(&writer.binaryFieldWriter, snapshot)
	if err != nil {
		return destination, err
	}
	if !written {
		return appendReplicationValueBinary(destination, entry)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCanonicalReplicationSparseBitsetValueBinary(destination []byte, entry snapshotEntry, snapshot sparseBitsetSnapshot) ([]byte, error) {
	size, ok := snapshotValueBinarySparseBitsetBase64Size(snapshot)
	if !ok {
		return appendReplicationValueBinary(destination, entry)
	}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return appendReplicationValueBinary(destination, entry)
	}
	written, err := writeSnapshotValueBinarySparseBitsetBase64(&writer.binaryFieldWriter, snapshot)
	if err != nil {
		return destination, err
	}
	if !written {
		return appendReplicationValueBinary(destination, entry)
	}
	return finishReplicationDirectPayload(writer, entry), nil
}
