package hatriecache

import "encoding/base64"

func marshalLevelDBBloomFilterEntryBase64(entry snapshotEntry, snapshot bloomFilterSnapshot) ([]byte, bool, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Bits)
	if !ok {
		return nil, false, nil
	}
	size, err := snapshotValueBinaryBloomFilterSize(snapshot, decodedSize)
	if err != nil {
		return nil, false, nil
	}
	writer, err := newLevelDBDirectPayloadWriter(entry, size)
	if err != nil {
		return nil, false, nil
	}
	written, err := writeSnapshotValueBinaryBloomFilterBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil || !written {
		return nil, err != nil, err
	}
	return finishLevelDBDirectPayload(writer, entry), true, nil
}

func marshalLevelDBCountMinSketchEntryBase64(entry snapshotEntry, snapshot countMinSketchSnapshot) ([]byte, bool, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Counters)
	if !ok {
		return nil, false, nil
	}
	size, err := snapshotValueBinaryCountMinSketchSize(snapshot, decodedSize)
	if err != nil {
		return nil, false, nil
	}
	writer, err := newLevelDBDirectPayloadWriter(entry, size)
	if err != nil {
		return nil, false, nil
	}
	written, err := writeSnapshotValueBinaryCountMinSketchBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil || !written {
		return nil, err != nil, err
	}
	return finishLevelDBDirectPayload(writer, entry), true, nil
}

func marshalLevelDBHyperLogLogEntryBase64(entry snapshotEntry, snapshot hyperLogLogSnapshot) ([]byte, bool, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Registers)
	if !ok {
		return nil, false, nil
	}
	size, err := snapshotValueBinaryHyperLogLogSize(snapshot, decodedSize)
	if err != nil {
		return nil, false, nil
	}
	writer, err := newLevelDBDirectPayloadWriter(entry, size)
	if err != nil {
		return nil, false, nil
	}
	written, err := writeSnapshotValueBinaryHyperLogLogBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil || !written {
		return nil, err != nil, err
	}
	return finishLevelDBDirectPayload(writer, entry), true, nil
}

func marshalLevelDBCuckooFilterEntryBase64(entry snapshotEntry, snapshot cuckooFilterSnapshot) ([]byte, bool, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Fingerprints)
	if !ok {
		return nil, false, nil
	}
	size, err := snapshotValueBinaryCuckooFilterSize(snapshot, decodedSize)
	if err != nil {
		return nil, false, nil
	}
	writer, err := newLevelDBDirectPayloadWriter(entry, size)
	if err != nil {
		return nil, false, nil
	}
	written, err := writeSnapshotValueBinaryCuckooFilterBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil || !written {
		return nil, err != nil, err
	}
	return finishLevelDBDirectPayload(writer, entry), true, nil
}

func marshalLevelDBBuiltXorFilterEntryBase64(entry snapshotEntry, snapshot xorFilterSnapshot) ([]byte, bool, error) {
	decodedSize, ok := snapshotBase64DecodedSizeHint(snapshot.Fingerprints)
	if !ok {
		return nil, false, nil
	}
	size, err := snapshotValueBinaryXorFilterSize(snapshot, decodedSize)
	if err != nil {
		return nil, false, nil
	}
	writer, err := newLevelDBDirectPayloadWriter(entry, size)
	if err != nil {
		return nil, false, nil
	}
	written, err := writeSnapshotValueBinaryXorFilterBase64(&writer.binaryFieldWriter, snapshot, decodedSize)
	if err != nil || !written {
		return nil, err != nil, err
	}
	return finishLevelDBDirectPayload(writer, entry), true, nil
}

func writeSnapshotValueBinaryBloomFilterBase64(writer *binaryFieldWriter, snapshot bloomFilterSnapshot, decodedSize int) (bool, error) {
	writer.buf = append(writer.buf, snapshotValueBinaryBloomFilter)
	writer.writeUvarint(snapshot.BitCount)
	writer.writeUvarint(uint64(snapshot.HashCount))
	writer.writeUvarint(snapshot.Insertions)
	return writeSnapshotValueBinaryBase64(writer, snapshot.Bits, decodedSize)
}

func writeSnapshotValueBinaryCountMinSketchBase64(writer *binaryFieldWriter, snapshot countMinSketchSnapshot, decodedSize int) (bool, error) {
	writer.buf = append(writer.buf, snapshotValueBinaryCountMinSketch)
	writer.writeUvarint(snapshot.Width)
	writer.writeUvarint(uint64(snapshot.Depth))
	writer.writeUvarint(snapshot.TotalCount)
	return writeSnapshotValueBinaryBase64(writer, snapshot.Counters, decodedSize)
}

func writeSnapshotValueBinaryHyperLogLogBase64(writer *binaryFieldWriter, snapshot hyperLogLogSnapshot, decodedSize int) (bool, error) {
	writer.buf = append(writer.buf, snapshotValueBinaryHyperLogLog)
	writer.writeUvarint(uint64(snapshot.Precision))
	writer.writeUvarint(snapshot.Observations)
	return writeSnapshotValueBinaryBase64(writer, snapshot.Registers, decodedSize)
}

func writeSnapshotValueBinaryCuckooFilterBase64(writer *binaryFieldWriter, snapshot cuckooFilterSnapshot, decodedSize int) (bool, error) {
	writer.buf = append(writer.buf, snapshotValueBinaryCuckooFilter)
	writer.writeUvarint(snapshot.BucketCount)
	writer.writeUvarint(uint64(snapshot.BucketSize))
	writer.writeUvarint(uint64(snapshot.FingerprintBits))
	writer.writeUvarint(snapshot.Count)
	return writeSnapshotValueBinaryBase64(writer, snapshot.Fingerprints, decodedSize)
}

func writeSnapshotValueBinaryXorFilterBase64(writer *binaryFieldWriter, snapshot xorFilterSnapshot, decodedSize int) (bool, error) {
	writer.buf = append(writer.buf, snapshotValueBinaryXorFilter)
	writer.writeUvarint(snapshot.ExpectedItems)
	writer.writeUvarint(snapshot.Items)
	writer.writeUvarint(snapshot.Seed)
	writer.writeUvarint(uint64(snapshot.BlockLength))
	return writeSnapshotValueBinaryBase64(writer, snapshot.Fingerprints, decodedSize)
}

func writeSnapshotValueBinaryBase64(writer *binaryFieldWriter, encoded string, decodedSize int) (bool, error) {
	start := len(writer.buf)
	writer.writeUvarint(uint64(decodedSize))
	payloadStart := len(writer.buf)
	writer.buf = writer.buf[:payloadStart+decodedSize]
	written, err := base64.StdEncoding.Decode(writer.buf[payloadStart:], []byte(encoded))
	if err != nil {
		writer.buf = writer.buf[:start]
		return false, err
	}
	if written != decodedSize {
		writer.buf = writer.buf[:start]
		return false, nil
	}
	return true, nil
}

func snapshotBase64DecodedSizeHint(encoded string) (int, bool) {
	if len(encoded)%4 != 0 {
		return 0, false
	}
	padding := 0
	if len(encoded) > 0 && encoded[len(encoded)-1] == '=' {
		padding++
		if len(encoded) > 1 && encoded[len(encoded)-2] == '=' {
			padding++
		}
	}
	return len(encoded)/4*3 - padding, true
}
