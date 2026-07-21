package hatriecache

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	json "github.com/goccy/go-json"
)

var (
	snapshotValueBinaryPrefix  = []byte{'h', 'c', 'v', 'b'}
	snapshotValueBinaryMagicV1 = []byte{'h', 'c', 'v', 'b', 1}
	snapshotValueBinaryMagic   = []byte{'h', 'c', 'v', 'b', 2}
)

const (
	snapshotValueBinaryNull byte = iota
	snapshotValueBinaryFalse
	snapshotValueBinaryTrue
	snapshotValueBinaryString
	snapshotValueBinaryNumber
	snapshotValueBinaryArray
	snapshotValueBinaryObject
	snapshotValueBinaryPriorityQueue
	snapshotValueBinaryRadixTree
	snapshotValueBinaryBloomFilter
	snapshotValueBinaryCountMinSketch
	snapshotValueBinaryHyperLogLog
	snapshotValueBinaryCuckooFilter
	snapshotValueBinaryXorFilter
	snapshotValueBinaryRoaringBitmap
	snapshotValueBinarySparseBitset
	snapshotValueBinaryFenwickTree
	snapshotValueBinaryQuantileSketch
	snapshotValueBinaryTopK
	snapshotValueBinaryReservoirSample
	snapshotValueBinarySigned
	snapshotValueBinaryUnsigned
	snapshotValueBinaryBytes
	snapshotValueBinaryStagedXorFilter
)

var errSnapshotValueBinaryTooLarge = errors.New("hatriecache: binary snapshot value is too large")

const snapshotValueBinaryMaxInitialCapacity = 4096

func snapshotValueDataIsBinary(data []byte) bool {
	_, ok := snapshotValueBinaryPayload(data)
	return ok
}

func snapshotValueBinaryPayload(data []byte) ([]byte, bool) {
	if len(data) < len(snapshotValueBinaryPrefix)+1 || !bytes.Equal(data[:len(snapshotValueBinaryPrefix)], snapshotValueBinaryPrefix) {
		return nil, false
	}
	switch data[len(snapshotValueBinaryPrefix)] {
	case snapshotValueBinaryMagicV1[len(snapshotValueBinaryPrefix)], snapshotValueBinaryMagic[len(snapshotValueBinaryPrefix)]:
		return data[len(snapshotValueBinaryPrefix)+1:], true
	default:
		return nil, false
	}
}

func marshalSnapshotCollectionValueBinary(value interface{}) ([]byte, bool, error) {
	prepared, _, err := prepareSnapshotDynamicValueBinary(value)
	if err != nil {
		return nil, true, err
	}
	value = prepared
	size, ok, err := snapshotValueBinarySize(value)
	if err != nil || !ok {
		return nil, ok, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	if ok := writeSnapshotValueBinary(&writer, value); !ok {
		return nil, false, nil
	}
	return writer.bytes(), true, nil
}

func marshalSnapshotPriorityQueueValueBinary(items []priorityQueueItem) ([]byte, bool, error) {
	prepared, err := prepareSnapshotPriorityQueueItemsBinary(items)
	if err != nil {
		return nil, true, err
	}
	items = prepared
	size, ok, err := snapshotValueBinaryPriorityQueueSize(items)
	if err != nil || !ok {
		return nil, ok, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	if ok := writeSnapshotValueBinaryPriorityQueue(&writer, items); !ok {
		return nil, false, nil
	}
	return writer.bytes(), true, nil
}

func marshalSnapshotTopKValueBinary(snapshot topKSnapshot) ([]byte, bool, error) {
	prepared, err := prepareSnapshotTopKBinary(snapshot)
	if err != nil {
		return nil, true, err
	}
	snapshot = prepared
	size, ok, err := snapshotValueBinaryTopKSize(snapshot)
	if err != nil || !ok {
		return nil, ok, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	if ok := writeSnapshotValueBinaryTopK(&writer, snapshot); !ok {
		return nil, false, nil
	}
	return writer.bytes(), true, nil
}

func marshalSnapshotReservoirSampleValueBinary(snapshot reservoirSampleSnapshot) ([]byte, bool, error) {
	prepared, err := prepareSnapshotReservoirSampleBinary(snapshot)
	if err != nil {
		return nil, true, err
	}
	snapshot = prepared
	size, ok, err := snapshotValueBinaryReservoirSampleSize(snapshot)
	if err != nil || !ok {
		return nil, ok, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	if ok := writeSnapshotValueBinaryReservoirSample(&writer, snapshot); !ok {
		return nil, false, nil
	}
	return writer.bytes(), true, nil
}

func marshalSnapshotRadixTreeValueBinary(snapshot radixTreeSnapshot) ([]byte, bool, error) {
	prepared, err := prepareSnapshotRadixTreeBinary(snapshot)
	if err != nil {
		return nil, true, err
	}
	snapshot = prepared
	size, ok, err := snapshotValueBinaryRadixTreeSize(snapshot)
	if err != nil || !ok {
		return nil, ok, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	if ok := writeSnapshotValueBinaryRadixTree(&writer, snapshot); !ok {
		return nil, false, nil
	}
	return writer.bytes(), true, nil
}

func marshalSnapshotBloomFilterValueBinary(snapshot bloomFilterSnapshot) ([]byte, error) {
	bits, err := snapshotBloomFilterRawBits(snapshot)
	if err != nil {
		return nil, err
	}
	size, err := snapshotValueBinaryBloomFilterSize(snapshot, len(bits))
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryBloomFilter(&writer, snapshot, bits)
	return writer.bytes(), nil
}

func snapshotBloomFilterRawBits(snapshot bloomFilterSnapshot) ([]byte, error) {
	if snapshot.Bits == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(snapshot.Bits)
}

func marshalSnapshotCountMinSketchValueBinary(snapshot countMinSketchSnapshot) ([]byte, error) {
	counters, err := snapshotCountMinSketchRawCounters(snapshot)
	if err != nil {
		return nil, err
	}
	size, err := snapshotValueBinaryCountMinSketchSize(snapshot, len(counters))
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryCountMinSketch(&writer, snapshot, counters)
	return writer.bytes(), nil
}

func snapshotCountMinSketchRawCounters(snapshot countMinSketchSnapshot) ([]byte, error) {
	if snapshot.Counters == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(snapshot.Counters)
}

func marshalSnapshotHyperLogLogValueBinary(snapshot hyperLogLogSnapshot) ([]byte, error) {
	registers, err := snapshotHyperLogLogRawRegisters(snapshot)
	if err != nil {
		return nil, err
	}
	size, err := snapshotValueBinaryHyperLogLogSize(snapshot, len(registers))
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryHyperLogLog(&writer, snapshot, registers)
	return writer.bytes(), nil
}

func snapshotHyperLogLogRawRegisters(snapshot hyperLogLogSnapshot) ([]byte, error) {
	if snapshot.Registers == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(snapshot.Registers)
}

func marshalSnapshotCuckooFilterValueBinary(snapshot cuckooFilterSnapshot) ([]byte, error) {
	fingerprints, err := snapshotCuckooFilterRawFingerprints(snapshot)
	if err != nil {
		return nil, err
	}
	size, err := snapshotValueBinaryCuckooFilterSize(snapshot, len(fingerprints))
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryCuckooFilter(&writer, snapshot, fingerprints)
	return writer.bytes(), nil
}

func snapshotCuckooFilterRawFingerprints(snapshot cuckooFilterSnapshot) ([]byte, error) {
	if snapshot.Fingerprints == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(snapshot.Fingerprints)
}

func marshalSnapshotXorFilterValueBinary(snapshot xorFilterSnapshot) ([]byte, bool, error) {
	if !snapshot.Built {
		prepared, err := prepareSnapshotStagedXorFilterBinary(snapshot)
		if err != nil {
			return nil, true, err
		}
		size, ok, err := snapshotValueBinaryStagedXorFilterSize(prepared)
		if err != nil || !ok {
			return nil, ok, err
		}
		writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
		if ok := writeSnapshotValueBinaryStagedXorFilter(&writer, prepared); !ok {
			return nil, false, nil
		}
		return writer.bytes(), true, nil
	}
	fingerprints, err := snapshotXorFilterRawFingerprints(snapshot)
	if err != nil {
		return nil, true, err
	}
	size, err := snapshotValueBinaryXorFilterSize(snapshot, len(fingerprints))
	if err != nil {
		return nil, true, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryXorFilter(&writer, snapshot, fingerprints)
	return writer.bytes(), true, nil
}

func snapshotXorFilterRawFingerprints(snapshot xorFilterSnapshot) ([]byte, error) {
	if snapshot.Fingerprints == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(snapshot.Fingerprints)
}

func marshalSnapshotFenwickTreeValueBinary(snapshot fenwickTreeSnapshot) ([]byte, error) {
	size, err := snapshotValueBinaryFenwickTreeSize(snapshot)
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryFenwickTree(&writer, snapshot)
	return writer.bytes(), nil
}

func marshalSnapshotQuantileSketchValueBinary(snapshot quantileSketchSnapshot) ([]byte, error) {
	size, err := snapshotValueBinaryQuantileSketchSize(snapshot)
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryQuantileSketch(&writer, snapshot)
	return writer.bytes(), nil
}

func prepareSnapshotDynamicValueBinary(value interface{}) (interface{}, bool, error) {
	_, ok, err := snapshotValueBinarySize(value)
	if err != nil || ok {
		return value, false, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var normalized interface{}
	if err := decoder.Decode(&normalized); err != nil {
		return nil, false, err
	}
	if _, ok, err := snapshotValueBinarySize(normalized); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, errors.New("hatriecache: unsupported normalized binary snapshot value")
	}
	return normalized, true, nil
}

func prepareSnapshotPriorityQueueItemsBinary(items []priorityQueueItem) ([]priorityQueueItem, error) {
	prepared := items
	copied := false
	for idx := range items {
		value, changed, err := prepareSnapshotDynamicValueBinary(items[idx].Value)
		if err != nil {
			return nil, err
		}
		if !changed {
			continue
		}
		if !copied {
			prepared = append([]priorityQueueItem(nil), items...)
			copied = true
		}
		prepared[idx].Value = value
		prepared[idx].stringValue = ""
		prepared[idx].hasString = false
	}
	return prepared, nil
}

func prepareSnapshotTopKBinary(snapshot topKSnapshot) (topKSnapshot, error) {
	prepared := snapshot
	copied := false
	for idx := range snapshot.Items {
		value, changed, err := prepareSnapshotDynamicValueBinary(snapshot.Items[idx].Value)
		if err != nil {
			return topKSnapshot{}, err
		}
		if !changed {
			continue
		}
		if !copied {
			prepared.Items = append([]topKItem(nil), snapshot.Items...)
			copied = true
		}
		prepared.Items[idx].Value = value
	}
	return prepared, nil
}

func prepareSnapshotReservoirSampleBinary(snapshot reservoirSampleSnapshot) (reservoirSampleSnapshot, error) {
	prepared := snapshot
	copied := false
	for idx := range snapshot.Items {
		value, changed, err := prepareSnapshotDynamicValueBinary(snapshot.Items[idx].Value)
		if err != nil {
			return reservoirSampleSnapshot{}, err
		}
		if !changed {
			continue
		}
		if !copied {
			prepared.Items = append([]reservoirSampleItem(nil), snapshot.Items...)
			copied = true
		}
		prepared.Items[idx].Value = value
	}
	return prepared, nil
}

func prepareSnapshotRadixTreeBinary(snapshot radixTreeSnapshot) (radixTreeSnapshot, error) {
	prepared := snapshot
	copied := false
	for idx := range snapshot.Items {
		value, changed, err := prepareSnapshotDynamicValueBinary(snapshot.Items[idx].Value)
		if err != nil {
			return radixTreeSnapshot{}, err
		}
		if !changed {
			continue
		}
		if !copied {
			prepared.Items = append([]RadixTreeItem(nil), snapshot.Items...)
			copied = true
		}
		prepared.Items[idx].Value = value
	}
	return prepared, nil
}

func prepareSnapshotStagedXorFilterBinary(snapshot xorFilterSnapshot) (xorFilterSnapshot, error) {
	prepared := snapshot
	copied := false
	for idx := range snapshot.Staged {
		value, changed, err := prepareSnapshotDynamicValueBinary(snapshot.Staged[idx].Value)
		if err != nil {
			return xorFilterSnapshot{}, err
		}
		if !changed {
			continue
		}
		if !copied {
			prepared.Staged = append([]xorFilterStagedItem(nil), snapshot.Staged...)
			copied = true
		}
		prepared.Staged[idx].Value = value
	}
	return prepared, nil
}

func snapshotValueBinarySize(value interface{}) (int, bool, error) {
	switch v := value.(type) {
	case nil:
		return 1, true, nil
	case bool:
		return 1, true, nil
	case string:
		size, err := snapshotValueBinaryBytesSize(len(v))
		if err != nil {
			return 0, true, err
		}
		total, err := snapshotValueBinaryAdd(1, size)
		return total, true, err
	case []byte:
		size, err := snapshotValueBinaryBytesSize(len(v))
		if err != nil {
			return 0, true, err
		}
		total, err := snapshotValueBinaryAdd(1, size)
		return total, true, err
	case json.Number:
		if _, err := jsonNumberValue(v.String()); err != nil {
			return 0, true, err
		}
		size, err := snapshotValueBinaryBytesSize(len(v.String()))
		if err != nil {
			return 0, true, err
		}
		total, err := snapshotValueBinaryAdd(1, size)
		return total, true, err
	case int:
		return 1 + binaryVarintSize(int64(v)), true, nil
	case int8:
		return 1 + binaryVarintSize(int64(v)), true, nil
	case int16:
		return 1 + binaryVarintSize(int64(v)), true, nil
	case int32:
		return 1 + binaryVarintSize(int64(v)), true, nil
	case int64:
		return 1 + binaryVarintSize(v), true, nil
	case uint:
		return 1 + binaryUvarintSize(uint64(v)), true, nil
	case uint8:
		return 1 + binaryUvarintSize(uint64(v)), true, nil
	case uint16:
		return 1 + binaryUvarintSize(uint64(v)), true, nil
	case uint32:
		return 1 + binaryUvarintSize(uint64(v)), true, nil
	case uint64:
		return 1 + binaryUvarintSize(v), true, nil
	case float32, float64:
		number, err := jsonEncodedString(v)
		if err != nil {
			return 0, false, nil
		}
		size, err := snapshotValueBinaryBytesSize(len(number))
		if err != nil {
			return 0, true, err
		}
		total, err := snapshotValueBinaryAdd(1, size)
		return total, true, err
	case map[string]interface{}:
		return snapshotValueBinaryMapSize(v)
	case []interface{}:
		return snapshotValueBinaryArraySize(v)
	case PriorityQueue:
		return snapshotValueBinaryPublicPriorityQueueSize(v)
	default:
		return 0, false, nil
	}
}

func snapshotValueBinaryArraySize(values []interface{}) (int, bool, error) {
	total, err := snapshotValueBinaryAdd(1, binaryUvarintSize(uint64(len(values))))
	if err != nil {
		return 0, true, err
	}
	for _, value := range values {
		size, ok, err := snapshotValueBinarySize(value)
		if err != nil || !ok {
			return 0, ok, err
		}
		total, err = snapshotValueBinaryAdd(total, size)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryPublicPriorityQueueSize(items PriorityQueue) (int, bool, error) {
	total, err := snapshotValueBinaryAdd(1, binaryUvarintSize(uint64(len(items))))
	if err != nil {
		return 0, true, err
	}
	priorityKeySize, err := snapshotValueBinaryBytesSize(len("priority"))
	if err != nil {
		return 0, true, err
	}
	valueKeySize, err := snapshotValueBinaryBytesSize(len("value"))
	if err != nil {
		return 0, true, err
	}
	for _, item := range items {
		itemSize := 1 + binaryUvarintSize(2) + priorityKeySize + 1 + binaryVarintSize(item.Priority) + valueKeySize
		valueSize, ok, err := snapshotValueBinarySize(item.Value)
		if err != nil || !ok {
			return 0, ok, err
		}
		itemSize, err = snapshotValueBinaryAdd(itemSize, valueSize)
		if err != nil {
			return 0, true, err
		}
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryMapSize(values map[string]interface{}) (int, bool, error) {
	total, err := snapshotValueBinaryAdd(1, binaryUvarintSize(uint64(len(values))))
	if err != nil {
		return 0, true, err
	}
	for key, value := range values {
		keySize, err := snapshotValueBinaryBytesSize(len(key))
		if err != nil {
			return 0, true, err
		}
		total, err = snapshotValueBinaryAdd(total, keySize)
		if err != nil {
			return 0, true, err
		}
		valueSize, ok, err := snapshotValueBinarySize(value)
		if err != nil || !ok {
			return 0, ok, err
		}
		total, err = snapshotValueBinaryAdd(total, valueSize)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryPriorityQueueSize(items []priorityQueueItem) (int, bool, error) {
	total, err := snapshotValueBinaryAdd(1, binaryUvarintSize(uint64(len(items))))
	if err != nil {
		return 0, true, err
	}
	for _, item := range items {
		itemSize := binaryVarintSize(item.Priority) + binaryUvarintSize(item.Sequence)
		valueSize, ok, err := snapshotValueBinarySize(item.Value)
		if err != nil || !ok {
			return 0, ok, err
		}
		itemSize, err = snapshotValueBinaryAdd(itemSize, valueSize)
		if err != nil {
			return 0, true, err
		}
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryTopKSize(snapshot topKSnapshot) (int, bool, error) {
	total := 1 +
		binaryUvarintSize(snapshot.Capacity) +
		binaryUvarintSize(snapshot.Total) +
		binaryUvarintSize(uint64(len(snapshot.Items)))
	for _, item := range snapshot.Items {
		keySize, err := snapshotValueBinaryBytesSize(len(item.Key))
		if err != nil {
			return 0, true, err
		}
		itemSize := keySize + binaryUvarintSize(item.Count) + binaryUvarintSize(item.Error)
		valueSize, ok, err := snapshotValueBinarySize(item.Value)
		if err != nil || !ok {
			return 0, ok, err
		}
		itemSize, err = snapshotValueBinaryAdd(itemSize, valueSize)
		if err != nil {
			return 0, true, err
		}
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryReservoirSampleSize(snapshot reservoirSampleSnapshot) (int, bool, error) {
	total := 1 +
		binaryUvarintSize(snapshot.Capacity) +
		binaryUvarintSize(snapshot.Seen) +
		binaryUvarintSize(uint64(len(snapshot.Items)))
	for _, item := range snapshot.Items {
		itemSize := binaryUvarintSize(item.Priority) + binaryUvarintSize(item.Sequence)
		valueSize, ok, err := snapshotValueBinarySize(item.Value)
		if err != nil || !ok {
			return 0, ok, err
		}
		itemSize, err = snapshotValueBinaryAdd(itemSize, valueSize)
		if err != nil {
			return 0, true, err
		}
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryRadixTreeSize(snapshot radixTreeSnapshot) (int, bool, error) {
	if snapshot.Count != uint64(len(snapshot.Items)) {
		return 0, true, errors.New("hatriecache: radix tree count does not match items")
	}
	total, err := snapshotValueBinaryAdd(1, binaryUvarintSize(uint64(len(snapshot.Items))))
	if err != nil {
		return 0, true, err
	}
	for _, item := range snapshot.Items {
		keySize, err := snapshotValueBinaryBytesSize(len(item.Key))
		if err != nil {
			return 0, true, err
		}
		total, err = snapshotValueBinaryAdd(total, keySize)
		if err != nil {
			return 0, true, err
		}
		valueSize, ok, err := snapshotValueBinarySize(item.Value)
		if err != nil || !ok {
			return 0, ok, err
		}
		total, err = snapshotValueBinaryAdd(total, valueSize)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryStagedXorFilterSize(snapshot xorFilterSnapshot) (int, bool, error) {
	total := 1 +
		binaryUvarintSize(snapshot.ExpectedItems) +
		binaryUvarintSize(snapshot.Items) +
		binaryUvarintSize(uint64(len(snapshot.Staged)))
	for _, item := range snapshot.Staged {
		keySize, err := snapshotValueBinaryBytesSize(len(item.Key))
		if err != nil {
			return 0, true, err
		}
		total, err = snapshotValueBinaryAdd(total, keySize)
		if err != nil {
			return 0, true, err
		}
		valueSize, ok, err := snapshotValueBinarySize(item.Value)
		if err != nil || !ok {
			return 0, ok, err
		}
		total, err = snapshotValueBinaryAdd(total, valueSize)
		if err != nil {
			return 0, true, err
		}
	}
	return total, true, nil
}

func snapshotValueBinaryBloomFilterSize(snapshot bloomFilterSnapshot, bitBytes int) (int, error) {
	total := 1 +
		binaryUvarintSize(snapshot.BitCount) +
		binaryUvarintSize(uint64(snapshot.HashCount)) +
		binaryUvarintSize(snapshot.Insertions)
	bitsSize, err := snapshotValueBinaryBytesSize(bitBytes)
	if err != nil {
		return 0, err
	}
	return snapshotValueBinaryAdd(total, bitsSize)
}

func snapshotValueBinaryCountMinSketchSize(snapshot countMinSketchSnapshot, counterBytes int) (int, error) {
	total := 1 +
		binaryUvarintSize(snapshot.Width) +
		binaryUvarintSize(uint64(snapshot.Depth)) +
		binaryUvarintSize(snapshot.TotalCount)
	counterSize, err := snapshotValueBinaryBytesSize(counterBytes)
	if err != nil {
		return 0, err
	}
	return snapshotValueBinaryAdd(total, counterSize)
}

func snapshotValueBinaryHyperLogLogSize(snapshot hyperLogLogSnapshot, registerBytes int) (int, error) {
	total := 1 +
		binaryUvarintSize(uint64(snapshot.Precision)) +
		binaryUvarintSize(snapshot.Observations)
	registerSize, err := snapshotValueBinaryBytesSize(registerBytes)
	if err != nil {
		return 0, err
	}
	return snapshotValueBinaryAdd(total, registerSize)
}

func snapshotValueBinaryCuckooFilterSize(snapshot cuckooFilterSnapshot, fingerprintBytes int) (int, error) {
	total := 1 +
		binaryUvarintSize(snapshot.BucketCount) +
		binaryUvarintSize(uint64(snapshot.BucketSize)) +
		binaryUvarintSize(uint64(snapshot.FingerprintBits)) +
		binaryUvarintSize(snapshot.Count)
	fingerprintSize, err := snapshotValueBinaryBytesSize(fingerprintBytes)
	if err != nil {
		return 0, err
	}
	return snapshotValueBinaryAdd(total, fingerprintSize)
}

func snapshotValueBinaryXorFilterSize(snapshot xorFilterSnapshot, fingerprintBytes int) (int, error) {
	total := 1 +
		binaryUvarintSize(snapshot.ExpectedItems) +
		binaryUvarintSize(snapshot.Items) +
		binaryUvarintSize(snapshot.Seed) +
		binaryUvarintSize(uint64(snapshot.BlockLength))
	fingerprintSize, err := snapshotValueBinaryBytesSize(fingerprintBytes)
	if err != nil {
		return 0, err
	}
	return snapshotValueBinaryAdd(total, fingerprintSize)
}

func snapshotValueBinaryFenwickTreeSize(snapshot fenwickTreeSnapshot) (int, error) {
	total := 1 +
		binaryUvarintSize(snapshot.Size) +
		binaryUvarintSize(snapshot.Updates) +
		binaryVarintSize(snapshot.Total) +
		binaryUvarintSize(uint64(len(snapshot.Tree)))
	for _, value := range snapshot.Tree {
		var err error
		total, err = snapshotValueBinaryAdd(total, binaryVarintSize(value))
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func snapshotValueBinaryQuantileSketchSize(snapshot quantileSketchSnapshot) (int, error) {
	total := 1 + 8 + binaryUvarintSize(snapshot.Count) + binaryUvarintSize(uint64(len(snapshot.Summary)))
	for _, sample := range snapshot.Summary {
		itemSize := 8 + binaryUvarintSize(sample.Gap) + binaryUvarintSize(sample.Delta)
		var err error
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func snapshotValueBinaryBytesSize(size int) (int, error) {
	if size < 0 {
		return 0, errSnapshotValueBinaryTooLarge
	}
	return snapshotValueBinaryAdd(binaryUvarintSize(uint64(size)), size)
}

func snapshotValueBinaryAdd(left int, right int) (int, error) {
	max := int(^uint(0) >> 1)
	if right < 0 || left > max-right {
		return 0, errSnapshotValueBinaryTooLarge
	}
	return left + right, nil
}

func snapshotValueBinaryInitialCapacity(count uint64, remaining int, minEncodedItemSize int) (int, error) {
	if count > uint64(int(^uint(0)>>1)) {
		return 0, errSnapshotValueBinaryTooLarge
	}
	if minEncodedItemSize > 0 && count > uint64(remaining/minEncodedItemSize) {
		return 0, io.ErrUnexpectedEOF
	}
	capacity := int(count)
	if capacity > snapshotValueBinaryMaxInitialCapacity {
		return snapshotValueBinaryMaxInitialCapacity, nil
	}
	return capacity, nil
}

func writeSnapshotValueBinary(writer *binaryFieldWriter, value interface{}) bool {
	switch v := value.(type) {
	case nil:
		writer.buf = append(writer.buf, snapshotValueBinaryNull)
	case bool:
		if v {
			writer.buf = append(writer.buf, snapshotValueBinaryTrue)
		} else {
			writer.buf = append(writer.buf, snapshotValueBinaryFalse)
		}
	case string:
		writer.buf = append(writer.buf, snapshotValueBinaryString)
		writer.writeString(v)
	case []byte:
		writer.buf = append(writer.buf, snapshotValueBinaryBytes)
		writer.writeBytes(v)
	case json.Number:
		writer.buf = append(writer.buf, snapshotValueBinaryNumber)
		writer.writeString(v.String())
	case int:
		writer.buf = append(writer.buf, snapshotValueBinarySigned)
		writer.writeVarint(int64(v))
	case int8:
		writer.buf = append(writer.buf, snapshotValueBinarySigned)
		writer.writeVarint(int64(v))
	case int16:
		writer.buf = append(writer.buf, snapshotValueBinarySigned)
		writer.writeVarint(int64(v))
	case int32:
		writer.buf = append(writer.buf, snapshotValueBinarySigned)
		writer.writeVarint(int64(v))
	case int64:
		writer.buf = append(writer.buf, snapshotValueBinarySigned)
		writer.writeVarint(v)
	case uint:
		writer.buf = append(writer.buf, snapshotValueBinaryUnsigned)
		writer.writeUvarint(uint64(v))
	case uint8:
		writer.buf = append(writer.buf, snapshotValueBinaryUnsigned)
		writer.writeUvarint(uint64(v))
	case uint16:
		writer.buf = append(writer.buf, snapshotValueBinaryUnsigned)
		writer.writeUvarint(uint64(v))
	case uint32:
		writer.buf = append(writer.buf, snapshotValueBinaryUnsigned)
		writer.writeUvarint(uint64(v))
	case uint64:
		writer.buf = append(writer.buf, snapshotValueBinaryUnsigned)
		writer.writeUvarint(v)
	case float32, float64:
		number, err := jsonEncodedString(v)
		if err != nil {
			return false
		}
		writer.buf = append(writer.buf, snapshotValueBinaryNumber)
		writer.writeString(number)
	case map[string]interface{}:
		return writeSnapshotValueBinaryMap(writer, v)
	case []interface{}:
		return writeSnapshotValueBinaryArray(writer, v)
	case PriorityQueue:
		return writeSnapshotValueBinaryPublicPriorityQueue(writer, v)
	default:
		return false
	}
	return true
}

func writeSnapshotValueBinaryArray(writer *binaryFieldWriter, values []interface{}) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryArray)
	writer.writeUvarint(uint64(len(values)))
	for _, value := range values {
		if ok := writeSnapshotValueBinary(writer, value); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryPublicPriorityQueue(writer *binaryFieldWriter, items PriorityQueue) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryArray)
	writer.writeUvarint(uint64(len(items)))
	for _, item := range items {
		writer.buf = append(writer.buf, snapshotValueBinaryObject)
		writer.writeUvarint(2)
		writer.writeString("priority")
		writer.buf = append(writer.buf, snapshotValueBinarySigned)
		writer.writeVarint(item.Priority)
		writer.writeString("value")
		if ok := writeSnapshotValueBinary(writer, item.Value); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryMap(writer *binaryFieldWriter, values map[string]interface{}) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryObject)
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writer.writeUvarint(uint64(len(keys)))
	for _, key := range keys {
		writer.writeString(key)
		if ok := writeSnapshotValueBinary(writer, values[key]); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryPriorityQueue(writer *binaryFieldWriter, items []priorityQueueItem) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryPriorityQueue)
	writer.writeUvarint(uint64(len(items)))
	for _, item := range items {
		writer.writeVarint(item.Priority)
		writer.writeUvarint(item.Sequence)
		if ok := writeSnapshotValueBinary(writer, item.Value); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryTopK(writer *binaryFieldWriter, snapshot topKSnapshot) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryTopK)
	writer.writeUvarint(snapshot.Capacity)
	writer.writeUvarint(snapshot.Total)
	writer.writeUvarint(uint64(len(snapshot.Items)))
	for _, item := range snapshot.Items {
		writer.writeString(item.Key)
		writer.writeUvarint(item.Count)
		writer.writeUvarint(item.Error)
		if ok := writeSnapshotValueBinary(writer, item.Value); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryReservoirSample(writer *binaryFieldWriter, snapshot reservoirSampleSnapshot) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryReservoirSample)
	writer.writeUvarint(snapshot.Capacity)
	writer.writeUvarint(snapshot.Seen)
	writer.writeUvarint(uint64(len(snapshot.Items)))
	for _, item := range snapshot.Items {
		writer.writeUvarint(item.Priority)
		writer.writeUvarint(item.Sequence)
		if ok := writeSnapshotValueBinary(writer, item.Value); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryRadixTree(writer *binaryFieldWriter, snapshot radixTreeSnapshot) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryRadixTree)
	writer.writeUvarint(uint64(len(snapshot.Items)))
	for _, item := range snapshot.Items {
		writer.writeString(item.Key)
		if ok := writeSnapshotValueBinary(writer, item.Value); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryBloomFilter(writer *binaryFieldWriter, snapshot bloomFilterSnapshot, bits []byte) {
	writer.buf = append(writer.buf, snapshotValueBinaryBloomFilter)
	writer.writeUvarint(snapshot.BitCount)
	writer.writeUvarint(uint64(snapshot.HashCount))
	writer.writeUvarint(snapshot.Insertions)
	writer.writeBytes(bits)
}

func writeSnapshotValueBinaryCountMinSketch(writer *binaryFieldWriter, snapshot countMinSketchSnapshot, counters []byte) {
	writer.buf = append(writer.buf, snapshotValueBinaryCountMinSketch)
	writer.writeUvarint(snapshot.Width)
	writer.writeUvarint(uint64(snapshot.Depth))
	writer.writeUvarint(snapshot.TotalCount)
	writer.writeBytes(counters)
}

func writeSnapshotValueBinaryHyperLogLog(writer *binaryFieldWriter, snapshot hyperLogLogSnapshot, registers []byte) {
	writer.buf = append(writer.buf, snapshotValueBinaryHyperLogLog)
	writer.writeUvarint(uint64(snapshot.Precision))
	writer.writeUvarint(snapshot.Observations)
	writer.writeBytes(registers)
}

func writeSnapshotValueBinaryCuckooFilter(writer *binaryFieldWriter, snapshot cuckooFilterSnapshot, fingerprints []byte) {
	writer.buf = append(writer.buf, snapshotValueBinaryCuckooFilter)
	writer.writeUvarint(snapshot.BucketCount)
	writer.writeUvarint(uint64(snapshot.BucketSize))
	writer.writeUvarint(uint64(snapshot.FingerprintBits))
	writer.writeUvarint(snapshot.Count)
	writer.writeBytes(fingerprints)
}

func writeSnapshotValueBinaryXorFilter(writer *binaryFieldWriter, snapshot xorFilterSnapshot, fingerprints []byte) {
	writer.buf = append(writer.buf, snapshotValueBinaryXorFilter)
	writer.writeUvarint(snapshot.ExpectedItems)
	writer.writeUvarint(snapshot.Items)
	writer.writeUvarint(snapshot.Seed)
	writer.writeUvarint(uint64(snapshot.BlockLength))
	writer.writeBytes(fingerprints)
}

func writeSnapshotValueBinaryStagedXorFilter(writer *binaryFieldWriter, snapshot xorFilterSnapshot) bool {
	writer.buf = append(writer.buf, snapshotValueBinaryStagedXorFilter)
	writer.writeUvarint(snapshot.ExpectedItems)
	writer.writeUvarint(snapshot.Items)
	writer.writeUvarint(uint64(len(snapshot.Staged)))
	for _, item := range snapshot.Staged {
		writer.writeString(item.Key)
		if ok := writeSnapshotValueBinary(writer, item.Value); !ok {
			return false
		}
	}
	return true
}

func writeSnapshotValueBinaryFenwickTree(writer *binaryFieldWriter, snapshot fenwickTreeSnapshot) {
	writer.buf = append(writer.buf, snapshotValueBinaryFenwickTree)
	writer.writeUvarint(snapshot.Size)
	writer.writeUvarint(snapshot.Updates)
	writer.writeVarint(snapshot.Total)
	writer.writeUvarint(uint64(len(snapshot.Tree)))
	for _, value := range snapshot.Tree {
		writer.writeVarint(value)
	}
}

func writeSnapshotValueBinaryQuantileSketch(writer *binaryFieldWriter, snapshot quantileSketchSnapshot) {
	writer.buf = append(writer.buf, snapshotValueBinaryQuantileSketch)
	writer.writeFloat64(snapshot.Epsilon)
	writer.writeUvarint(snapshot.Count)
	writer.writeUvarint(uint64(len(snapshot.Summary)))
	for _, sample := range snapshot.Summary {
		writer.writeFloat64(sample.Value)
		writer.writeUvarint(sample.Gap)
		writer.writeUvarint(sample.Delta)
	}
}

func unmarshalSnapshotValueBinary(data []byte) (interface{}, error) {
	payload, ok := snapshotValueBinaryPayload(data)
	if !ok {
		return nil, errors.New("hatriecache: invalid binary snapshot value")
	}
	reader := newBinaryFieldReader(payload)
	value, err := readSnapshotValueBinary(&reader)
	if err != nil {
		return nil, err
	}
	if !reader.done() {
		return nil, errors.New("hatriecache: invalid trailing binary snapshot value data")
	}
	return value, nil
}

func readSnapshotValueBinary(reader *binaryFieldReader) (interface{}, error) {
	if reader.off >= len(reader.data) {
		return nil, io.ErrUnexpectedEOF
	}
	tag := reader.data[reader.off]
	reader.off++
	switch tag {
	case snapshotValueBinaryNull:
		return nil, nil
	case snapshotValueBinaryFalse:
		return false, nil
	case snapshotValueBinaryTrue:
		return true, nil
	case snapshotValueBinaryString:
		return reader.readString()
	case snapshotValueBinaryNumber:
		value, err := reader.readString()
		if err != nil {
			return nil, err
		}
		return jsonNumberValue(value)
	case snapshotValueBinarySigned:
		value, err := reader.readVarint()
		if err != nil {
			return nil, err
		}
		return json.Number(strconv.FormatInt(value, 10)), nil
	case snapshotValueBinaryUnsigned:
		value, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		return json.Number(strconv.FormatUint(value, 10)), nil
	case snapshotValueBinaryBytes:
		value, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		return base64.StdEncoding.EncodeToString(value), nil
	case snapshotValueBinaryArray:
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if count > uint64(int(^uint(0)>>1)) {
			return nil, errSnapshotValueBinaryTooLarge
		}
		capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 1)
		if err != nil {
			return nil, err
		}
		values := make(Slice, 0, capacity)
		for idx := 0; idx < int(count); idx++ {
			value, err := readSnapshotValueBinary(reader)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	case snapshotValueBinaryObject:
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if count > uint64(int(^uint(0)>>1)) {
			return nil, errSnapshotValueBinaryTooLarge
		}
		capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 2)
		if err != nil {
			return nil, err
		}
		values := make(Map, capacity)
		for idx := 0; idx < int(count); idx++ {
			key, err := reader.readString()
			if err != nil {
				return nil, err
			}
			if _, exists := values[key]; exists {
				return nil, errors.New("hatriecache: duplicate binary snapshot value object key")
			}
			value, err := readSnapshotValueBinary(reader)
			if err != nil {
				return nil, err
			}
			values[key] = value
		}
		return values, nil
	case snapshotValueBinaryPriorityQueue:
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if count > uint64(int(^uint(0)>>1)) {
			return nil, errSnapshotValueBinaryTooLarge
		}
		capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 3)
		if err != nil {
			return nil, err
		}
		items := make([]priorityQueueItem, 0, capacity)
		for idx := 0; idx < int(count); idx++ {
			priority, err := reader.readVarint()
			if err != nil {
				return nil, err
			}
			sequence, err := reader.readUvarint()
			if err != nil {
				return nil, err
			}
			value, err := readSnapshotValueBinary(reader)
			if err != nil {
				return nil, err
			}
			items = append(items, priorityQueueItem{
				Priority: priority,
				Sequence: sequence,
				Value:    value,
			})
		}
		return items, nil
	case snapshotValueBinaryTopK:
		capacity, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		total, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		capacityHint, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 4)
		if err != nil {
			return nil, err
		}
		items := make([]topKItem, 0, capacityHint)
		for idx := 0; idx < int(count); idx++ {
			key, err := reader.readString()
			if err != nil {
				return nil, err
			}
			itemCount, err := reader.readUvarint()
			if err != nil {
				return nil, err
			}
			itemError, err := reader.readUvarint()
			if err != nil {
				return nil, err
			}
			value, err := readSnapshotValueBinary(reader)
			if err != nil {
				return nil, err
			}
			items = append(items, topKItem{
				Key:   key,
				Value: value,
				Count: itemCount,
				Error: itemError,
			})
		}
		return topKSnapshot{
			Capacity: capacity,
			Total:    total,
			Items:    items,
		}, nil
	case snapshotValueBinaryReservoirSample:
		capacity, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		seen, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		capacityHint, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 3)
		if err != nil {
			return nil, err
		}
		items := make([]reservoirSampleItem, 0, capacityHint)
		for idx := 0; idx < int(count); idx++ {
			priority, err := reader.readUvarint()
			if err != nil {
				return nil, err
			}
			sequence, err := reader.readUvarint()
			if err != nil {
				return nil, err
			}
			value, err := readSnapshotValueBinary(reader)
			if err != nil {
				return nil, err
			}
			items = append(items, reservoirSampleItem{
				Value:    value,
				Priority: priority,
				Sequence: sequence,
			})
		}
		return reservoirSampleSnapshot{
			Capacity: capacity,
			Seen:     seen,
			Items:    items,
		}, nil
	case snapshotValueBinaryRadixTree:
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if count > uint64(int(^uint(0)>>1)) {
			return nil, errSnapshotValueBinaryTooLarge
		}
		capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 2)
		if err != nil {
			return nil, err
		}
		items := make([]RadixTreeItem, 0, capacity)
		for idx := 0; idx < int(count); idx++ {
			key, err := reader.readString()
			if err != nil {
				return nil, err
			}
			value, err := readSnapshotValueBinary(reader)
			if err != nil {
				return nil, err
			}
			items = append(items, RadixTreeItem{
				Key:   key,
				Value: value,
			})
		}
		return radixTreeSnapshot{
			Count: uint64(len(items)),
			Items: items,
		}, nil
	case snapshotValueBinaryBloomFilter:
		bitCount, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		hashCount, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if hashCount > uint64(^uint8(0)) {
			return nil, errors.New("hatriecache: binary bloom filter hash count is too large")
		}
		insertions, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		bits, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		return bloomFilterSnapshot{
			BitCount:   bitCount,
			HashCount:  uint8(hashCount),
			Insertions: insertions,
			Bits:       base64.StdEncoding.EncodeToString(bits),
		}, nil
	case snapshotValueBinaryCountMinSketch:
		width, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		depth, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if depth > uint64(^uint8(0)) {
			return nil, errors.New("hatriecache: binary count-min sketch depth is too large")
		}
		total, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		counters, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		return countMinSketchSnapshot{
			Width:      width,
			Depth:      uint8(depth),
			TotalCount: total,
			Counters:   base64.StdEncoding.EncodeToString(counters),
		}, nil
	case snapshotValueBinaryHyperLogLog:
		precision, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if precision > uint64(^uint8(0)) {
			return nil, errors.New("hatriecache: binary hyperloglog precision is too large")
		}
		observations, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		registers, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		return hyperLogLogSnapshot{
			Precision:    uint8(precision),
			Observations: observations,
			Registers:    base64.StdEncoding.EncodeToString(registers),
		}, nil
	case snapshotValueBinaryCuckooFilter:
		bucketCount, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		bucketSize, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if bucketSize > uint64(^uint8(0)) {
			return nil, errors.New("hatriecache: binary cuckoo filter bucket size is too large")
		}
		fingerprintBits, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if fingerprintBits > uint64(^uint8(0)) {
			return nil, errors.New("hatriecache: binary cuckoo filter fingerprint bits is too large")
		}
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		fingerprints, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		return cuckooFilterSnapshot{
			BucketCount:     bucketCount,
			BucketSize:      uint8(bucketSize),
			FingerprintBits: uint8(fingerprintBits),
			Count:           count,
			Fingerprints:    base64.StdEncoding.EncodeToString(fingerprints),
		}, nil
	case snapshotValueBinaryXorFilter:
		expectedItems, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		items, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		seed, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		blockLength, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if blockLength > uint64(^uint32(0)) {
			return nil, errors.New("hatriecache: binary xor filter block length is too large")
		}
		fingerprints, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		return xorFilterSnapshot{
			ExpectedItems: expectedItems,
			Built:         true,
			Items:         items,
			Seed:          seed,
			BlockLength:   uint32(blockLength),
			Fingerprints:  base64.StdEncoding.EncodeToString(fingerprints),
		}, nil
	case snapshotValueBinaryStagedXorFilter:
		expectedItems, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		items, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 2)
		if err != nil {
			return nil, err
		}
		staged := make([]xorFilterStagedItem, 0, capacity)
		seen := make(map[string]struct{}, capacity)
		for idx := 0; idx < int(count); idx++ {
			key, err := reader.readString()
			if err != nil {
				return nil, err
			}
			if _, exists := seen[key]; exists {
				return nil, errors.New("hatriecache: duplicate binary staged xor filter key")
			}
			seen[key] = struct{}{}
			value, err := readSnapshotValueBinary(reader)
			if err != nil {
				return nil, err
			}
			staged = append(staged, xorFilterStagedItem{Key: key, Value: value})
		}
		return xorFilterSnapshot{
			ExpectedItems: expectedItems,
			Items:         items,
			Staged:        staged,
		}, nil
	case snapshotValueBinaryRoaringBitmap:
		return readSnapshotValueBinaryRoaringBitmap(reader)
	case snapshotValueBinarySparseBitset:
		return readSnapshotValueBinarySparseBitset(reader)
	case snapshotValueBinaryFenwickTree:
		size, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		updates, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		total, err := reader.readVarint()
		if err != nil {
			return nil, err
		}
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 1)
		if err != nil {
			return nil, err
		}
		values := make([]int64, 0, capacity)
		for idx := 0; idx < int(count); idx++ {
			value, err := reader.readVarint()
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return fenwickTreeSnapshot{
			Size:    size,
			Updates: updates,
			Total:   total,
			Tree:    values,
		}, nil
	case snapshotValueBinaryQuantileSketch:
		epsilon, err := reader.readFloat64()
		if err != nil {
			return nil, err
		}
		count, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		sampleCount, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		capacity, err := snapshotValueBinaryInitialCapacity(sampleCount, len(reader.data)-reader.off, 10)
		if err != nil {
			return nil, err
		}
		summary := make([]quantileSketchSample, 0, capacity)
		for idx := 0; idx < int(sampleCount); idx++ {
			value, err := reader.readFloat64()
			if err != nil {
				return nil, err
			}
			gap, err := reader.readUvarint()
			if err != nil {
				return nil, err
			}
			delta, err := reader.readUvarint()
			if err != nil {
				return nil, err
			}
			summary = append(summary, quantileSketchSample{
				Value: value,
				Gap:   gap,
				Delta: delta,
			})
		}
		return quantileSketchSnapshot{
			Epsilon: epsilon,
			Count:   count,
			Summary: summary,
		}, nil
	default:
		return nil, fmt.Errorf("hatriecache: unknown binary snapshot value tag %d", tag)
	}
}

func jsonNumberValue(value string) (json.Number, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	decoder.UseNumber()
	var decoded interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return "", err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return "", errors.New("hatriecache: invalid binary snapshot number")
		}
		return "", err
	}
	number, ok := decoded.(json.Number)
	if !ok {
		return "", errors.New("hatriecache: invalid binary snapshot number")
	}
	return number, nil
}
