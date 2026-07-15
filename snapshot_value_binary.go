package hatriecache

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"

	json "github.com/goccy/go-json"
)

var snapshotValueBinaryMagic = []byte{'h', 'c', 'v', 'b', 1}

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
)

var errSnapshotValueBinaryTooLarge = errors.New("hatriecache: binary snapshot value is too large")

const snapshotValueBinaryMaxInitialCapacity = 4096

func snapshotValueDataIsBinary(data []byte) bool {
	return bytes.HasPrefix(data, snapshotValueBinaryMagic)
}

func marshalSnapshotCollectionValueBinary(value interface{}) ([]byte, bool, error) {
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

func marshalSnapshotRadixTreeValueBinary(snapshot radixTreeSnapshot) ([]byte, bool, error) {
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
		size, err := snapshotValueBinaryBytesSize(base64.StdEncoding.EncodedLen(len(v)))
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
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
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
		writer.buf = append(writer.buf, snapshotValueBinaryString)
		writer.writeUvarint(uint64(base64.StdEncoding.EncodedLen(len(v))))
		writer.buf = base64.StdEncoding.AppendEncode(writer.buf, v)
	case json.Number:
		writer.buf = append(writer.buf, snapshotValueBinaryNumber)
		writer.writeString(v.String())
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
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

func unmarshalSnapshotValueBinary(data []byte) (interface{}, error) {
	if !snapshotValueDataIsBinary(data) {
		return nil, errors.New("hatriecache: invalid binary snapshot value")
	}
	reader := newBinaryFieldReader(data[len(snapshotValueBinaryMagic):])
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
