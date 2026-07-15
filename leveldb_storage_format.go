package hatriecache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	json "github.com/goccy/go-json"
)

type StorageFormat string

const (
	StorageFormatJSON   StorageFormat = "json"
	StorageFormatBinary StorageFormat = "binary"
)

const DefaultStorageFormat = StorageFormatBinary

var levelDBBinaryMagic = []byte{'h', 'c', 'd', 'b', 1}

var errLevelDBBinaryRecordTooLarge = errors.New("hatriecache: binary leveldb entry is too large")

func ParseStorageFormat(value string) (StorageFormat, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(StorageFormatBinary), "bin":
		return StorageFormatBinary, nil
	case string(StorageFormatJSON):
		return StorageFormatJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported storage format %q", value)
	}
}

func marshalLevelDBEntry(entry snapshotEntry, format StorageFormat) ([]byte, error) {
	format, err := ParseStorageFormat(string(format))
	if err != nil {
		return nil, err
	}
	switch format {
	case StorageFormatJSON:
		return marshalSnapshotEntryJSON(entry)
	case StorageFormatBinary:
		return marshalLevelDBEntryBinary(entry)
	default:
		return nil, fmt.Errorf("hatriecache: unsupported storage format %q", format)
	}
}

func levelDBEntryDataIsBinary(data []byte) bool {
	return bytes.HasPrefix(data, levelDBBinaryMagic)
}

func marshalLevelDBEntryBinary(entry snapshotEntry) ([]byte, error) {
	value, err := prepareLevelDBBinaryEntryValue(entry)
	if err != nil {
		return nil, err
	}
	capacity, err := levelDBBinaryRecordCapacityForValue(entry.Key, entry.Type, value.encodedSize, entry.ExpiresAt, entry.Stats)
	if err != nil {
		return nil, err
	}
	writer := newLevelDBBinaryWriterWithCapacity(capacity)
	writer.writeString(entry.Key)
	writer.writeString(entry.Type)
	writer.writePreparedSnapshotEntryValue(value)
	writer.writeTimePtr(entry.ExpiresAt)
	writer.writeKeyStatsPtr(entry.Stats)
	return writer.bytes(), nil
}

func marshalLevelDBBytesEntryBinary(key string, raw []byte, expiresAt *time.Time, stats *KeyStats) ([]byte, error) {
	capacity, err := levelDBBinaryRecordCapacity(key, "bytes", int64(len(raw)), expiresAt, stats)
	if err != nil {
		return nil, err
	}
	writer := newLevelDBBinaryWriterWithCapacity(capacity)
	writer.writeString(key)
	writer.writeString("bytes")
	writer.writeBytes(raw)
	writer.writeTimePtr(expiresAt)
	writer.writeKeyStatsPtr(stats)
	return writer.bytes(), nil
}

func marshalLevelDBBytesEntryBinaryFromReader(key string, rawSize int64, reader io.Reader, expiresAt *time.Time, stats *KeyStats) ([]byte, error) {
	if rawSize < 0 {
		return nil, errLevelDBBinaryRecordTooLarge
	}
	capacity, err := levelDBBinaryRecordCapacity(key, "bytes", rawSize, expiresAt, stats)
	if err != nil {
		return nil, err
	}
	writer := newLevelDBBinaryWriterWithCapacity(capacity)
	writer.writeString(key)
	writer.writeString("bytes")
	writer.writeUvarint(uint64(rawSize))
	written, err := io.Copy(&writer, io.LimitReader(reader, rawSize+1))
	if err != nil {
		return nil, err
	}
	if written != rawSize {
		if written < rawSize {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, errors.New("hatriecache: binary leveldb bytes changed while encoding")
	}
	writer.writeTimePtr(expiresAt)
	writer.writeKeyStatsPtr(stats)
	return writer.bytes(), nil
}

type levelDBBinaryWriter struct {
	binaryFieldWriter
}

func newLevelDBBinaryWriterWithCapacity(capacity int) levelDBBinaryWriter {
	return levelDBBinaryWriter{binaryFieldWriter: newBinaryFieldWriter(levelDBBinaryMagic, capacity)}
}

func levelDBBinaryRecordCapacity(key string, entryType string, valueBytes int64, expiresAt *time.Time, stats *KeyStats) (int, error) {
	if valueBytes < 0 {
		return 0, errLevelDBBinaryRecordTooLarge
	}
	valueSize, err := binaryLengthPrefixedSize(valueBytes)
	if err != nil {
		return 0, err
	}
	return levelDBBinaryRecordCapacityForValue(key, entryType, valueSize, expiresAt, stats)
}

func levelDBBinaryRecordCapacityForValue(key string, entryType string, encodedValueBytes int64, expiresAt *time.Time, stats *KeyStats) (int, error) {
	if encodedValueBytes < 0 {
		return 0, errLevelDBBinaryRecordTooLarge
	}
	keySize, err := binaryLengthPrefixedSize(int64(len(key)))
	if err != nil {
		return 0, err
	}
	typeSize, err := binaryLengthPrefixedSize(int64(len(entryType)))
	if err != nil {
		return 0, err
	}
	total := int64(len(levelDBBinaryMagic))
	for _, size := range []int64{
		keySize,
		typeSize,
		encodedValueBytes,
		int64(levelDBBinaryTimePtrSize(expiresAt)),
		int64(levelDBBinaryKeyStatsPtrSize(stats)),
	} {
		var err error
		total, err = addLevelDBBinaryRecordSize(total, size)
		if err != nil {
			return 0, err
		}
	}
	return int(total), nil
}

func binaryLengthPrefixedSize(payloadBytes int64) (int64, error) {
	if payloadBytes < 0 {
		return 0, errLevelDBBinaryRecordTooLarge
	}
	return addLevelDBBinaryRecordSize(int64(binaryUvarintSize(uint64(payloadBytes))), payloadBytes)
}

func addLevelDBBinaryRecordSize(left int64, right int64) (int64, error) {
	max := int64(int(^uint(0) >> 1))
	if right < 0 || left > max-right {
		return 0, errLevelDBBinaryRecordTooLarge
	}
	return left + right, nil
}

func levelDBBinaryTimeSize(value time.Time) int {
	if value.IsZero() {
		return 1
	}
	return 1 + binaryVarintSize(value.UnixNano())
}

func levelDBBinaryTimePtrSize(value *time.Time) int {
	if value == nil {
		return 1
	}
	return 1 + binaryVarintSize(value.UnixNano())
}

func levelDBBinaryKeyStatsPtrSize(stats *KeyStats) int {
	if stats == nil {
		return 1
	}
	return 1 +
		binaryUvarintSize(stats.Reads) +
		binaryUvarintSize(stats.Hits) +
		binaryUvarintSize(stats.Misses) +
		binaryUvarintSize(stats.Writes) +
		levelDBBinaryTimeSize(stats.LastHit) +
		levelDBBinaryTimeSize(stats.LastMiss) +
		levelDBBinaryTimeSize(stats.LastWrite) +
		16
}

type levelDBBinaryPreparedValueKind uint8

const (
	levelDBBinaryPreparedCounter levelDBBinaryPreparedValueKind = iota + 1
	levelDBBinaryPreparedString
	levelDBBinaryPreparedBytes
)

type levelDBBinaryPreparedValue struct {
	kind        levelDBBinaryPreparedValueKind
	counter     int64
	stringValue string
	bytes       []byte
	encodedSize int64
}

func prepareLevelDBBinaryEntryValue(entry snapshotEntry) (levelDBBinaryPreparedValue, error) {
	switch entry.Type {
	case "counter":
		return levelDBBinaryPreparedValue{
			kind:        levelDBBinaryPreparedCounter,
			counter:     int64(entry.Counter),
			encodedSize: int64(binaryVarintSize(int64(entry.Counter))),
		}, nil
	case "string":
		size, err := binaryLengthPrefixedSize(int64(len(entry.String)))
		if err != nil {
			return levelDBBinaryPreparedValue{}, err
		}
		return levelDBBinaryPreparedValue{
			kind:        levelDBBinaryPreparedString,
			stringValue: entry.String,
			encodedSize: size,
		}, nil
	case "bytes":
		raw, err := snapshotEntryBytesValue(entry)
		if err != nil {
			return levelDBBinaryPreparedValue{}, err
		}
		size, err := binaryLengthPrefixedSize(int64(len(raw)))
		if err != nil {
			return levelDBBinaryPreparedValue{}, err
		}
		return levelDBBinaryPreparedValue{
			kind:        levelDBBinaryPreparedBytes,
			bytes:       raw,
			encodedSize: size,
		}, nil
	case "map":
		return prepareLevelDBBinaryCollectionValue(entry.Map)
	case "slice":
		return prepareLevelDBBinaryCollectionValue(entry.Slice)
	case "set":
		return prepareLevelDBBinaryCollectionValue(entry.Set)
	case "priority_queue":
		return prepareLevelDBBinaryPriorityQueueValue(entry.PriorityQueue)
	case "radix_tree":
		if entry.RadixTree == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: radix tree snapshot is required")
		}
		return prepareLevelDBBinaryRadixTreeValue(*entry.RadixTree)
	case "bloom_filter":
		if entry.BloomFilter == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: bloom filter snapshot is required")
		}
		return prepareLevelDBBinaryBloomFilterValue(*entry.BloomFilter)
	case "count_min_sketch":
		if entry.CountMinSketch == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: count-min sketch snapshot is required")
		}
		return prepareLevelDBBinaryCountMinSketchValue(*entry.CountMinSketch)
	case "hyperloglog":
		if entry.HyperLogLog == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: hyperloglog snapshot is required")
		}
		return prepareLevelDBBinaryHyperLogLogValue(*entry.HyperLogLog)
	case "cuckoo_filter":
		if entry.CuckooFilter == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: cuckoo filter snapshot is required")
		}
		return prepareLevelDBBinaryCuckooFilterValue(*entry.CuckooFilter)
	case "xor_filter":
		if entry.XorFilter == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: xor filter snapshot is required")
		}
		return prepareLevelDBBinaryXorFilterValue(*entry.XorFilter)
	case "roaring_bitmap":
		if entry.RoaringBitmap == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: roaring bitmap snapshot is required")
		}
		return prepareLevelDBBinaryRoaringBitmapValue(*entry.RoaringBitmap)
	case "sparse_bitset":
		if entry.SparseBitset == nil {
			return levelDBBinaryPreparedValue{}, errors.New("hatriecache: sparse bitset snapshot is required")
		}
		return prepareLevelDBBinarySparseBitsetValue(*entry.SparseBitset)
	default:
		payload, err := marshalSnapshotEntryValueJSON(entry)
		if err != nil {
			return levelDBBinaryPreparedValue{}, err
		}
		size, err := binaryLengthPrefixedSize(int64(len(payload)))
		if err != nil {
			return levelDBBinaryPreparedValue{}, err
		}
		return levelDBBinaryPreparedValue{
			kind:        levelDBBinaryPreparedBytes,
			bytes:       payload,
			encodedSize: size,
		}, nil
	}
}

func prepareLevelDBBinaryCollectionValue(value interface{}) (levelDBBinaryPreparedValue, error) {
	payload, ok, err := marshalSnapshotCollectionValueBinary(value)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	if !ok {
		var marshalErr error
		payload, marshalErr = json.Marshal(value)
		if marshalErr != nil {
			return levelDBBinaryPreparedValue{}, marshalErr
		}
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryPriorityQueueValue(items []priorityQueueItem) (levelDBBinaryPreparedValue, error) {
	payload, ok, err := marshalSnapshotPriorityQueueValueBinary(items)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	if !ok {
		var marshalErr error
		payload, marshalErr = json.Marshal(items)
		if marshalErr != nil {
			return levelDBBinaryPreparedValue{}, marshalErr
		}
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryRadixTreeValue(snapshot radixTreeSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, ok, err := marshalSnapshotRadixTreeValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	if !ok {
		var marshalErr error
		payload, marshalErr = json.Marshal(snapshot)
		if marshalErr != nil {
			return levelDBBinaryPreparedValue{}, marshalErr
		}
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryBloomFilterValue(snapshot bloomFilterSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, err := marshalSnapshotBloomFilterValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryCountMinSketchValue(snapshot countMinSketchSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, err := marshalSnapshotCountMinSketchValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryHyperLogLogValue(snapshot hyperLogLogSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, err := marshalSnapshotHyperLogLogValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryCuckooFilterValue(snapshot cuckooFilterSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, err := marshalSnapshotCuckooFilterValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryXorFilterValue(snapshot xorFilterSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, ok, err := marshalSnapshotXorFilterValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	if !ok {
		var marshalErr error
		payload, marshalErr = json.Marshal(snapshot)
		if marshalErr != nil {
			return levelDBBinaryPreparedValue{}, marshalErr
		}
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinaryRoaringBitmapValue(snapshot roaringBitmapSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, err := marshalSnapshotRoaringBitmapValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func prepareLevelDBBinarySparseBitsetValue(snapshot sparseBitsetSnapshot) (levelDBBinaryPreparedValue, error) {
	payload, err := marshalSnapshotSparseBitsetValueBinary(snapshot)
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		return levelDBBinaryPreparedValue{}, err
	}
	return levelDBBinaryPreparedValue{
		kind:        levelDBBinaryPreparedBytes,
		bytes:       payload,
		encodedSize: size,
	}, nil
}

func (writer *levelDBBinaryWriter) writePreparedSnapshotEntryValue(value levelDBBinaryPreparedValue) {
	switch value.kind {
	case levelDBBinaryPreparedCounter:
		writer.writeVarint(value.counter)
	case levelDBBinaryPreparedString:
		writer.writeString(value.stringValue)
	case levelDBBinaryPreparedBytes:
		writer.writeBytes(value.bytes)
	}
}

func (writer *levelDBBinaryWriter) writeTime(value time.Time) {
	if value.IsZero() {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeVarint(value.UnixNano())
}

func (writer *levelDBBinaryWriter) writeTimePtr(value *time.Time) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeVarint(value.UnixNano())
}

func (writer *levelDBBinaryWriter) writeKeyStatsPtr(stats *KeyStats) {
	if stats == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeUvarint(stats.Reads)
	writer.writeUvarint(stats.Hits)
	writer.writeUvarint(stats.Misses)
	writer.writeUvarint(stats.Writes)
	writer.writeTime(stats.LastHit)
	writer.writeTime(stats.LastMiss)
	writer.writeTime(stats.LastWrite)
	writer.writeFloat64(stats.HitRate)
	writer.writeFloat64(stats.CumulativeHitRate)
}

func marshalSnapshotEntryValueJSON(entry snapshotEntry) ([]byte, error) {
	switch entry.Type {
	case "map":
		return json.Marshal(entry.Map)
	case "slice":
		return json.Marshal(entry.Slice)
	case "set":
		return json.Marshal(entry.Set)
	case "priority_queue":
		return json.Marshal(entry.PriorityQueue)
	case "bloom_filter":
		return json.Marshal(entry.BloomFilter)
	case "count_min_sketch":
		return json.Marshal(entry.CountMinSketch)
	case "hyperloglog":
		return json.Marshal(entry.HyperLogLog)
	case "top_k":
		return json.Marshal(entry.TopK)
	case "cuckoo_filter":
		return json.Marshal(entry.CuckooFilter)
	case "roaring_bitmap":
		return json.Marshal(entry.RoaringBitmap)
	case "quantile_sketch":
		return json.Marshal(entry.QuantileSketch)
	case "fenwick_tree":
		return json.Marshal(entry.FenwickTree)
	case "sparse_bitset":
		return json.Marshal(entry.SparseBitset)
	case "reservoir_sample":
		return json.Marshal(entry.ReservoirSample)
	case "xor_filter":
		return json.Marshal(entry.XorFilter)
	case "radix_tree":
		return json.Marshal(entry.RadixTree)
	default:
		return nil, errors.New("hatriecache: unsupported snapshot value type")
	}
}

type levelDBBinaryReader struct {
	binaryFieldReader
}

func unmarshalLevelDBEntryBinary(data []byte) (snapshotEntry, error) {
	if !levelDBEntryDataIsBinary(data) {
		return snapshotEntry{}, errors.New("hatriecache: invalid binary leveldb entry")
	}
	reader := levelDBBinaryReader{binaryFieldReader: newBinaryFieldReader(data[len(levelDBBinaryMagic):])}
	key, err := reader.readString()
	if err != nil {
		return snapshotEntry{}, err
	}
	entryType, err := reader.readString()
	if err != nil {
		return snapshotEntry{}, err
	}
	entry := snapshotEntry{
		Key:  key,
		Type: entryType,
	}
	if err := reader.readSnapshotEntryValue(&entry); err != nil {
		return snapshotEntry{}, err
	}
	entry.ExpiresAt, err = reader.readTimePtr()
	if err != nil {
		return snapshotEntry{}, err
	}
	entry.Stats, err = reader.readKeyStatsPtr()
	if err != nil {
		return snapshotEntry{}, err
	}
	if !reader.done() {
		return snapshotEntry{}, errors.New("hatriecache: invalid trailing binary leveldb entry data")
	}
	return entry, nil
}

func (reader *levelDBBinaryReader) readTime() (time.Time, error) {
	present, err := reader.readBool()
	if err != nil {
		return time.Time{}, err
	}
	if !present {
		return time.Time{}, nil
	}
	unixNano, err := reader.readVarint()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, unixNano), nil
}

func (reader *levelDBBinaryReader) readTimePtr() (*time.Time, error) {
	present, err := reader.readBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	unixNano, err := reader.readVarint()
	if err != nil {
		return nil, err
	}
	value := time.Unix(0, unixNano)
	return &value, nil
}

func (reader *levelDBBinaryReader) readKeyStatsPtr() (*KeyStats, error) {
	present, err := reader.readBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	stats := &KeyStats{}
	if stats.Reads, err = reader.readUvarint(); err != nil {
		return nil, err
	}
	if stats.Hits, err = reader.readUvarint(); err != nil {
		return nil, err
	}
	if stats.Misses, err = reader.readUvarint(); err != nil {
		return nil, err
	}
	if stats.Writes, err = reader.readUvarint(); err != nil {
		return nil, err
	}
	if stats.LastHit, err = reader.readTime(); err != nil {
		return nil, err
	}
	if stats.LastMiss, err = reader.readTime(); err != nil {
		return nil, err
	}
	if stats.LastWrite, err = reader.readTime(); err != nil {
		return nil, err
	}
	if stats.HitRate, err = reader.readFloat64(); err != nil {
		return nil, err
	}
	if stats.CumulativeHitRate, err = reader.readFloat64(); err != nil {
		return nil, err
	}
	return stats, nil
}

func (reader *levelDBBinaryReader) readSnapshotEntryValue(entry *snapshotEntry) error {
	switch entry.Type {
	case "counter":
		value, err := reader.readVarint()
		if err != nil {
			return err
		}
		if value < int64(-1<<31) || value > int64(1<<31-1) {
			return errors.New("hatriecache: binary counter is outside int32 range")
		}
		entry.Counter = int32(value)
	case "string":
		value, err := reader.readString()
		if err != nil {
			return err
		}
		entry.String = value
	case "bytes":
		value, err := reader.readBytes()
		if err != nil {
			return err
		}
		entry.rawBytes = value
	default:
		payload, err := reader.readBytes()
		if err != nil {
			return err
		}
		return unmarshalSnapshotEntryValueJSON(payload, entry)
	}
	return nil
}

func unmarshalSnapshotEntryValueJSON(data []byte, entry *snapshotEntry) error {
	switch entry.Type {
	case "map":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			m, ok := value.(Map)
			if !ok {
				return errors.New("hatriecache: binary map value is not an object")
			}
			entry.Map = m
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.Map)
	case "slice":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			items, ok := value.(Slice)
			if !ok {
				return errors.New("hatriecache: binary slice value is not an array")
			}
			entry.Slice = items
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.Slice)
	case "set":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			items, ok := value.(Slice)
			if !ok {
				return errors.New("hatriecache: binary set value is not an array")
			}
			entry.Set = Set(items)
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.Set)
	case "priority_queue":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			items, ok := value.([]priorityQueueItem)
			if !ok {
				return errors.New("hatriecache: binary priority queue value is not a priority queue")
			}
			entry.PriorityQueue = items
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.PriorityQueue)
	case "bloom_filter":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(bloomFilterSnapshot)
			if !ok {
				return errors.New("hatriecache: binary bloom filter value is not a bloom filter")
			}
			entry.BloomFilter = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.BloomFilter)
	case "count_min_sketch":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(countMinSketchSnapshot)
			if !ok {
				return errors.New("hatriecache: binary count-min sketch value is not a count-min sketch")
			}
			entry.CountMinSketch = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.CountMinSketch)
	case "hyperloglog":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(hyperLogLogSnapshot)
			if !ok {
				return errors.New("hatriecache: binary hyperloglog value is not a hyperloglog")
			}
			entry.HyperLogLog = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.HyperLogLog)
	case "top_k":
		return decodeLevelDBStorageJSON(data, &entry.TopK)
	case "cuckoo_filter":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(cuckooFilterSnapshot)
			if !ok {
				return errors.New("hatriecache: binary cuckoo filter value is not a cuckoo filter")
			}
			entry.CuckooFilter = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.CuckooFilter)
	case "roaring_bitmap":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(roaringBitmapSnapshot)
			if !ok {
				return errors.New("hatriecache: binary roaring bitmap value is not a roaring bitmap")
			}
			entry.RoaringBitmap = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.RoaringBitmap)
	case "quantile_sketch":
		return decodeLevelDBStorageJSON(data, &entry.QuantileSketch)
	case "fenwick_tree":
		return decodeLevelDBStorageJSON(data, &entry.FenwickTree)
	case "sparse_bitset":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(sparseBitsetSnapshot)
			if !ok {
				return errors.New("hatriecache: binary sparse bitset value is not a sparse bitset")
			}
			entry.SparseBitset = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.SparseBitset)
	case "reservoir_sample":
		return decodeLevelDBStorageJSON(data, &entry.ReservoirSample)
	case "xor_filter":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(xorFilterSnapshot)
			if !ok {
				return errors.New("hatriecache: binary xor filter value is not an xor filter")
			}
			entry.XorFilter = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.XorFilter)
	case "radix_tree":
		if snapshotValueDataIsBinary(data) {
			value, err := unmarshalSnapshotValueBinary(data)
			if err != nil {
				return err
			}
			snapshot, ok := value.(radixTreeSnapshot)
			if !ok {
				return errors.New("hatriecache: binary radix tree value is not a radix tree")
			}
			entry.RadixTree = &snapshot
			return nil
		}
		return decodeLevelDBStorageJSON(data, &entry.RadixTree)
	default:
		return errors.New("hatriecache: unsupported snapshot value type")
	}
}

func decodeLevelDBStorageJSON(data []byte, value interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("hatriecache: invalid storage JSON")
		}
		return err
	}
	return nil
}

func writeLevelDBRecordSnapshotJSON(writer io.Writer, key string, data []byte, prefix string) error {
	if !levelDBEntryDataIsBinary(data) {
		return writeSnapshotRawEntryJSON(writer, data, prefix)
	}
	entry, err := decodeLevelDBEntryForKey(key, data)
	if err != nil {
		return err
	}
	return writeSnapshotEntryFieldsJSON(writer, entry, prefix)
}
