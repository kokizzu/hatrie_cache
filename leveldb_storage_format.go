package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
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
	writer := newLevelDBBinaryWriter()
	writer.writeString(entry.Key)
	writer.writeString(entry.Type)
	if err := writer.writeSnapshotEntryValue(entry); err != nil {
		return nil, err
	}
	writer.writeTimePtr(entry.ExpiresAt)
	writer.writeKeyStatsPtr(entry.Stats)
	return writer.bytes(), nil
}

func marshalLevelDBBytesEntryBinary(key string, raw []byte, expiresAt *time.Time, stats *KeyStats) ([]byte, error) {
	writer := newLevelDBBinaryWriter()
	writer.writeString(key)
	writer.writeString("bytes")
	writer.writeBytes(raw)
	writer.writeTimePtr(expiresAt)
	writer.writeKeyStatsPtr(stats)
	return writer.bytes(), nil
}

type levelDBBinaryWriter struct {
	buf []byte
}

func newLevelDBBinaryWriter() levelDBBinaryWriter {
	out := make([]byte, 0, 128)
	out = append(out, levelDBBinaryMagic...)
	return levelDBBinaryWriter{buf: out}
}

func (writer *levelDBBinaryWriter) bytes() []byte {
	return writer.buf
}

func (writer *levelDBBinaryWriter) writeBool(value bool) {
	if value {
		writer.buf = append(writer.buf, 1)
		return
	}
	writer.buf = append(writer.buf, 0)
}

func (writer *levelDBBinaryWriter) writeUvarint(value uint64) {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	writer.buf = append(writer.buf, scratch[:n]...)
}

func (writer *levelDBBinaryWriter) writeVarint(value int64) {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutVarint(scratch[:], value)
	writer.buf = append(writer.buf, scratch[:n]...)
}

func (writer *levelDBBinaryWriter) writeBytes(value []byte) {
	writer.writeUvarint(uint64(len(value)))
	writer.buf = append(writer.buf, value...)
}

func (writer *levelDBBinaryWriter) writeString(value string) {
	writer.writeBytes([]byte(value))
}

func (writer *levelDBBinaryWriter) writeFloat64(value float64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], math.Float64bits(value))
	writer.buf = append(writer.buf, scratch[:]...)
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

func (writer *levelDBBinaryWriter) writeSnapshotEntryValue(entry snapshotEntry) error {
	switch entry.Type {
	case "counter":
		writer.writeVarint(int64(entry.Counter))
	case "string":
		writer.writeString(entry.String)
	case "bytes":
		raw, err := base64.StdEncoding.DecodeString(entry.Bytes)
		if err != nil {
			return err
		}
		writer.writeBytes(raw)
	default:
		payload, err := marshalSnapshotEntryValueJSON(entry)
		if err != nil {
			return err
		}
		writer.writeBytes(payload)
	}
	return nil
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
	data []byte
	off  int
}

func unmarshalLevelDBEntryBinary(data []byte) (snapshotEntry, error) {
	if !levelDBEntryDataIsBinary(data) {
		return snapshotEntry{}, errors.New("hatriecache: invalid binary leveldb entry")
	}
	reader := levelDBBinaryReader{data: data[len(levelDBBinaryMagic):]}
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

func (reader *levelDBBinaryReader) done() bool {
	return reader.off == len(reader.data)
}

func (reader *levelDBBinaryReader) readBool() (bool, error) {
	if reader.off >= len(reader.data) {
		return false, io.ErrUnexpectedEOF
	}
	value := reader.data[reader.off]
	reader.off++
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errors.New("hatriecache: invalid binary boolean")
	}
}

func (reader *levelDBBinaryReader) readUvarint() (uint64, error) {
	value, n := binary.Uvarint(reader.data[reader.off:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, errors.New("hatriecache: invalid binary unsigned integer")
	}
	reader.off += n
	return value, nil
}

func (reader *levelDBBinaryReader) readVarint() (int64, error) {
	value, n := binary.Varint(reader.data[reader.off:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, errors.New("hatriecache: invalid binary signed integer")
	}
	reader.off += n
	return value, nil
}

func (reader *levelDBBinaryReader) readBytes() ([]byte, error) {
	size, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	if size > uint64(len(reader.data)-reader.off) {
		return nil, io.ErrUnexpectedEOF
	}
	start := reader.off
	reader.off += int(size)
	return reader.data[start:reader.off], nil
}

func (reader *levelDBBinaryReader) readString() (string, error) {
	value, err := reader.readBytes()
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (reader *levelDBBinaryReader) readFloat64() (float64, error) {
	if len(reader.data)-reader.off < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	value := binary.LittleEndian.Uint64(reader.data[reader.off : reader.off+8])
	reader.off += 8
	return math.Float64frombits(value), nil
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
		entry.Bytes = base64.StdEncoding.EncodeToString(value)
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
		return decodeLevelDBStorageJSON(data, &entry.Map)
	case "slice":
		return decodeLevelDBStorageJSON(data, &entry.Slice)
	case "set":
		return decodeLevelDBStorageJSON(data, &entry.Set)
	case "priority_queue":
		return decodeLevelDBStorageJSON(data, &entry.PriorityQueue)
	case "bloom_filter":
		return decodeLevelDBStorageJSON(data, &entry.BloomFilter)
	case "count_min_sketch":
		return decodeLevelDBStorageJSON(data, &entry.CountMinSketch)
	case "hyperloglog":
		return decodeLevelDBStorageJSON(data, &entry.HyperLogLog)
	case "top_k":
		return decodeLevelDBStorageJSON(data, &entry.TopK)
	case "cuckoo_filter":
		return decodeLevelDBStorageJSON(data, &entry.CuckooFilter)
	case "roaring_bitmap":
		return decodeLevelDBStorageJSON(data, &entry.RoaringBitmap)
	case "quantile_sketch":
		return decodeLevelDBStorageJSON(data, &entry.QuantileSketch)
	case "fenwick_tree":
		return decodeLevelDBStorageJSON(data, &entry.FenwickTree)
	case "sparse_bitset":
		return decodeLevelDBStorageJSON(data, &entry.SparseBitset)
	case "reservoir_sample":
		return decodeLevelDBStorageJSON(data, &entry.ReservoirSample)
	case "xor_filter":
		return decodeLevelDBStorageJSON(data, &entry.XorFilter)
	case "radix_tree":
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
