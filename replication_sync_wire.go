package hatriecache

import (
	"errors"
	"io"
	"math"
	"strconv"
	"sync"
	"unsafe"

	"hatrie_cache/internal/jsonwire"

	"google.golang.org/protobuf/encoding/protowire"
)

const replicationProtoChunkSize = 64 << 10
const maxReplicationSyncArenaInitialEntries = 64 << 10

var replicationProtoChunkPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, replicationProtoChunkSize)
	},
}

type replicationSyncPayload struct {
	key          string
	binaryValue  []byte
	payloadBytes int
}

type replicationSyncPayloadArenaRecord struct {
	keyOffset    uint32
	keyLength    uint32
	valueOffset  uint32
	valueLength  uint32
	payloadBytes uint32
}

type replicationSyncPayloadDirectRecord struct {
	key         string
	valueOffset uint32
	valueLength uint32
}

type replicationSyncPayloadArena struct {
	keys          []byte
	values        []byte
	records       []replicationSyncPayloadArenaRecord
	directRecords []replicationSyncPayloadDirectRecord
}

func newReplicationSyncPayloadArena(capacity int) *replicationSyncPayloadArena {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxReplicationSyncArenaInitialEntries {
		capacity = maxReplicationSyncArenaInitialEntries
	}
	return &replicationSyncPayloadArena{
		keys:    make([]byte, 0, boundedReplicationSyncArenaCapacity(capacity, 12)),
		values:  make([]byte, 0, boundedReplicationSyncArenaCapacity(capacity, 24)),
		records: make([]replicationSyncPayloadArenaRecord, 0, capacity),
	}
}

func newReplicationSyncPayloadDirectArena(capacity int) *replicationSyncPayloadArena {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxReplicationSyncArenaInitialEntries {
		capacity = maxReplicationSyncArenaInitialEntries
	}
	return &replicationSyncPayloadArena{
		values:        make([]byte, 0, boundedReplicationSyncArenaCapacity(capacity, 24)),
		directRecords: make([]replicationSyncPayloadDirectRecord, 0, capacity),
	}
}

func boundedReplicationSyncArenaCapacity(entries int, bytesPerEntry int) int {
	if entries <= 0 || bytesPerEntry <= 0 || entries > math.MaxInt/bytesPerEntry {
		return 0
	}
	return entries * bytesPerEntry
}

func (arena *replicationSyncPayloadArena) append(key string, value []byte, payloadBytes int) (uint32, error) {
	if arena == nil {
		return 0, errors.New("hatriecache: replication sync payload arena is nil")
	}
	valueOffset := len(arena.values)
	arena.values = append(arena.values, value...)
	return arena.appendRecord(key, valueOffset, len(value), payloadBytes)
}

func (arena *replicationSyncPayloadArena) appendRecord(key string, valueOffset int, valueLength int, payloadBytes int) (uint32, error) {
	if arena == nil {
		return 0, errors.New("hatriecache: replication sync payload arena is nil")
	}
	keyOffset := len(arena.keys)
	if valueOffset < 0 || valueOffset > len(arena.values) || valueLength < 0 || valueLength > len(arena.values)-valueOffset {
		return 0, errors.New("hatriecache: replication sync payload value is outside arena")
	}
	if uint64(len(arena.records)) > uint64(math.MaxUint32) ||
		uint64(keyOffset)+uint64(len(key)) > uint64(math.MaxUint32) ||
		uint64(valueOffset)+uint64(valueLength) > uint64(math.MaxUint32) ||
		payloadBytes < 0 || uint64(payloadBytes) > uint64(math.MaxUint32) {
		return 0, errors.New("hatriecache: replication sync payload arena exceeds 4 GiB")
	}
	arena.keys = append(arena.keys, key...)
	recordIndex := uint32(len(arena.records))
	arena.records = append(arena.records, replicationSyncPayloadArenaRecord{
		keyOffset:    uint32(keyOffset),
		keyLength:    uint32(len(key)),
		valueOffset:  uint32(valueOffset),
		valueLength:  uint32(valueLength),
		payloadBytes: uint32(payloadBytes),
	})
	return recordIndex, nil
}

func (arena *replicationSyncPayloadArena) appendDirectRecord(key string, valueOffset int, valueLength int) error {
	if arena == nil {
		return errors.New("hatriecache: replication sync payload arena is nil")
	}
	if valueOffset < 0 || valueOffset > len(arena.values) || valueLength < 0 || valueLength > len(arena.values)-valueOffset {
		return errors.New("hatriecache: replication sync payload value is outside arena")
	}
	if uint64(len(arena.directRecords)) >= uint64(math.MaxUint32) ||
		uint64(valueOffset)+uint64(valueLength) > uint64(math.MaxUint32) {
		return errors.New("hatriecache: replication sync payload arena exceeds 4 GiB")
	}
	arena.directRecords = append(arena.directRecords, replicationSyncPayloadDirectRecord{
		key:         key,
		valueOffset: uint32(valueOffset),
		valueLength: uint32(valueLength),
	})
	return nil
}

func (arena *replicationSyncPayloadArena) payload(index uint32) replicationSyncPayload {
	if arena == nil || uint64(index) >= uint64(len(arena.records)) {
		return replicationSyncPayload{}
	}
	record := arena.records[index]
	keyBytes := arena.keys[int(record.keyOffset):int(record.keyOffset+record.keyLength)]
	value := arena.values[int(record.valueOffset):int(record.valueOffset+record.valueLength)]
	key := ""
	if len(keyBytes) > 0 {
		key = unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))
	}
	return replicationSyncPayload{key: key, binaryValue: value, payloadBytes: int(record.payloadBytes)}
}

func (arena *replicationSyncPayloadArena) directPayload(index uint32) replicationSyncPayload {
	if arena == nil || uint64(index) >= uint64(len(arena.directRecords)) {
		return replicationSyncPayload{}
	}
	record := arena.directRecords[index]
	value := arena.values[int(record.valueOffset):int(record.valueOffset+record.valueLength)]
	return replicationSyncPayload{key: record.key, binaryValue: value}
}

type replicationSyncPayloadBatch struct {
	inline  []replicationSyncPayload
	arena   *replicationSyncPayloadArena
	indexes []uint32
	start   uint32
	count   uint32
}

func (batch replicationSyncPayloadBatch) len() int {
	if batch.arena != nil {
		if batch.indexes == nil {
			return int(batch.count)
		}
		return len(batch.indexes)
	}
	return len(batch.inline)
}

func (batch replicationSyncPayloadBatch) payload(index int) replicationSyncPayload {
	if batch.arena != nil {
		if batch.indexes == nil {
			if batch.arena.directRecords != nil {
				return batch.arena.directPayload(batch.start + uint32(index))
			}
			return batch.arena.payload(batch.start + uint32(index))
		}
		return batch.arena.payload(batch.indexes[index])
	}
	return batch.inline[index]
}

func replicationSyncBatchRequestBody(payloads []replicationSyncPayload, command string, source string, sequence uint64, fingerprint string, compressionThreshold int) (io.Reader, string, string, error) {
	return replicationSyncBatchRequestBodyBatch(replicationSyncPayloadBatch{inline: payloads}, command, source, sequence, fingerprint, compressionThreshold)
}

func replicationSyncBatchRequestBodyBatch(payloads replicationSyncPayloadBatch, command string, source string, sequence uint64, fingerprint string, compressionThreshold int) (io.Reader, string, string, error) {
	size := replicationSyncBatchProtoSizeBatch(payloads, command, source, sequence, fingerprint)
	if compressionThreshold > 0 && size >= compressionThreshold {
		body := jsonwire.StreamingGzipWriterReader(func(writer io.Writer) error {
			return writeReplicationSyncBatchProtoBatch(writer, payloads, command, source, sequence, fingerprint)
		})
		return body, commandWireContentTypeProtobuf, "gzip", nil
	}

	data := acquireCommandWireBuffer(size)
	data = appendReplicationSyncBatchProtoBatch(data, payloads, command, source, sequence, fingerprint)
	body, contentEncoding, err := jsonwire.EncodedRequestBodyWithRelease(data, 0, releaseCommandWireBuffer)
	if err != nil {
		releaseCommandWireBuffer(data)
		return nil, "", "", err
	}
	return body, commandWireContentTypeProtobuf, contentEncoding, nil
}

func replicationSyncBatchProtoSize(payloads []replicationSyncPayload, command string, source string, sequence uint64, fingerprint string) int {
	return replicationSyncBatchProtoSizeBatch(replicationSyncPayloadBatch{inline: payloads}, command, source, sequence, fingerprint)
}

func replicationSyncBatchProtoSizeBatch(payloads replicationSyncPayloadBatch, command string, source string, sequence uint64, fingerprint string) int {
	size := protoStringFieldSize(1, replicationBatchEnvelopeCommand)
	if source != "" {
		size += protoMapStringEntryFieldSize(6, replicationMetaSourceNode, source)
	}
	if sequence > 0 {
		size += protoMapStringEntryFieldSize(6, replicationMetaSequence, strconv.FormatUint(sequence, 10))
	}
	if fingerprint != "" {
		size += protoMapStringEntryFieldSize(6, replicationMetaTopologyFingerprint, fingerprint)
	}
	for idx := 0; idx < payloads.len(); idx++ {
		payload := payloads.payload(idx)
		childSize := replicationSyncPayloadProtoSize(payload, command)
		size += protowire.SizeTag(10) + protowire.SizeBytes(childSize)
	}
	return size
}

func replicationSyncPayloadProtoSize(payload replicationSyncPayload, command string) int {
	return protoStringFieldSize(1, command) +
		protoStringFieldSize(2, payload.key) +
		protoBytesFieldSize(11, len(payload.binaryValue))
}

func protoStringFieldSize(number protowire.Number, value string) int {
	return protowire.SizeTag(number) + protowire.SizeBytes(len(value))
}

func protoBytesFieldSize(number protowire.Number, size int) int {
	return protowire.SizeTag(number) + protowire.SizeBytes(size)
}

func protoMapStringEntryFieldSize(number protowire.Number, key string, value string) int {
	entrySize := protoStringFieldSize(1, key) + protoStringFieldSize(2, value)
	return protowire.SizeTag(number) + protowire.SizeBytes(entrySize)
}

func appendReplicationSyncBatchProto(data []byte, payloads []replicationSyncPayload, command string, source string, sequence uint64, fingerprint string) []byte {
	return appendReplicationSyncBatchProtoBatch(data, replicationSyncPayloadBatch{inline: payloads}, command, source, sequence, fingerprint)
}

func appendReplicationSyncBatchProtoBatch(data []byte, payloads replicationSyncPayloadBatch, command string, source string, sequence uint64, fingerprint string) []byte {
	data = appendProtoStringField(data, 1, replicationBatchEnvelopeCommand)
	if source != "" {
		data = appendProtoMapStringEntryField(data, 6, replicationMetaSourceNode, source)
	}
	if sequence > 0 {
		data = appendProtoMapStringEntryField(data, 6, replicationMetaSequence, strconv.FormatUint(sequence, 10))
	}
	if fingerprint != "" {
		data = appendProtoMapStringEntryField(data, 6, replicationMetaTopologyFingerprint, fingerprint)
	}
	for idx := 0; idx < payloads.len(); idx++ {
		data = appendReplicationSyncPayloadProto(data, payloads.payload(idx), command)
	}
	return data
}

func appendReplicationSyncPayloadProto(data []byte, payload replicationSyncPayload, command string) []byte {
	childSize := replicationSyncPayloadProtoSize(payload, command)
	data = protowire.AppendTag(data, 10, protowire.BytesType)
	data = protowire.AppendVarint(data, uint64(childSize))
	data = appendProtoStringField(data, 1, command)
	data = appendProtoStringField(data, 2, payload.key)
	return appendProtoBytesField(data, 11, payload.binaryValue)
}

func appendProtoStringField(data []byte, number protowire.Number, value string) []byte {
	data = protowire.AppendTag(data, number, protowire.BytesType)
	return protowire.AppendString(data, value)
}

func appendProtoBytesField(data []byte, number protowire.Number, value []byte) []byte {
	data = protowire.AppendTag(data, number, protowire.BytesType)
	return protowire.AppendBytes(data, value)
}

func appendProtoMapStringEntryField(data []byte, number protowire.Number, key string, value string) []byte {
	entrySize := protoStringFieldSize(1, key) + protoStringFieldSize(2, value)
	data = protowire.AppendTag(data, number, protowire.BytesType)
	data = protowire.AppendVarint(data, uint64(entrySize))
	data = appendProtoStringField(data, 1, key)
	return appendProtoStringField(data, 2, value)
}

func writeReplicationSyncBatchProto(writer io.Writer, payloads []replicationSyncPayload, command string, source string, sequence uint64, fingerprint string) error {
	return writeReplicationSyncBatchProtoBatch(writer, replicationSyncPayloadBatch{inline: payloads}, command, source, sequence, fingerprint)
}

func writeReplicationSyncBatchProtoBatch(writer io.Writer, payloads replicationSyncPayloadBatch, command string, source string, sequence uint64, fingerprint string) error {
	chunk := replicationProtoChunkPool.Get().([]byte)[:0]
	defer replicationProtoChunkPool.Put(chunk[:0])

	chunk = appendProtoStringField(chunk, 1, replicationBatchEnvelopeCommand)
	if source != "" {
		chunk = appendProtoMapStringEntryField(chunk, 6, replicationMetaSourceNode, source)
	}
	if sequence > 0 {
		chunk = appendProtoMapStringEntryField(chunk, 6, replicationMetaSequence, strconv.FormatUint(sequence, 10))
	}
	if fingerprint != "" {
		chunk = appendProtoMapStringEntryField(chunk, 6, replicationMetaTopologyFingerprint, fingerprint)
	}
	for idx := 0; idx < payloads.len(); idx++ {
		payload := payloads.payload(idx)
		payloadSize := protowire.SizeTag(10) + protowire.SizeBytes(replicationSyncPayloadProtoSize(payload, command))
		if payloadSize > cap(chunk) {
			if err := writeFullBytes(writer, chunk); err != nil {
				return err
			}
			chunk = chunk[:0]
			if err := writeReplicationSyncPayloadProto(writer, payload, command); err != nil {
				return err
			}
			continue
		}
		if len(chunk)+payloadSize > cap(chunk) {
			if err := writeFullBytes(writer, chunk); err != nil {
				return err
			}
			chunk = chunk[:0]
		}
		chunk = appendReplicationSyncPayloadProto(chunk, payload, command)
	}
	return writeFullBytes(writer, chunk)
}

func writeReplicationSyncPayloadProto(writer io.Writer, payload replicationSyncPayload, command string) error {
	if err := writeProtoMessageHeader(writer, 10, replicationSyncPayloadProtoSize(payload, command)); err != nil {
		return err
	}
	if err := writeProtoStringField(writer, 1, command); err != nil {
		return err
	}
	if err := writeProtoStringField(writer, 2, payload.key); err != nil {
		return err
	}
	return writeProtoBytesField(writer, 11, payload.binaryValue)
}

func writeProtoStringField(writer io.Writer, number protowire.Number, value string) error {
	if err := writeProtoMessageHeader(writer, number, len(value)); err != nil {
		return err
	}
	return writeFullString(writer, value)
}

func writeProtoBytesField(writer io.Writer, number protowire.Number, value []byte) error {
	if err := writeProtoMessageHeader(writer, number, len(value)); err != nil {
		return err
	}
	return writeFullBytes(writer, value)
}

func writeProtoMapStringEntryField(writer io.Writer, number protowire.Number, key string, value string) error {
	entrySize := protoStringFieldSize(1, key) + protoStringFieldSize(2, value)
	if err := writeProtoMessageHeader(writer, number, entrySize); err != nil {
		return err
	}
	if err := writeProtoStringField(writer, 1, key); err != nil {
		return err
	}
	return writeProtoStringField(writer, 2, value)
}

func writeProtoMessageHeader(writer io.Writer, number protowire.Number, size int) error {
	var scratch [20]byte
	header := protowire.AppendTag(scratch[:0], number, protowire.BytesType)
	header = protowire.AppendVarint(header, uint64(size))
	return writeFullBytes(writer, header)
}

func writeFullString(writer io.Writer, value string) error {
	written, err := io.WriteString(writer, value)
	if err == nil && written != len(value) {
		return io.ErrShortWrite
	}
	return err
}

func writeFullBytes(writer io.Writer, value []byte) error {
	written, err := writer.Write(value)
	if err == nil && written != len(value) {
		return io.ErrShortWrite
	}
	return err
}
