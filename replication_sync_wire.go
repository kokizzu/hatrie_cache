package hatriecache

import (
	"io"
	"strconv"
	"sync"

	"hatrie_cache/internal/jsonwire"

	"google.golang.org/protobuf/encoding/protowire"
)

const replicationProtoChunkSize = 64 << 10

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

func replicationSyncBatchRequestBody(payloads []replicationSyncPayload, command string, source string, sequence uint64, fingerprint string, compressionThreshold int) (io.Reader, string, string, error) {
	size := replicationSyncBatchProtoSize(payloads, command, source, sequence, fingerprint)
	if compressionThreshold > 0 && size >= compressionThreshold {
		body := jsonwire.StreamingGzipWriterReader(func(writer io.Writer) error {
			return writeReplicationSyncBatchProto(writer, payloads, command, source, sequence, fingerprint)
		})
		return body, commandWireContentTypeProtobuf, "gzip", nil
	}

	data := acquireCommandWireBuffer(size)
	data = appendReplicationSyncBatchProto(data, payloads, command, source, sequence, fingerprint)
	body, contentEncoding, err := jsonwire.EncodedRequestBodyWithRelease(data, 0, releaseCommandWireBuffer)
	if err != nil {
		releaseCommandWireBuffer(data)
		return nil, "", "", err
	}
	return body, commandWireContentTypeProtobuf, contentEncoding, nil
}

func replicationSyncBatchProtoSize(payloads []replicationSyncPayload, command string, source string, sequence uint64, fingerprint string) int {
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
	for _, payload := range payloads {
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
	for _, payload := range payloads {
		data = appendReplicationSyncPayloadProto(data, payload, command)
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
	for _, payload := range payloads {
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
