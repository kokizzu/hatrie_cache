package hatriecache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	json "github.com/goccy/go-json"
)

type CommandJournalFormat string

const (
	CommandJournalFormatJSON   CommandJournalFormat = "json"
	CommandJournalFormatBinary CommandJournalFormat = "binary"
)

const DefaultCommandJournalFormat = CommandJournalFormatBinary

var commandJournalBinaryMagic = []byte{'h', 'c', 'j', 'n', 1}

func ParseCommandJournalFormat(value string) (CommandJournalFormat, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(CommandJournalFormatBinary), "bin":
		return CommandJournalFormatBinary, nil
	case string(CommandJournalFormatJSON):
		return CommandJournalFormatJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported command journal format %q", value)
	}
}

func marshalCommandJournalEntry(entry commandJournalEntry, format CommandJournalFormat) ([]byte, error) {
	format, err := ParseCommandJournalFormat(string(format))
	if err != nil {
		return nil, err
	}
	switch format {
	case CommandJournalFormatJSON:
		return marshalCommandJournalEntryJSON(entry)
	case CommandJournalFormatBinary:
		return marshalCommandJournalEntryBinary(entry)
	default:
		return nil, fmt.Errorf("hatriecache: unsupported command journal format %q", format)
	}
}

func marshalCommandJournalEntryJSON(entry commandJournalEntry) ([]byte, error) {
	return marshalCommandJournalEntryJSONLimited(entry, maxCommandJournalJSONRecordBytes)
}

func marshalCommandJournalEntryJSONLimited(entry commandJournalEntry, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, errCommandJournalJSONRecordTooLarge
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	if len(payload)+1 > limit {
		return nil, errCommandJournalJSONRecordTooLarge
	}
	payload = append(payload, '\n')
	return payload, nil
}

func commandJournalRecordIsBinary(data []byte) bool {
	return bytes.HasPrefix(data, commandJournalBinaryMagic)
}

func marshalCommandJournalEntryBinary(entry commandJournalEntry) ([]byte, error) {
	fields, err := prepareCommandJournalBinaryEntryFields(entry)
	if err != nil {
		return nil, err
	}
	payloadBytes, err := commandJournalEntryBinaryPayloadCapacity(entry, len(fields.values), len(fields.pairs), len(fields.outbox))
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, commandJournalBinaryRecordCapacity(payloadBytes))
	return appendPreparedCommandJournalEntryBinary(data, entry, fields, payloadBytes)
}

type commandJournalBinaryEntryFields struct {
	values []byte
	pairs  []byte
	outbox []byte
}

func prepareCommandJournalBinaryEntryFields(entry commandJournalEntry) (commandJournalBinaryEntryFields, error) {
	values, pairs, err := marshalCommandJournalRequestBinaryDynamicFields(entry.Request)
	if err != nil {
		return commandJournalBinaryEntryFields{}, err
	}
	var outbox []byte
	if entry.Outbox != nil {
		outbox, err = marshalReplicationOutboxJobBinary(*entry.Outbox)
		if err != nil {
			return commandJournalBinaryEntryFields{}, err
		}
	}
	return commandJournalBinaryEntryFields{values: values, pairs: pairs, outbox: outbox}, nil
}

func marshalCommandJournalEntryBinaryPayload(entry commandJournalEntry) ([]byte, error) {
	fields, err := prepareCommandJournalBinaryEntryFields(entry)
	if err != nil {
		return nil, err
	}
	payloadBytes, err := commandJournalEntryBinaryPayloadCapacity(entry, len(fields.values), len(fields.pairs), len(fields.outbox))
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(nil, payloadBytes)
	if err := writePreparedCommandJournalEntryBinaryPayload(&writer, entry, fields); err != nil {
		return nil, err
	}
	return writer.bytes(), nil
}

func appendCommandJournalEntryBinary(data []byte, entry commandJournalEntry) ([]byte, error) {
	fields, err := prepareCommandJournalBinaryEntryFields(entry)
	if err != nil {
		return nil, err
	}
	payloadBytes, err := commandJournalEntryBinaryPayloadCapacity(entry, len(fields.values), len(fields.pairs), len(fields.outbox))
	if err != nil {
		return nil, err
	}
	return appendPreparedCommandJournalEntryBinary(data, entry, fields, payloadBytes)
}

func appendPreparedCommandJournalEntryBinary(data []byte, entry commandJournalEntry, fields commandJournalBinaryEntryFields, payloadBytes int) ([]byte, error) {
	writer := binaryFieldWriter{buf: data}
	writer.buf = append(writer.buf, commandJournalBinaryMagic...)
	writer.writeUvarint(uint64(payloadBytes))
	if err := writePreparedCommandJournalEntryBinaryPayload(&writer, entry, fields); err != nil {
		return nil, err
	}
	return writer.bytes(), nil
}

func writePreparedCommandJournalEntryBinaryPayload(writer *binaryFieldWriter, entry commandJournalEntry, fields commandJournalBinaryEntryFields) error {
	writer.writeUvarint(commandJournalBinaryPayloadVersion)
	writer.writeUvarint(entry.Sequence)
	writer.writeBool(entry.Checkpoint)
	if err := writeCommandJournalRequestBinaryFields(writer, entry.Request, fields.values, fields.pairs); err != nil {
		return err
	}
	writer.writeBytes(fields.outbox)
	return nil
}

func commandJournalBinaryRecordCapacity(payloadBytes int) int {
	return len(commandJournalBinaryMagic) + binaryUvarintSize(uint64(payloadBytes)) + payloadBytes
}

func commandJournalEntryBinaryPayloadCapacity(entry commandJournalEntry, valuesBytes int, pairsBytes int, optionalOutboxBytes ...int) (int, error) {
	outboxBytes := 0
	includeOutbox := len(optionalOutboxBytes) > 0
	if len(optionalOutboxBytes) > 0 {
		outboxBytes = optionalOutboxBytes[0]
	}
	total := int64(binaryUvarintSize(commandJournalBinaryPayloadVersion))
	var err error
	requestSize, err := commandJournalRequestBinarySize(entry.Request, valuesBytes, pairsBytes)
	if err != nil {
		return 0, err
	}
	sizes := [4]int64{int64(binaryUvarintSize(entry.Sequence)), 1, requestSize}
	sizeCount := 3
	if includeOutbox {
		sizes[sizeCount] = commandJournalBinaryBytesSize(outboxBytes)
		sizeCount++
	}
	for _, size := range sizes[:sizeCount] {
		total, err = addCommandJournalBinaryPayloadSize(total, size)
		if err != nil {
			return 0, err
		}
	}
	if err := validateCommandJournalBinaryRecordSize(uint64(total)); err != nil {
		return 0, err
	}
	return int(total), nil
}

func commandJournalRequestBinarySize(request CacheCommandRequest, valuesBytes int, pairsBytes int) (int64, error) {
	total := int64(0)
	for _, size := range []int64{
		commandJournalBinaryStringSize(request.Command),
		commandJournalBinaryStringSize(request.Key),
		commandJournalBinaryStringSize(request.Value),
		commandJournalBinaryStringSize(request.Subkey),
		int64(commandJournalOptionalInt64BinarySize(request.Priority)),
		int64(commandJournalOptionalInt64BinarySize(request.TTLSeconds)),
		int64(commandJournalOptionalInt64BinarySize(request.UnixSeconds)),
		commandJournalBinaryBytesSize(valuesBytes),
		commandJournalBinaryBytesSize(pairsBytes),
	} {
		next, err := addCommandJournalBinaryPayloadSize(total, size)
		if err != nil {
			return 0, err
		}
		total = next
	}
	return total, nil
}

func commandJournalBinaryStringSize(value string) int64 {
	return commandJournalBinaryBytesSize(len(value))
}

func commandJournalBinaryBytesSize(size int) int64 {
	return int64(binaryUvarintSize(uint64(size)) + size)
}

func commandJournalOptionalInt64BinarySize(value *int64) int {
	if value == nil {
		return 1
	}
	return 1 + binaryVarintSize(*value)
}

func addCommandJournalBinaryPayloadSize(left int64, right int64) (int64, error) {
	if right < 0 || left > int64(maxCommandJournalBinaryRecordBytes)-right {
		return 0, errCommandJournalBinaryRecordTooLarge
	}
	return left + right, nil
}

func writeCommandJournalOptionalInt64Binary(writer *binaryFieldWriter, value *int64) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeVarint(*value)
}

func marshalCommandJournalRequestDynamicFields(request CacheCommandRequest) ([]byte, []byte, error) {
	values, err := marshalJournalDynamicJSON(request.Values)
	if err != nil {
		return nil, nil, err
	}
	pairs, err := marshalJournalDynamicJSON(request.Pairs)
	if err != nil {
		return nil, nil, err
	}
	return values, pairs, nil
}

func marshalCommandJournalRequestBinaryDynamicFields(request CacheCommandRequest) ([]byte, []byte, error) {
	values, err := marshalJournalDynamicBinary(request.Values)
	if err != nil {
		return nil, nil, err
	}
	pairs, err := marshalJournalDynamicBinary(request.Pairs)
	if err != nil {
		return nil, nil, err
	}
	return values, pairs, nil
}

func writeCommandJournalRequestBinaryFields(writer *binaryFieldWriter, request CacheCommandRequest, values []byte, pairs []byte) error {
	writer.writeString(request.Command)
	writer.writeString(request.Key)
	writer.writeString(request.Value)
	writer.writeString(request.Subkey)
	writeCommandJournalOptionalInt64Binary(writer, request.Priority)
	writeCommandJournalOptionalInt64Binary(writer, request.TTLSeconds)
	writeCommandJournalOptionalInt64Binary(writer, request.UnixSeconds)
	writer.writeBytes(values)
	writer.writeBytes(pairs)
	return nil
}

func marshalJournalDynamicBinary(value interface{}) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case Slice:
		if len(value) == 0 {
			return nil, nil
		}
	case Map:
		if len(value) == 0 {
			return nil, nil
		}
	}
	binaryPayload, ok, err := marshalSnapshotCollectionValueBinary(value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("hatriecache: unsupported binary journal value")
	}
	return binaryPayload, nil
}

func marshalJournalDynamicJSON(value interface{}) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case Slice:
		if len(value) == 0 {
			return nil, nil
		}
	case Map:
		if len(value) == 0 {
			return nil, nil
		}
	}
	return json.Marshal(value)
}

func decodeCommandJournalEntry(data []byte) (commandJournalEntry, error) {
	if commandJournalRecordIsBinary(data) {
		return decodeCommandJournalEntryBinary(data)
	}
	return decodeCommandJournalEntryJSON(data)
}

func decodeCommandJournalEntryJSON(data []byte) (commandJournalEntry, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()

	var entry commandJournalEntry
	if err := decoder.Decode(&entry); err != nil {
		return commandJournalEntry{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return commandJournalEntry{}, errors.New("hatriecache: invalid journal JSON")
		}
		return commandJournalEntry{}, err
	}
	if err := validateCommandJournalEntry(entry); err != nil {
		return commandJournalEntry{}, err
	}
	return entry, nil
}

func decodeCommandJournalEntryBinary(data []byte) (commandJournalEntry, error) {
	if !commandJournalRecordIsBinary(data) {
		return commandJournalEntry{}, errors.New("hatriecache: invalid binary command journal record")
	}
	reader := newBinaryFieldReader(data[len(commandJournalBinaryMagic):])
	payload, err := reader.readBytes()
	if err != nil {
		return commandJournalEntry{}, err
	}
	if !reader.done() {
		return commandJournalEntry{}, errors.New("hatriecache: invalid trailing binary command journal record data")
	}
	return decodeCommandJournalEntryBinaryPayload(payload)
}

func decodeCommandJournalEntryBinaryPayload(data []byte) (commandJournalEntry, error) {
	reader := newBinaryFieldReader(data)
	version, err := reader.readUvarint()
	if err != nil {
		return commandJournalEntry{}, err
	}
	if version != commandJournalVersion && version != commandJournalBinaryDynamicVersion && version != commandJournalBinaryPayloadVersion {
		return commandJournalEntry{}, errors.New("hatriecache: unsupported journal version")
	}
	if version > uint64(int(^uint(0)>>1)) {
		return commandJournalEntry{}, errors.New("hatriecache: invalid journal version")
	}
	sequence, err := reader.readUvarint()
	if err != nil {
		return commandJournalEntry{}, err
	}
	checkpoint, err := reader.readBool()
	if err != nil {
		return commandJournalEntry{}, err
	}
	request, err := readCommandJournalRequestBinary(&reader, version)
	if err != nil {
		return commandJournalEntry{}, err
	}
	var outbox *replicationOutboxJob
	if version >= commandJournalBinaryPayloadVersion {
		data, err := reader.readBytes()
		if err != nil {
			return commandJournalEntry{}, err
		}
		if len(data) > 0 {
			record, err := unmarshalReplicationOutboxJob(data)
			if err != nil {
				return commandJournalEntry{}, err
			}
			outbox = &record
		}
	}
	if !reader.done() {
		return commandJournalEntry{}, errors.New("hatriecache: invalid trailing binary command journal payload data")
	}
	entry := commandJournalEntry{
		Version:    commandJournalVersion,
		Sequence:   sequence,
		Checkpoint: checkpoint,
		Request:    request,
		Outbox:     outbox,
	}
	if err := validateCommandJournalEntry(entry); err != nil {
		return commandJournalEntry{}, err
	}
	return entry, nil
}

func readCommandJournalOptionalInt64Binary(reader *binaryFieldReader) (*int64, error) {
	present, err := reader.readBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	value, err := reader.readVarint()
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func readCommandJournalRequestBinary(reader *binaryFieldReader, version uint64) (CacheCommandRequest, error) {
	var request CacheCommandRequest
	if err := readCommandJournalRequestBinaryInto(reader, version, &request); err != nil {
		return CacheCommandRequest{}, err
	}
	return request, nil
}

func readCommandJournalRequestBinaryInto(reader *binaryFieldReader, version uint64, request *CacheCommandRequest) error {
	*request = CacheCommandRequest{}
	var err error
	if request.Command, err = reader.readString(); err != nil {
		return err
	}
	if request.Key, err = reader.readString(); err != nil {
		return err
	}
	if request.Value, err = reader.readString(); err != nil {
		return err
	}
	if request.Subkey, err = reader.readString(); err != nil {
		return err
	}
	if request.Priority, err = readCommandJournalOptionalInt64Binary(reader); err != nil {
		return err
	}
	if request.TTLSeconds, err = readCommandJournalOptionalInt64Binary(reader); err != nil {
		return err
	}
	if request.UnixSeconds, err = readCommandJournalOptionalInt64Binary(reader); err != nil {
		return err
	}
	values, err := reader.readBytes()
	if err != nil {
		return err
	}
	pairs, err := reader.readBytes()
	if err != nil {
		return err
	}
	if len(values) > 0 {
		if err := decodeJournalDynamicValues(values, version, &request.Values); err != nil {
			return err
		}
	}
	if len(pairs) > 0 {
		if err := decodeJournalDynamicPairs(pairs, version, &request.Pairs); err != nil {
			return err
		}
	}
	return nil
}

func decodeJournalDynamicValues(data []byte, version uint64, values *Slice) error {
	if version >= commandJournalBinaryDynamicVersion && snapshotValueDataIsBinary(data) {
		value, err := unmarshalSnapshotValueBinary(data)
		if err != nil {
			return err
		}
		decoded, ok := value.(Slice)
		if !ok {
			return errors.New("hatriecache: binary command journal values field is not an array")
		}
		*values = decoded
		return nil
	}
	return decodeJournalDynamicJSON(data, values)
}

func decodeJournalDynamicPairs(data []byte, version uint64, pairs *Map) error {
	if version >= commandJournalBinaryDynamicVersion && snapshotValueDataIsBinary(data) {
		value, err := unmarshalSnapshotValueBinary(data)
		if err != nil {
			return err
		}
		decoded, ok := value.(Map)
		if !ok {
			return errors.New("hatriecache: binary command journal pairs field is not an object")
		}
		*pairs = decoded
		return nil
	}
	return decodeJournalDynamicJSON(data, pairs)
}

func decodeJournalDynamicJSON(data []byte, value interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("hatriecache: invalid command journal JSON field")
		}
		return err
	}
	return nil
}

func validateCommandJournalEntry(entry commandJournalEntry) error {
	if entry.Version != commandJournalVersion {
		return errors.New("hatriecache: unsupported journal version")
	}
	if entry.Sequence == 0 {
		return errors.New("hatriecache: invalid journal sequence")
	}
	return validateCommandJournalEntryRequest(entry)
}
