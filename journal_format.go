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
	payload, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	payload = append(payload, '\n')
	return payload, nil
}

func commandJournalRecordIsBinary(data []byte) bool {
	return bytes.HasPrefix(data, commandJournalBinaryMagic)
}

func marshalCommandJournalEntryBinary(entry commandJournalEntry) ([]byte, error) {
	payload, err := marshalCommandJournalEntryBinaryPayload(entry)
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(commandJournalBinaryMagic, len(commandJournalBinaryMagic)+binaryFieldMaxVarintLen64+len(payload))
	writer.writeBytes(payload)
	return writer.bytes(), nil
}

func marshalCommandJournalEntryBinaryPayload(entry commandJournalEntry) ([]byte, error) {
	writer := newBinaryFieldWriter(nil, 64)
	writer.writeUvarint(uint64(entry.Version))
	writer.writeUvarint(entry.Sequence)
	writer.writeBool(entry.Checkpoint)
	if err := writeCommandJournalRequestBinary(&writer, entry.Request); err != nil {
		return nil, err
	}
	return writer.bytes(), nil
}

func writeCommandJournalOptionalInt64Binary(writer *binaryFieldWriter, value *int64) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeVarint(*value)
}

func writeCommandJournalRequestBinary(writer *binaryFieldWriter, request CacheCommandRequest) error {
	writer.writeString(request.Command)
	writer.writeString(request.Key)
	writer.writeString(request.Value)
	writer.writeString(request.Subkey)
	writeCommandJournalOptionalInt64Binary(writer, request.Priority)
	writeCommandJournalOptionalInt64Binary(writer, request.TTLSeconds)
	writeCommandJournalOptionalInt64Binary(writer, request.UnixSeconds)
	values, err := marshalJournalDynamicJSON(request.Values)
	if err != nil {
		return err
	}
	pairs, err := marshalJournalDynamicJSON(request.Pairs)
	if err != nil {
		return err
	}
	writer.writeBytes(values)
	writer.writeBytes(pairs)
	return nil
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
	request, err := readCommandJournalRequestBinary(&reader)
	if err != nil {
		return commandJournalEntry{}, err
	}
	if !reader.done() {
		return commandJournalEntry{}, errors.New("hatriecache: invalid trailing binary command journal payload data")
	}
	entry := commandJournalEntry{
		Version:    int(version),
		Sequence:   sequence,
		Checkpoint: checkpoint,
		Request:    request,
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

func readCommandJournalRequestBinary(reader *binaryFieldReader) (CacheCommandRequest, error) {
	var request CacheCommandRequest
	var err error
	if request.Command, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Key, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Value, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Subkey, err = reader.readString(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.Priority, err = readCommandJournalOptionalInt64Binary(reader); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.TTLSeconds, err = readCommandJournalOptionalInt64Binary(reader); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.UnixSeconds, err = readCommandJournalOptionalInt64Binary(reader); err != nil {
		return CacheCommandRequest{}, err
	}
	values, err := reader.readBytes()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	pairs, err := reader.readBytes()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if len(values) > 0 {
		if err := decodeJournalDynamicJSON(values, &request.Values); err != nil {
			return CacheCommandRequest{}, err
		}
	}
	if len(pairs) > 0 {
		if err := decodeJournalDynamicJSON(pairs, &request.Pairs); err != nil {
			return CacheCommandRequest{}, err
		}
	}
	return request, nil
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
