package hatriecache

import (
	"bytes"
	"encoding/binary"
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
	out := make([]byte, 0, len(commandJournalBinaryMagic)+binary.MaxVarintLen64+len(payload))
	out = append(out, commandJournalBinaryMagic...)
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(payload)))
	out = append(out, scratch[:n]...)
	out = append(out, payload...)
	return out, nil
}

func marshalCommandJournalEntryBinaryPayload(entry commandJournalEntry) ([]byte, error) {
	writer := journalBinaryWriter{}
	writer.writeUvarint(uint64(entry.Version))
	writer.writeUvarint(entry.Sequence)
	writer.writeBool(entry.Checkpoint)
	if err := writer.writeCommandRequest(entry.Request); err != nil {
		return nil, err
	}
	return writer.bytes(), nil
}

type journalBinaryWriter struct {
	buf []byte
}

func (writer *journalBinaryWriter) bytes() []byte {
	return writer.buf
}

func (writer *journalBinaryWriter) writeBool(value bool) {
	if value {
		writer.buf = append(writer.buf, 1)
		return
	}
	writer.buf = append(writer.buf, 0)
}

func (writer *journalBinaryWriter) writeUvarint(value uint64) {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	writer.buf = append(writer.buf, scratch[:n]...)
}

func (writer *journalBinaryWriter) writeVarint(value int64) {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutVarint(scratch[:], value)
	writer.buf = append(writer.buf, scratch[:n]...)
}

func (writer *journalBinaryWriter) writeBytes(value []byte) {
	writer.writeUvarint(uint64(len(value)))
	writer.buf = append(writer.buf, value...)
}

func (writer *journalBinaryWriter) writeString(value string) {
	writer.writeBytes([]byte(value))
}

func (writer *journalBinaryWriter) writeOptionalInt64(value *int64) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeVarint(*value)
}

func (writer *journalBinaryWriter) writeCommandRequest(request CacheCommandRequest) error {
	writer.writeString(request.Command)
	writer.writeString(request.Key)
	writer.writeString(request.Value)
	writer.writeString(request.Subkey)
	writer.writeOptionalInt64(request.Priority)
	writer.writeOptionalInt64(request.TTLSeconds)
	writer.writeOptionalInt64(request.UnixSeconds)
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
	reader := journalBinaryReader{data: data[len(commandJournalBinaryMagic):]}
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
	reader := journalBinaryReader{data: data}
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
	request, err := reader.readCommandRequest()
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

type journalBinaryReader struct {
	data []byte
	off  int
}

func (reader *journalBinaryReader) done() bool {
	return reader.off == len(reader.data)
}

func (reader *journalBinaryReader) readBool() (bool, error) {
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

func (reader *journalBinaryReader) readUvarint() (uint64, error) {
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

func (reader *journalBinaryReader) readVarint() (int64, error) {
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

func (reader *journalBinaryReader) readBytes() ([]byte, error) {
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

func (reader *journalBinaryReader) readString() (string, error) {
	value, err := reader.readBytes()
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (reader *journalBinaryReader) readOptionalInt64() (*int64, error) {
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

func (reader *journalBinaryReader) readCommandRequest() (CacheCommandRequest, error) {
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
	if request.Priority, err = reader.readOptionalInt64(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.TTLSeconds, err = reader.readOptionalInt64(); err != nil {
		return CacheCommandRequest{}, err
	}
	if request.UnixSeconds, err = reader.readOptionalInt64(); err != nil {
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
