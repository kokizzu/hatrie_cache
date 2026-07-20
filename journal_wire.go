package hatriecache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

type CommandJournalWireFormat string

const (
	CommandJournalWireFormatJSON   CommandJournalWireFormat = "json"
	CommandJournalWireFormatBinary CommandJournalWireFormat = "binary"
)

const DefaultCommandJournalWireFormat = CommandJournalWireFormatBinary

var commandJournalTailBinaryMagic = []byte{'h', 'c', 'j', 't', 1}

func ParseCommandJournalWireFormat(value string) (CommandJournalWireFormat, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(CommandJournalWireFormatBinary), "bin":
		return CommandJournalWireFormatBinary, nil
	case string(CommandJournalWireFormatJSON):
		return CommandJournalWireFormatJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported command journal wire format %q", value)
	}
}

func marshalCommandJournalTailBinary(tail CommandJournalTail) ([]byte, error) {
	if tail.Limit < 0 || tail.Limit > MaxCommandJournalTailLimit || len(tail.Entries) > tail.Limit {
		return nil, errors.New("hatriecache: invalid command journal tail size")
	}
	initialCapacity := len(commandJournalTailBinaryMagic) + 64 + len(tail.Entries)*32
	if initialCapacity > maxCommandJournalTailResponseBytes {
		initialCapacity = maxCommandJournalTailResponseBytes
	}
	writer := newBinaryFieldWriter(commandJournalTailBinaryMagic, initialCapacity)
	writer.writeUvarint(tail.LastSequence)
	writer.writeUvarint(tail.CompactedThrough)
	writer.writeUvarint(uint64(tail.Limit))
	writer.writeBool(tail.HasMore)
	writer.writeUvarint(uint64(len(tail.Entries)))
	for _, record := range tail.Entries {
		if record.Sequence == 0 {
			return nil, errors.New("hatriecache: invalid command journal tail sequence")
		}
		values, pairs, err := marshalCommandJournalRequestBinaryDynamicFields(record.Request)
		if err != nil {
			return nil, err
		}
		writer.writeUvarint(record.Sequence)
		if err := writeCommandJournalRequestBinaryFields(&writer, record.Request, values, pairs); err != nil {
			return nil, err
		}
	}
	return writer.bytes(), nil
}

func decodeCommandJournalTailBinaryResponse(body io.Reader, optionalContentLength ...int64) (CommandJournalTail, error) {
	contentLength := int64(-1)
	if len(optionalContentLength) > 0 {
		contentLength = optionalContentLength[0]
	}
	data, err := readCommandJournalTailBinaryBody(body, contentLength)
	if err != nil {
		return CommandJournalTail{}, err
	}
	if !bytes.HasPrefix(data, commandJournalTailBinaryMagic) {
		return CommandJournalTail{}, errors.New("journal source returned an invalid binary tail")
	}
	reader := newBorrowingBinaryFieldReader(data[len(commandJournalTailBinaryMagic):])
	lastSequence, err := reader.readUvarint()
	if err != nil {
		return CommandJournalTail{}, err
	}
	compactedThrough, err := reader.readUvarint()
	if err != nil {
		return CommandJournalTail{}, err
	}
	limit, err := reader.readUvarint()
	if err != nil || limit > MaxCommandJournalTailLimit {
		return CommandJournalTail{}, errors.New("journal source returned an invalid binary tail limit")
	}
	hasMore, err := reader.readBool()
	if err != nil {
		return CommandJournalTail{}, err
	}
	count, err := reader.readUvarint()
	if err != nil || count > MaxCommandJournalTailLimit || count > limit || count > uint64(int(^uint(0)>>1)) {
		return CommandJournalTail{}, errors.New("journal source returned an invalid binary tail entry count")
	}
	tail := CommandJournalTail{
		LastSequence:     lastSequence,
		CompactedThrough: compactedThrough,
		Limit:            int(limit),
		HasMore:          hasMore,
		Entries:          make([]CommandJournalRecord, int(count)),
		wireFormat:       CommandJournalWireFormatBinary,
	}
	for index := uint64(0); index < count; index++ {
		sequence, err := reader.readUvarint()
		if err != nil {
			return CommandJournalTail{}, err
		}
		if sequence == 0 {
			return CommandJournalTail{}, errors.New("journal source returned an invalid binary tail sequence")
		}
		record := &tail.Entries[int(index)]
		record.Sequence = sequence
		if err := readCommandJournalRequestBinaryInto(&reader, commandJournalBinaryDynamicVersion, &record.Request); err != nil {
			return CommandJournalTail{}, err
		}
	}
	if !reader.done() {
		return CommandJournalTail{}, errors.New("journal source returned invalid trailing binary tail data")
	}
	return tail, nil
}

func readCommandJournalTailBinaryBody(body io.Reader, contentLength int64) ([]byte, error) {
	if contentLength > maxCommandJournalTailResponseBytes {
		return nil, errCommandJournalTailResponseTooLarge
	}
	if contentLength >= 0 {
		data := make([]byte, int(contentLength)+1)
		n, err := io.ReadFull(body, data)
		if n > maxCommandJournalTailResponseBytes {
			return nil, errCommandJournalTailResponseTooLarge
		}
		if n < int(contentLength) {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return nil, err
		}
		return data[:n], nil
	}
	data, err := io.ReadAll(io.LimitReader(body, maxCommandJournalTailResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxCommandJournalTailResponseBytes {
		return nil, errCommandJournalTailResponseTooLarge
	}
	return data, nil
}
