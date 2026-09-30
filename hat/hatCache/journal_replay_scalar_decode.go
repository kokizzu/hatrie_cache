package hatCache

import "errors"

func decodeCommandJournalScalarRecordBinaryPayload(data []byte) (commandJournalEntry, nativeCommandBatchFamily, bool, error) {
	reader := newBorrowingBinaryFieldReader(data)
	version, err := reader.readUvarint()
	if err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if version != commandJournalVersion && version != commandJournalBinaryDynamicVersion && version != commandJournalBinaryOutboxVersion && version != commandJournalBinaryPayloadVersion {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, errors.New("hatriecache: unsupported journal version")
	}
	sequence, err := reader.readUvarint()
	if err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	checkpoint, err := reader.readBool()
	if err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if checkpoint {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, nil
	}

	request := CacheCommandRequest{}
	if request.Command, err = reader.readString(); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if request.Key, err = reader.readString(); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if request.Value, err = reader.readString(); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if request.Subkey, err = reader.readString(); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if version >= commandJournalBinaryIdempotencyVersion {
		if request.IdempotencyKey, err = reader.readString(); err != nil {
			return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
		}
	}
	if request.Priority, err = readCommandJournalOptionalInt64Binary(&reader); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if request.TTLSeconds, err = readCommandJournalOptionalInt64Binary(&reader); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if request.UnixSeconds, err = readCommandJournalOptionalInt64Binary(&reader); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	values, err := reader.readBytes()
	if err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	pairs, err := reader.readBytes()
	if err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	if version >= commandJournalBinaryBatchVersion {
		if request.Atomic, err = reader.readBool(); err != nil {
			return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
		}
		if _, err := reader.readBytes(); err != nil {
			return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
		}
	}
	var idempotencyFingerprint []byte
	if version >= commandJournalBinaryIdempotencyVersion {
		idempotencyFingerprint, err = reader.readBytes()
		if err != nil {
			return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
		}
	}
	var outbox []byte
	if version >= commandJournalBinaryOutboxVersion {
		outbox, err = reader.readBytes()
		if err != nil {
			return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
		}
	}
	if !reader.done() {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, errors.New("hatriecache: invalid trailing binary command journal payload data")
	}

	if request.Subkey != "" || request.IdempotencyKey != "" || request.Priority != nil || request.TTLSeconds != nil || request.UnixSeconds != nil || len(values) != 0 || len(pairs) != 0 || request.Atomic || len(idempotencyFingerprint) != 0 || len(outbox) != 0 {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, nil
	}
	family := scalarCommandJournalFamilyWithoutNormalization(request.Command)
	if family == nativeCommandBatchUnsupported {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, nil
	}
	entry := commandJournalEntry{
		Version:                commandJournalVersion,
		Sequence:               sequence,
		Checkpoint:             checkpoint,
		Request:                request,
		IdempotencyFingerprint: idempotencyFingerprint,
	}
	if err := validateCommandJournalEntry(entry); err != nil {
		return commandJournalEntry{}, nativeCommandBatchUnsupported, false, err
	}
	return entry, family, true, nil
}

func scalarCommandJournalFamilyWithoutNormalization(command string) nativeCommandBatchFamily {
	switch command {
	case "SET", "SETSTR":
		return nativeCommandBatchSetString
	case "SETINT":
		return nativeCommandBatchSetCounter
	default:
		return nativeCommandBatchUnsupported
	}
}
