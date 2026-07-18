package hatriecache

import (
	"bytes"
	"errors"
	"time"
)

var replicationValueBinaryMagic = []byte{'h', 'c', 'r', 'v', 1}

func replicationValueDataIsBinary(data []byte) bool {
	return bytes.HasPrefix(data, replicationValueBinaryMagic)
}

func marshalReplicationValueBinary(entry snapshotEntry) ([]byte, error) {
	value, err := prepareLevelDBBinaryEntryValue(entry)
	if err != nil {
		return nil, err
	}
	capacity, err := replicationValueBinaryCapacity(entry.Type, value.encodedSize, entry.ExpiresAt)
	if err != nil {
		return nil, err
	}
	writer := levelDBBinaryWriter{binaryFieldWriter: newBinaryFieldWriter(replicationValueBinaryMagic, capacity)}
	writer.writeString(entry.Type)
	writer.writePreparedSnapshotEntryValue(value)
	writer.writeTimePtr(entry.ExpiresAt)
	return writer.bytes(), nil
}

func marshalReplicationStringValueBinary(value string) ([]byte, error) {
	return marshalReplicationValueBinary(snapshotEntry{Type: "string", String: value})
}

func replicationValueBinaryCapacity(entryType string, encodedValueBytes int64, expiresAt *time.Time) (int, error) {
	if encodedValueBytes < 0 {
		return 0, errLevelDBBinaryRecordTooLarge
	}
	typeSize, err := binaryLengthPrefixedSize(int64(len(entryType)))
	if err != nil {
		return 0, err
	}
	total := int64(len(replicationValueBinaryMagic))
	for _, size := range []int64{typeSize, encodedValueBytes, int64(levelDBBinaryTimePtrSize(expiresAt))} {
		total, err = addLevelDBBinaryRecordSize(total, size)
		if err != nil {
			return 0, err
		}
	}
	return int(total), nil
}

func unmarshalReplicationValueBinary(key string, data []byte) (snapshotEntry, error) {
	if !replicationValueDataIsBinary(data) {
		return snapshotEntry{}, errors.New("hatriecache: invalid binary replication value")
	}
	reader := levelDBBinaryReader{binaryFieldReader: newBinaryFieldReader(data[len(replicationValueBinaryMagic):])}
	entryType, err := reader.readString()
	if err != nil {
		return snapshotEntry{}, err
	}
	entry := snapshotEntry{Key: key, Type: entryType}
	if err := reader.readSnapshotEntryValue(&entry); err != nil {
		return snapshotEntry{}, err
	}
	entry.ExpiresAt, err = reader.readTimePtr()
	if err != nil {
		return snapshotEntry{}, err
	}
	if !reader.done() {
		return snapshotEntry{}, errors.New("hatriecache: invalid trailing binary replication value data")
	}
	return entry, nil
}
