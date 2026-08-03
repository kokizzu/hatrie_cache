package hatriecache

import (
	"errors"
	"time"
)

func (ht *HatTrie) appendCommandDumpMapEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_MAP {
		return destination, false, nil
	}
	var expiresAt *time.Time
	if entry.Value.HasTtl() {
		expiration := ht.expirationTimeLocked(entry.Key)
		expiresAt = &expiration
	}
	if smallIndex, ok := decodeSmallMapIndex(entry.Value.Index); ok {
		if smallIndex < 0 || int(smallIndex) >= len(ht.maps.small) || ht.maps.smallReusables.Has(smallIndex) {
			return destination, true, errors.New("hatriecache: map backing index is missing")
		}
		data, err := appendCommandDumpSmallMapBinary(destination, expiresAt, &ht.maps.small[smallIndex])
		return data, true, err
	}
	index := entry.Value.Index
	if index < 0 || int(index) >= len(ht.maps.array) || ht.maps.reusables.Has(index) {
		return destination, true, errors.New("hatriecache: map backing index is missing")
	}
	snapshot := snapshotEntry{Type: "map", Map: ht.maps.array[index], ExpiresAt: expiresAt}
	data, err := appendReplicationCollectionValueBinary(destination, snapshot, snapshot.Map)
	return data, true, err
}

func appendCommandDumpSmallMapBinary(destination []byte, expiresAt *time.Time, data *smallMapData) ([]byte, error) {
	length := int(data.length)
	var fields [smallMapEntryLimit]smallMapEntry
	for index := 0; index < length; index++ {
		fields[index] = data.entries[index]
		value, _, err := prepareSnapshotDynamicValueBinary(fields[index].value)
		if err != nil {
			return destination, err
		}
		fields[index].value = value
	}
	if length == 2 && fields[1].key < fields[0].key {
		fields[0], fields[1] = fields[1], fields[0]
	}

	size := 1 + binaryUvarintSize(uint64(length))
	for index := 0; index < length; index++ {
		keySize, err := snapshotValueBinaryBytesSize(len(fields[index].key))
		if err != nil {
			return destination, err
		}
		size, err = snapshotValueBinaryAdd(size, keySize)
		if err != nil {
			return destination, err
		}
		valueSize, ok, err := snapshotValueBinarySize(fields[index].value)
		if err != nil {
			return destination, err
		}
		if !ok {
			return destination, errors.New("hatriecache: unsupported binary snapshot value")
		}
		size, err = snapshotValueBinaryAdd(size, valueSize)
		if err != nil {
			return destination, err
		}
	}

	entry := snapshotEntry{Type: "map", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryObject)
	writer.writeUvarint(uint64(length))
	for index := 0; index < length; index++ {
		writer.writeString(fields[index].key)
		if !writeSnapshotValueBinary(&writer.binaryFieldWriter, fields[index].value) {
			return destination, errors.New("hatriecache: unsupported binary snapshot value")
		}
	}
	return finishReplicationDirectPayload(writer, entry), nil
}
