package hatriecache

import (
	"errors"
	"sort"
	"time"
)

func (ht *HatTrie) appendCommandDumpSetEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_SET {
		return ht.appendCommandDumpPriorityQueueEntryBinaryLocked(destination, entry)
	}
	var expiresAt *time.Time
	if entry.Value.HasTtl() {
		expiration := ht.expirationTimeLocked(entry.Key)
		expiresAt = &expiration
	}
	poolIndex, twoValuePool, packed := decodePackedStringSetIndex(entry.Value.Index)
	if packed {
		var values [2]interface{}
		length := 0
		if !twoValuePool {
			if poolIndex < 0 || int(poolIndex) >= len(ht.sets.oneStrings) || ht.sets.oneReusable.Has(poolIndex) {
				return destination, true, errors.New("hatriecache: set backing index is missing")
			}
			values[0] = ht.sets.oneStrings[poolIndex].value
			if values[0] != nil {
				length = 1
			}
		} else {
			if poolIndex < 0 || int(poolIndex) >= len(ht.sets.twoStrings) || ht.sets.twoReusable.Has(poolIndex) {
				return destination, true, errors.New("hatriecache: set backing index is missing")
			}
			values = ht.sets.twoStrings[poolIndex].values
			if values[0] != nil {
				length = 1
			}
			if values[1] != nil {
				length = 2
			}
			if length == 2 && values[1].(string) < values[0].(string) {
				values[0], values[1] = values[1], values[0]
			}
		}
		data, handled, err := appendCommandDumpSmallSetBinary(destination, expiresAt, values, length)
		return data, handled, err
	}
	index := entry.Value.Index
	if index < 0 || int(index) >= len(ht.sets.array) || ht.sets.reusables.Has(index) {
		return destination, true, errors.New("hatriecache: set backing index is missing")
	}
	data, handled, err := appendCommandDumpSetDataBinary(destination, expiresAt, &ht.sets.array[index])
	return data, handled, err
}

func appendCommandDumpSetDataBinary(destination []byte, expiresAt *time.Time, set *setData) ([]byte, bool, error) {
	if set.items == nil {
		if len(set.small) > smallSetEntryLimit {
			return destination, true, errors.New("hatriecache: invalid small set backing")
		}
		var entries [smallSetEntryLimit]setSmallEntry
		copy(entries[:], set.small)
		for index := range set.small {
			if _, ok, err := snapshotValueBinarySize(entries[index].value); err != nil {
				return destination, true, err
			} else if !ok {
				return destination, false, nil
			}
		}
		if len(set.small) == 2 && setSmallEntryLess(entries[1], entries[0]) {
			entries[0], entries[1] = entries[1], entries[0]
		}
		var values [2]interface{}
		for index := range set.small {
			values[index] = entries[index].value
		}
		return appendCommandDumpSmallSetBinary(destination, expiresAt, values, len(set.small))
	}

	var inlineKeys [8]string
	keys := inlineKeys[:0]
	if len(set.items) > len(inlineKeys) {
		keys = make([]string, 0, len(set.items))
	}
	for key := range set.items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	size := 1 + binaryUvarintSize(uint64(len(keys)))
	for _, key := range keys {
		valueSize, ok, err := snapshotValueBinarySize(set.items[key])
		if err != nil {
			return destination, true, err
		}
		if !ok {
			return destination, false, nil
		}
		size, err = snapshotValueBinaryAdd(size, valueSize)
		if err != nil {
			return destination, true, err
		}
	}
	entry := snapshotEntry{Type: "set", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, true, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryArray)
	writer.writeUvarint(uint64(len(keys)))
	for _, key := range keys {
		if !writeSnapshotValueBinary(&writer.binaryFieldWriter, set.items[key]) {
			return destination, true, errors.New("hatriecache: unsupported binary snapshot value")
		}
	}
	return finishReplicationDirectPayload(writer, entry), true, nil
}

func appendCommandDumpSmallSetBinary(destination []byte, expiresAt *time.Time, values [2]interface{}, length int) ([]byte, bool, error) {
	size := 1 + binaryUvarintSize(uint64(length))
	for index := 0; index < length; index++ {
		valueSize, ok, err := snapshotValueBinarySize(values[index])
		if err != nil {
			return destination, true, err
		}
		if !ok {
			return destination, false, nil
		}
		size, err = snapshotValueBinaryAdd(size, valueSize)
		if err != nil {
			return destination, true, err
		}
	}
	entry := snapshotEntry{Type: "set", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, true, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryArray)
	writer.writeUvarint(uint64(length))
	for index := 0; index < length; index++ {
		if !writeSnapshotValueBinary(&writer.binaryFieldWriter, values[index]) {
			return destination, true, errors.New("hatriecache: unsupported binary snapshot value")
		}
	}
	return finishReplicationDirectPayload(writer, entry), true, nil
}
