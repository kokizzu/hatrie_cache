package hatriecache

import (
	"errors"
	"time"
)

func (ht *HatTrie) appendCommandDumpStoredSliceEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_SLICE {
		return ht.appendCommandDumpSetEntryBinaryLocked(destination, entry)
	}
	var expiresAt *time.Time
	if entry.Value.HasTtl() {
		expiration := ht.expirationTimeLocked(entry.Key)
		expiresAt = &expiration
	}
	poolIndex, twoValuePool, packed := decodePackedSliceIndex(entry.Value.Index)
	if packed {
		if !twoValuePool {
			if poolIndex < 0 || int(poolIndex) >= len(ht.slices.oneValues) || ht.slices.oneReusable.Has(poolIndex) {
				return destination, true, errors.New("hatriecache: slice backing index is missing")
			}
			stored := ht.slices.oneValues[poolIndex]
			var values [2]interface{}
			values[0] = stored.value
			data, handled, err := appendCommandDumpPackedSliceBinary(destination, expiresAt, values, int(stored.length))
			return data, handled, err
		}
		if poolIndex < 0 || int(poolIndex) >= len(ht.slices.twoValues) || ht.slices.twoReusable.Has(poolIndex) {
			return destination, true, errors.New("hatriecache: slice backing index is missing")
		}
		stored := ht.slices.twoValues[poolIndex]
		data, handled, err := appendCommandDumpPackedSliceBinary(destination, expiresAt, stored.values, int(stored.length))
		return data, handled, err
	}
	index := entry.Value.Index
	if index < 0 || int(index) >= len(ht.slices.array) || ht.slices.reusables.Has(index) {
		return destination, true, errors.New("hatriecache: slice backing index is missing")
	}
	data, handled, err := appendCommandDumpDequeBinary(destination, expiresAt, &ht.slices.array[index])
	return data, handled, err
}

func appendCommandDumpPackedSliceBinary(destination []byte, expiresAt *time.Time, values [2]interface{}, length int) ([]byte, bool, error) {
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
	entry := snapshotEntry{Type: "slice", ExpiresAt: expiresAt}
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

func appendCommandDumpDequeBinary(destination []byte, expiresAt *time.Time, deque *deque) ([]byte, bool, error) {
	if deque.size < 0 || deque.size > len(deque.values) || deque.head < 0 || deque.head > len(deque.values) || deque.size > 0 && deque.head == len(deque.values) {
		return destination, true, errors.New("hatriecache: invalid slice backing")
	}
	size := 1 + binaryUvarintSize(uint64(deque.size))
	valueIndex := deque.head
	for offset := 0; offset < deque.size; offset++ {
		valueSize, ok, err := snapshotValueBinarySize(deque.values[valueIndex])
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
		valueIndex++
		if valueIndex == len(deque.values) {
			valueIndex = 0
		}
	}
	entry := snapshotEntry{Type: "slice", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, true, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryArray)
	writer.writeUvarint(uint64(deque.size))
	valueIndex = deque.head
	for offset := 0; offset < deque.size; offset++ {
		if !writeSnapshotValueBinary(&writer.binaryFieldWriter, deque.values[valueIndex]) {
			return destination, true, errors.New("hatriecache: unsupported binary snapshot value")
		}
		valueIndex++
		if valueIndex == len(deque.values) {
			valueIndex = 0
		}
	}
	return finishReplicationDirectPayload(writer, entry), true, nil
}
