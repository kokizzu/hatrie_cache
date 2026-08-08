package hatriecache

import (
	"errors"
	"time"
)

func (ht *HatTrie) appendCommandDumpTopKEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_TOP_K {
		return destination, false, nil
	}
	index := entry.Value.Index
	if index < 0 || int(index) >= len(ht.topKs.array) || ht.topKs.reusables.Has(index) {
		return destination, true, errors.New("hatriecache: top-k backing index is missing")
	}
	top := ht.topKs.array[index]
	for itemIndex := range top.items {
		if _, ok, err := snapshotValueBinarySize(top.items[itemIndex].Value); err != nil {
			return destination, true, err
		} else if !ok {
			return destination, false, nil
		}
	}
	var expiresAt *time.Time
	if entry.Value.HasTtl() {
		expiration := ht.expirationTimeLocked(entry.Key)
		expiresAt = &expiration
	}
	data, err := appendCommandDumpTopKBinary(destination, expiresAt, top)
	return data, true, err
}

func appendCommandDumpTopKBinary(destination []byte, expiresAt *time.Time, top topKData) ([]byte, error) {
	snapshot := topKSnapshot{
		Capacity: top.capacity,
		Total:    top.total,
		Items:    top.sortedItems(),
	}
	size, _, err := snapshotValueBinaryTopKSize(snapshot)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "top_k", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinaryTopK(&writer.binaryFieldWriter, snapshot) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}
