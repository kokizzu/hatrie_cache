package hatriecache

import (
	"errors"
	"time"
)

func (ht *HatTrie) appendCommandDumpReservoirSampleEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_RESERVOIR_SAMPLE {
		return destination, false, nil
	}
	index := entry.Value.Index
	if index < 0 || int(index) >= len(ht.reservoirSamples.array) || ht.reservoirSamples.reusables.Has(index) {
		return destination, true, errors.New("hatriecache: reservoir sample backing index is missing")
	}
	sample := ht.reservoirSamples.array[index]
	for itemIndex := range sample.items {
		if _, ok, err := snapshotValueBinarySize(sample.items[itemIndex].Value); err != nil {
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
	data, err := appendCommandDumpReservoirSampleBinary(destination, expiresAt, sample)
	return data, true, err
}

func appendCommandDumpReservoirSampleBinary(destination []byte, expiresAt *time.Time, sample reservoirSampleData) ([]byte, error) {
	snapshot := reservoirSampleSnapshot{
		Capacity: sample.capacity,
		Seen:     sample.seen,
		Items:    sample.sortedItems(),
	}
	size, _, err := snapshotValueBinaryReservoirSampleSize(snapshot)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "reservoir_sample", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	if !writeSnapshotValueBinaryReservoirSample(&writer.binaryFieldWriter, snapshot) {
		return destination, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), nil
}
