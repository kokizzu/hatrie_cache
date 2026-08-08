package hatriecache

import (
	"errors"
	"sort"
	"time"
)

func (ht *HatTrie) appendCommandDumpPriorityQueueEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_PRIORITY_QUEUE {
		return ht.appendCommandDumpTopKEntryBinaryLocked(destination, entry)
	}
	index := entry.Value.Index
	if index < 0 || int(index) >= len(ht.priorityQueues.array) || ht.priorityQueues.reusables.Has(index) {
		return destination, true, errors.New("hatriecache: priority queue backing index is missing")
	}
	queue := &ht.priorityQueues.array[index]
	for itemIndex := range queue.items {
		if _, ok, err := commandDumpPriorityQueueItemValueSize(queue.items[itemIndex]); err != nil {
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
	data, err := appendCommandDumpPriorityQueueBinary(destination, expiresAt, queue.items)
	return data, true, err
}

func appendCommandDumpPriorityQueueBinary(destination []byte, expiresAt *time.Time, liveItems []priorityQueueItem) ([]byte, error) {
	items := append([]priorityQueueItem(nil), liveItems...)
	sort.Sort(priorityQueueItemsByOrder(items))
	size := 1 + binaryUvarintSize(uint64(len(items)))
	for index := range items {
		itemSize := binaryVarintSize(items[index].Priority) + binaryUvarintSize(items[index].Sequence)
		valueSize, _, err := commandDumpPriorityQueueItemValueSize(items[index])
		if err != nil {
			return destination, err
		}
		itemSize, err = snapshotValueBinaryAdd(itemSize, valueSize)
		if err != nil {
			return destination, err
		}
		size, err = snapshotValueBinaryAdd(size, itemSize)
		if err != nil {
			return destination, err
		}
	}
	entry := snapshotEntry{Type: "priority_queue", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryPriorityQueue)
	writer.writeUvarint(uint64(len(items)))
	for index := range items {
		writer.writeVarint(items[index].Priority)
		writer.writeUvarint(items[index].Sequence)
		if !writeCommandDumpPriorityQueueItemValue(&writer.binaryFieldWriter, items[index]) {
			return destination, errors.New("hatriecache: unsupported binary snapshot value")
		}
	}
	return finishReplicationDirectPayload(writer, entry), nil
}

func commandDumpPriorityQueueItemValueSize(item priorityQueueItem) (int, bool, error) {
	if item.stringValue == "" {
		return snapshotValueBinarySize(item.Value)
	}
	size, err := snapshotValueBinaryBytesSize(len(item.stringValue))
	if err != nil {
		return 0, true, err
	}
	total, err := snapshotValueBinaryAdd(1, size)
	return total, true, err
}

func writeCommandDumpPriorityQueueItemValue(writer *binaryFieldWriter, item priorityQueueItem) bool {
	if item.stringValue == "" {
		return writeSnapshotValueBinary(writer, item.Value)
	}
	writer.buf = append(writer.buf, snapshotValueBinaryString)
	writer.writeString(item.stringValue)
	return true
}

type priorityQueueItemsByOrder []priorityQueueItem

func (items priorityQueueItemsByOrder) Len() int { return len(items) }

func (items priorityQueueItemsByOrder) Less(left int, right int) bool {
	return priorityQueueLess(items[left], items[right])
}

func (items priorityQueueItemsByOrder) Swap(left int, right int) {
	items[left], items[right] = items[right], items[left]
}
