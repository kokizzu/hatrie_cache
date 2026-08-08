package hatriecache

import (
	"errors"
	"time"
)

func (ht *HatTrie) appendCommandDumpRadixTreeEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_RADIX_TREE {
		return destination, false, nil
	}
	index := entry.Value.Index
	if index < 0 || int(index) >= len(ht.radixTrees.array) || ht.radixTrees.reusables.Has(index) {
		return destination, true, errors.New("hatriecache: radix tree backing index is missing")
	}
	tree := ht.radixTrees.array[index]
	var expiresAt *time.Time
	if entry.Value.HasTtl() {
		expiration := ht.expirationTimeLocked(entry.Key)
		expiresAt = &expiration
	}
	data, handled, err := appendCommandDumpRadixTreeBinary(destination, expiresAt, tree)
	return data, handled, err
}

func appendCommandDumpRadixTreeBinary(destination []byte, expiresAt *time.Time, tree radixTreeData) ([]byte, bool, error) {
	var stackPath [radixTreeJSONStackPathCapacity]byte
	fieldsSize, count, ok, err := tree.root.commandDumpBinarySize(stackPath[:0])
	if err != nil || !ok {
		return destination, ok, err
	}
	size, err := snapshotValueBinaryAdd(1+binaryUvarintSize(count), fieldsSize)
	if err != nil {
		return destination, true, err
	}
	entry := snapshotEntry{Type: "radix_tree", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, true, err
	}
	writer.buf = append(writer.buf, snapshotValueBinaryRadixTree)
	writer.writeUvarint(count)
	if !tree.root.writeCommandDumpBinary(&writer.binaryFieldWriter, stackPath[:0]) {
		return destination, true, errors.New("hatriecache: unsupported binary snapshot value")
	}
	return finishReplicationDirectPayload(writer, entry), true, nil
}

func (node *radixTreeNode) commandDumpBinarySize(path []byte) (int, uint64, bool, error) {
	if node == nil {
		return 0, 0, true, nil
	}
	path = append(path, node.prefix...)
	total := 0
	var count uint64
	if node.hasValue {
		keySize, err := snapshotValueBinaryBytesSize(len(path))
		if err != nil {
			return 0, 0, true, err
		}
		valueSize, ok, err := snapshotValueBinarySize(node.value)
		if err != nil || !ok {
			return 0, 0, ok, err
		}
		total, err = snapshotValueBinaryAdd(keySize, valueSize)
		if err != nil {
			return 0, 0, true, err
		}
		count = 1
	}
	for index := range node.children {
		childSize, childCount, ok, err := node.children[index].commandDumpBinarySize(path)
		if err != nil || !ok {
			return 0, 0, ok, err
		}
		total, err = snapshotValueBinaryAdd(total, childSize)
		if err != nil {
			return 0, 0, true, err
		}
		if ^uint64(0)-count < childCount {
			return 0, 0, true, errors.New("hatriecache: radix tree item count overflow")
		}
		count += childCount
	}
	return total, count, true, nil
}

func (node *radixTreeNode) writeCommandDumpBinary(writer *binaryFieldWriter, path []byte) bool {
	if node == nil {
		return true
	}
	path = append(path, node.prefix...)
	if node.hasValue {
		writer.writeBytes(path)
		if !writeSnapshotValueBinary(writer, node.value) {
			return false
		}
	}
	for index := range node.children {
		if !node.children[index].writeCommandDumpBinary(writer, path) {
			return false
		}
	}
	return true
}
