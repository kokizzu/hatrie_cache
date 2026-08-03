package hatriecache

import "time"

func (ht *HatTrie) appendCommandDumpSliceEntryBinaryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	switch entry.Value.Type() {
	case DATAVALUE_TYPE_FENWICK_TREE:
		data, err := appendCommandDumpFenwickTreeBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.fenwickTrees.array[entry.Value.Index])
		return data, true, err
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		data, err := appendCommandDumpQuantileSketchBinary(destination, snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)), ht.quantileSketches.array[entry.Value.Index])
		return data, true, err
	default:
		return ht.appendCommandDumpMapEntryBinaryLocked(destination, entry)
	}
}

func appendCommandDumpFenwickTreeBinary(destination []byte, expiresAt *time.Time, tree fenwickTreeData) ([]byte, error) {
	values := tree.tree
	if fenwickTreeRawIsZero(values) {
		values = nil
	}
	snapshot := fenwickTreeSnapshot{
		Size:    tree.size,
		Updates: tree.updates,
		Total:   tree.total,
		Tree:    values,
	}
	size, err := snapshotValueBinaryFenwickTreeSize(snapshot)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "fenwick_tree", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryFenwickTree(&writer.binaryFieldWriter, snapshot)
	return finishReplicationDirectPayload(writer, entry), nil
}

func appendCommandDumpQuantileSketchBinary(destination []byte, expiresAt *time.Time, sketch quantileSketchData) ([]byte, error) {
	snapshot := quantileSketchSnapshot{
		Epsilon: sketch.epsilon,
		Count:   sketch.count,
		Summary: sketch.summary,
	}
	size, err := snapshotValueBinaryQuantileSketchSize(snapshot)
	if err != nil {
		return destination, err
	}
	entry := snapshotEntry{Type: "quantile_sketch", ExpiresAt: expiresAt}
	writer, err := newReplicationDirectPayloadWriter(destination, entry, size)
	if err != nil {
		return destination, err
	}
	writeSnapshotValueBinaryQuantileSketch(&writer.binaryFieldWriter, snapshot)
	return finishReplicationDirectPayload(writer, entry), nil
}
