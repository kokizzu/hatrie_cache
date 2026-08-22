package hatriecache

import "hatrie_cache/hat/hatDataStructure"

const (
	DefaultFenwickTreeSize = hatDataStructure.DefaultFenwickTreeSize
	maxFenwickTreeSize     = hatDataStructure.MaxFenwickTreeSize
	maxFenwickTreeInt64    = int64(^uint64(0) >> 1)
	minFenwickTreeInt64    = -maxFenwickTreeInt64 - 1
)

// FenwickTreeUpdate reports the result of an O(log n) point update.
type FenwickTreeUpdate = hatDataStructure.FenwickTreeUpdate

// FenwickTreeInfo reports the shape and memory footprint of a compact prefix
// sum tree.
type FenwickTreeInfo = hatDataStructure.FenwickTreeInfo

// fenwickTreeSnapshot remains the cache snapshot schema. Conversion at this
// boundary keeps persistence and replication formats independent of the
// public data-structure package.
type fenwickTreeSnapshot struct {
	Size    uint64  `json:"size"`
	Updates uint64  `json:"updates,omitempty"`
	Total   int64   `json:"total"`
	Tree    []int64 `json:"tree"`
}

type fenwickTreeData struct {
	tree hatDataStructure.FenwickTree
}

func newFenwickTreeData(size uint64) (fenwickTreeData, error) {
	tree, err := hatDataStructure.NewFenwickTree(size)
	return fenwickTreeData{tree: tree}, err
}

func newDefaultFenwickTreeData() fenwickTreeData {
	return fenwickTreeData{tree: hatDataStructure.NewDefaultFenwickTree()}
}

func validateFenwickTreeSize(size uint64) error {
	_, err := hatDataStructure.NewFenwickTree(size)
	return err
}

func validateFenwickTreeSnapshot(snapshot fenwickTreeSnapshot) error {
	return hatDataStructure.ValidateFenwickTreeSnapshot(fenwickTreeSnapshotToPublic(snapshot))
}

func newFenwickTreeDataFromSnapshot(snapshot fenwickTreeSnapshot) (fenwickTreeData, error) {
	tree, err := hatDataStructure.NewFenwickTreeFromSnapshot(fenwickTreeSnapshotToPublic(snapshot))
	return fenwickTreeData{tree: tree}, err
}

func fenwickTreeSnapshotToPublic(snapshot fenwickTreeSnapshot) hatDataStructure.FenwickTreeSnapshot {
	return hatDataStructure.FenwickTreeSnapshot{
		Size:    snapshot.Size,
		Updates: snapshot.Updates,
		Total:   snapshot.Total,
		Tree:    snapshot.Tree,
	}
}

func fenwickTreeSnapshotFromPublic(snapshot hatDataStructure.FenwickTreeSnapshot) fenwickTreeSnapshot {
	return fenwickTreeSnapshot{
		Size:    snapshot.Size,
		Updates: snapshot.Updates,
		Total:   snapshot.Total,
		Tree:    snapshot.Tree,
	}
}

func (tree *fenwickTreeData) Add(index uint64, delta int64) (FenwickTreeUpdate, bool) {
	if tree == nil {
		return FenwickTreeUpdate{}, false
	}
	return tree.tree.Add(index, delta)
}

func (tree fenwickTreeData) Value(index uint64) (int64, bool) {
	return tree.tree.Value(index)
}

func (tree fenwickTreeData) PrefixSum(index uint64) (int64, bool) {
	return tree.tree.PrefixSum(index)
}

func (tree fenwickTreeData) RangeSum(start uint64, end uint64) (int64, bool) {
	return tree.tree.RangeSum(start, end)
}

func (tree fenwickTreeData) Snapshot() fenwickTreeSnapshot {
	return fenwickTreeSnapshotFromPublic(tree.tree.Snapshot())
}

func (tree fenwickTreeData) Info() FenwickTreeInfo {
	return tree.tree.Info()
}

func (tree fenwickTreeData) EncodedSize() int64 {
	return tree.tree.EncodedSize()
}

func (tree fenwickTreeData) BackingLength() int {
	return tree.tree.BackingLength()
}

// FenwickTreeStorage stores compact Fenwick trees outside the trie.
type FenwickTreeStorage struct {
	array     []fenwickTreeData
	reusables reusableIndexes
}

func CreateFenwickTreeStorage() *FenwickTreeStorage {
	return &FenwickTreeStorage{
		array: []fenwickTreeData{},
	}
}

func (store *FenwickTreeStorage) PutData(idx int32, value fenwickTreeData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *FenwickTreeStorage) AppendData(value fenwickTreeData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *FenwickTreeStorage) AddData(value fenwickTreeData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *FenwickTreeStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = fenwickTreeData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertFenwickTree(key string, size uint64) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertFenwickTree(key, size)
	}

	data, err := newFenwickTreeData(size)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsFenwickTree() {
		ht.fenwickTrees.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.fenwickTrees.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_FENWICK_TREE}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddFenwickTree(key string, index uint64, delta int64) (FenwickTreeUpdate, bool) {
	update, ok, _ := ht.AddFenwickTreeChecked(key, index, delta)
	return update, ok
}

func (ht *HatTrie) AddFenwickTreeChecked(key string, index uint64, delta int64) (FenwickTreeUpdate, bool, error) {
	if ht == nil {
		return FenwickTreeUpdate{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AddFenwickTreeChecked(key, index, delta)
	}
	if delta == 0 || index >= maxFenwickTreeSize {
		return FenwickTreeUpdate{}, false, nil
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return FenwickTreeUpdate{}, false, err
	}
	if hval.IsFenwickTree() {
		update, ok := ht.fenwickTrees.array[hval.Index].Add(index, delta)
		if !ok {
			return FenwickTreeUpdate{}, false, nil
		}
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return update, true, nil
	}

	data := newDefaultFenwickTreeData()
	update, ok := data.Add(index, delta)
	if !ok {
		return FenwickTreeUpdate{}, false, nil
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.fenwickTrees.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_FENWICK_TREE}.toValue()
	ht.recordWriteLocked(key)
	return update, true, nil
}

func (ht *HatTrie) GetFenwickTree(key string, index uint64) (int64, bool) {
	value, ok, _ := ht.GetFenwickTreeChecked(key, index)
	return value, ok
}

func (ht *HatTrie) GetFenwickTreeChecked(key string, index uint64) (int64, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetFenwickTreeChecked(key, index)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	if !hval.IsFenwickTree() {
		ht.recordReadLocked(false, key)
		return 0, false, nil
	}
	value, ok := ht.fenwickTrees.array[hval.Index].Value(index)
	ht.recordReadLocked(ok, key)
	return value, ok, nil
}

func (ht *HatTrie) PrefixSumFenwickTree(key string, index uint64) (int64, bool) {
	value, ok, _ := ht.PrefixSumFenwickTreeChecked(key, index)
	return value, ok
}

func (ht *HatTrie) PrefixSumFenwickTreeChecked(key string, index uint64) (int64, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PrefixSumFenwickTreeChecked(key, index)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	if !hval.IsFenwickTree() {
		ht.recordReadLocked(false, key)
		return 0, false, nil
	}
	value, ok := ht.fenwickTrees.array[hval.Index].PrefixSum(index)
	ht.recordReadLocked(ok, key)
	return value, ok, nil
}

func (ht *HatTrie) RangeSumFenwickTree(key string, start uint64, end uint64) (int64, bool) {
	value, ok, _ := ht.RangeSumFenwickTreeChecked(key, start, end)
	return value, ok
}

func (ht *HatTrie) RangeSumFenwickTreeChecked(key string, start uint64, end uint64) (int64, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.RangeSumFenwickTreeChecked(key, start, end)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	if !hval.IsFenwickTree() {
		ht.recordReadLocked(false, key)
		return 0, false, nil
	}
	value, ok := ht.fenwickTrees.array[hval.Index].RangeSum(start, end)
	ht.recordReadLocked(ok, key)
	return value, ok, nil
}

func (ht *HatTrie) FenwickTreeInfo(key string) (FenwickTreeInfo, bool) {
	info, ok, _ := ht.FenwickTreeInfoChecked(key)
	return info, ok
}

func (ht *HatTrie) FenwickTreeInfoChecked(key string) (FenwickTreeInfo, bool, error) {
	if ht == nil {
		return FenwickTreeInfo{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.FenwickTreeInfoChecked(key)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return FenwickTreeInfo{}, false, err
	}
	if !hval.IsFenwickTree() {
		ht.recordReadLocked(false, key)
		return FenwickTreeInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.fenwickTrees.array[hval.Index].Info(), true, nil
}
