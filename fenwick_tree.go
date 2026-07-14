package hatriecache

import (
	"encoding/json"
	"errors"
	"strconv"
)

const (
	DefaultFenwickTreeSize = uint64(1024)
	maxFenwickTreeSize     = uint64(1 << 20)
)

const (
	maxFenwickTreeInt64 = int64(^uint64(0) >> 1)
	minFenwickTreeInt64 = -maxFenwickTreeInt64 - 1
)

// FenwickTreeUpdate reports the result of an O(log n) point update.
type FenwickTreeUpdate struct {
	Index     uint64 `json:"index"`
	Delta     int64  `json:"delta"`
	Value     int64  `json:"value"`
	PrefixSum int64  `json:"prefix_sum"`
	Total     int64  `json:"total"`
	Updates   uint64 `json:"updates"`
}

// FenwickTreeInfo reports the shape and memory footprint of a compact prefix
// sum tree.
type FenwickTreeInfo struct {
	Size         uint64 `json:"size"`
	Updates      uint64 `json:"updates"`
	Total        int64  `json:"total"`
	TreeBytes    uint64 `json:"tree_bytes"`
	EncodedBytes int64  `json:"encoded_bytes"`
}

type fenwickTreeSnapshot struct {
	Size    uint64  `json:"size"`
	Updates uint64  `json:"updates,omitempty"`
	Total   int64   `json:"total"`
	Tree    []int64 `json:"tree"`
}

type fenwickTreeData struct {
	size    uint64
	updates uint64
	total   int64
	tree    []int64
}

func newFenwickTreeData(size uint64) (fenwickTreeData, error) {
	if err := validateFenwickTreeSize(size); err != nil {
		return fenwickTreeData{}, err
	}
	return fenwickTreeData{
		size: size,
		tree: make([]int64, int(size)+1),
	}, nil
}

func newDefaultFenwickTreeData() fenwickTreeData {
	data, err := newFenwickTreeData(DefaultFenwickTreeSize)
	if err != nil {
		panic(err)
	}
	return data
}

func validateFenwickTreeSize(size uint64) error {
	if size == 0 || size > maxFenwickTreeSize {
		return errors.New("hatriecache: fenwick tree size must be between 1 and " + strconv.FormatUint(maxFenwickTreeSize, 10))
	}
	return nil
}

func validateFenwickTreeSnapshot(snapshot fenwickTreeSnapshot) error {
	if err := validateFenwickTreeSize(snapshot.Size); err != nil {
		return err
	}
	if len(snapshot.Tree) != int(snapshot.Size)+1 {
		return errors.New("hatriecache: fenwick tree snapshot size does not match tree length")
	}
	if snapshot.Tree[0] != 0 {
		return errors.New("hatriecache: fenwick tree snapshot sentinel must be zero")
	}
	data := fenwickTreeData{
		size:    snapshot.Size,
		updates: snapshot.Updates,
		total:   snapshot.Total,
		tree:    snapshot.Tree,
	}
	total, ok := data.PrefixSum(snapshot.Size - 1)
	if !ok {
		return errors.New("hatriecache: fenwick tree snapshot totals overflow")
	}
	if total != snapshot.Total {
		return errors.New("hatriecache: fenwick tree snapshot total does not match tree")
	}
	return nil
}

func newFenwickTreeDataFromSnapshot(snapshot fenwickTreeSnapshot) (fenwickTreeData, error) {
	if err := validateFenwickTreeSnapshot(snapshot); err != nil {
		return fenwickTreeData{}, err
	}
	data := fenwickTreeData{
		size:    snapshot.Size,
		updates: snapshot.Updates,
		total:   snapshot.Total,
		tree:    make([]int64, len(snapshot.Tree)),
	}
	copy(data.tree, snapshot.Tree)
	return data, nil
}

func (tree *fenwickTreeData) Add(index uint64, delta int64) (FenwickTreeUpdate, bool) {
	if tree == nil || delta == 0 || index >= tree.size || len(tree.tree) != int(tree.size)+1 {
		return FenwickTreeUpdate{}, false
	}
	if !tree.canAdd(index, delta) {
		return FenwickTreeUpdate{}, false
	}
	for pos := index + 1; pos <= tree.size; pos += pos & -pos {
		tree.tree[pos] += delta
	}
	tree.total += delta
	tree.updates = saturatingAddUint64(tree.updates, 1)
	value, ok := tree.Value(index)
	if !ok {
		return FenwickTreeUpdate{}, false
	}
	prefix, ok := tree.PrefixSum(index)
	if !ok {
		return FenwickTreeUpdate{}, false
	}
	return FenwickTreeUpdate{
		Index:     index,
		Delta:     delta,
		Value:     value,
		PrefixSum: prefix,
		Total:     tree.total,
		Updates:   tree.updates,
	}, true
}

func (tree fenwickTreeData) Value(index uint64) (int64, bool) {
	return tree.RangeSum(index, index)
}

func (tree fenwickTreeData) PrefixSum(index uint64) (int64, bool) {
	if index >= tree.size || len(tree.tree) != int(tree.size)+1 {
		return 0, false
	}
	var sum int64
	for pos := index + 1; pos > 0; pos -= pos & -pos {
		next, ok := checkedAddFenwickInt64(sum, tree.tree[pos])
		if !ok {
			return 0, false
		}
		sum = next
	}
	return sum, true
}

func (tree fenwickTreeData) RangeSum(start uint64, end uint64) (int64, bool) {
	if start > end || end >= tree.size || len(tree.tree) != int(tree.size)+1 {
		return 0, false
	}
	right, ok := tree.PrefixSum(end)
	if !ok {
		return 0, false
	}
	if start == 0 {
		return right, true
	}
	left, ok := tree.PrefixSum(start - 1)
	if !ok {
		return 0, false
	}
	return checkedSubFenwickInt64(right, left)
}

func (tree fenwickTreeData) Snapshot() fenwickTreeSnapshot {
	out := make([]int64, len(tree.tree))
	copy(out, tree.tree)
	return fenwickTreeSnapshot{
		Size:    tree.size,
		Updates: tree.updates,
		Total:   tree.total,
		Tree:    out,
	}
}

func (tree fenwickTreeData) Info() FenwickTreeInfo {
	info := FenwickTreeInfo{
		Size:      tree.size,
		Updates:   tree.updates,
		Total:     tree.total,
		TreeBytes: uint64(len(tree.tree)) * 8,
	}
	info.EncodedBytes = tree.EncodedSize()
	return info
}

func (tree fenwickTreeData) EncodedSize() int64 {
	data, err := json.Marshal(tree.Snapshot())
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (tree fenwickTreeData) canAdd(index uint64, delta int64) bool {
	value, ok := tree.Value(index)
	if !ok {
		return false
	}
	if _, ok := checkedAddFenwickInt64(value, delta); !ok {
		return false
	}
	prefix, ok := tree.PrefixSum(index)
	if !ok {
		return false
	}
	if _, ok := checkedAddFenwickInt64(prefix, delta); !ok {
		return false
	}
	if _, ok := checkedAddFenwickInt64(tree.total, delta); !ok {
		return false
	}
	for pos := index + 1; pos <= tree.size; pos += pos & -pos {
		if _, ok := checkedAddFenwickInt64(tree.tree[pos], delta); !ok {
			return false
		}
	}
	return true
}

func checkedAddFenwickInt64(left int64, right int64) (int64, bool) {
	if right > 0 && left > maxFenwickTreeInt64-right {
		return 0, false
	}
	if right < 0 && left < minFenwickTreeInt64-right {
		return 0, false
	}
	return left + right, true
}

func checkedSubFenwickInt64(left int64, right int64) (int64, bool) {
	if right > 0 && left < minFenwickTreeInt64+right {
		return 0, false
	}
	if right < 0 && left > maxFenwickTreeInt64+right {
		return 0, false
	}
	return left - right, true
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
	data, err := newFenwickTreeData(size)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertReplacementLocation(key)
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
