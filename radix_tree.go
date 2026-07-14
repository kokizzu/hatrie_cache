package hatriecache

import (
	"encoding/json"
	"errors"
	"sort"
)

// RadixTreeItem is one exact string key/value pair stored inside a compressed
// radix tree value.
type RadixTreeItem struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// RadixTreeInfo reports the shape and approximate compact encoded footprint of
// a path-compressed radix tree.
type RadixTreeInfo struct {
	Items        uint64 `json:"items"`
	Nodes        uint64 `json:"nodes"`
	Edges        uint64 `json:"edges"`
	LabelBytes   uint64 `json:"label_bytes"`
	ValueBytes   uint64 `json:"value_bytes"`
	EncodedBytes uint64 `json:"encoded_bytes"`
	MaxDepth     uint64 `json:"max_depth"`
}

type radixTreeSnapshot struct {
	Count uint64          `json:"count"`
	Items []RadixTreeItem `json:"items"`
}

type radixTreeData struct {
	root  *radixTreeNode
	items uint64
}

type radixTreeNode struct {
	prefix   string
	value    interface{}
	hasValue bool
	children []radixTreeNode
}

type radixTreeStats struct {
	nodes      uint64
	edges      uint64
	labelBytes uint64
	valueBytes uint64
	maxDepth   uint64
}

const maxRadixTreePrefixScanCapacity = 64

func newRadixTreeData() radixTreeData {
	return radixTreeData{root: &radixTreeNode{}}
}

func validateRadixTreeSnapshot(snapshot radixTreeSnapshot) error {
	if snapshot.Count != uint64(len(snapshot.Items)) {
		return errors.New("hatriecache: radix tree count does not match items")
	}
	var previous string
	for idx, item := range snapshot.Items {
		if idx > 0 && item.Key <= previous {
			return errors.New("hatriecache: radix tree items must be sorted and unique")
		}
		previous = item.Key
	}
	return nil
}

func newRadixTreeDataFromSnapshot(snapshot radixTreeSnapshot) (radixTreeData, error) {
	if err := validateRadixTreeSnapshot(snapshot); err != nil {
		return radixTreeData{}, err
	}
	tree := newRadixTreeData()
	for _, item := range snapshot.Items {
		tree.Put(item.Key, item.Value)
	}
	return tree, nil
}

func (tree *radixTreeData) Put(key string, value interface{}) bool {
	if tree == nil {
		return false
	}
	tree.ensureRoot()
	added := tree.root.put(key, value)
	if added {
		tree.items++
	}
	return added
}

func (tree *radixTreeData) PutEntries(entries Map) int {
	if tree == nil || len(entries) == 0 {
		return 0
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	added := 0
	for _, key := range keys {
		if tree.Put(key, entries[key]) {
			added++
		}
	}
	return added
}

func (tree radixTreeData) Get(key string) (interface{}, bool) {
	if tree.root == nil {
		return nil, false
	}
	value, ok := tree.root.get(key)
	return cloneValue(value), ok
}

func (tree *radixTreeData) Delete(key string) bool {
	if tree == nil || tree.root == nil {
		return false
	}
	if !tree.root.delete(key) {
		return false
	}
	tree.items--
	return true
}

func (tree radixTreeData) Contains(key string) bool {
	_, ok := tree.Get(key)
	return ok
}

func (tree radixTreeData) ItemsWithPrefix(prefix string) []RadixTreeItem {
	return tree.itemsWithPrefixCapacity(prefix, radixTreePrefixScanCapacity(tree.items))
}

func (tree radixTreeData) itemsWithPrefixCapacity(prefix string, capacity int) []RadixTreeItem {
	if tree.root == nil || tree.items == 0 {
		return []RadixTreeItem{}
	}
	items := make([]RadixTreeItem, 0, capacity)
	tree.root.collectPrefix(prefix, "", &items)
	return items
}

func (tree radixTreeData) Snapshot() radixTreeSnapshot {
	items := tree.itemsWithPrefixCapacity("", radixTreeFullScanCapacity(tree.items))
	return radixTreeSnapshot{
		Count: uint64(len(items)),
		Items: items,
	}
}

func (tree radixTreeData) Info() RadixTreeInfo {
	stats := tree.stats()
	return RadixTreeInfo{
		Items:        tree.items,
		Nodes:        stats.nodes,
		Edges:        stats.edges,
		LabelBytes:   stats.labelBytes,
		ValueBytes:   stats.valueBytes,
		EncodedBytes: stats.labelBytes + stats.valueBytes,
		MaxDepth:     stats.maxDepth,
	}
}

func (tree radixTreeData) EncodedSize() int64 {
	return int64(tree.Info().EncodedBytes)
}

func radixTreePrefixScanCapacity(items uint64) int {
	if items < maxRadixTreePrefixScanCapacity {
		return int(items)
	}
	return maxRadixTreePrefixScanCapacity
}

func radixTreeFullScanCapacity(items uint64) int {
	maxInt := uint64(int(^uint(0) >> 1))
	if items > maxInt {
		return 0
	}
	return int(items)
}

func (tree *radixTreeData) ensureRoot() {
	if tree.root == nil {
		tree.root = &radixTreeNode{}
	}
}

func (tree radixTreeData) stats() radixTreeStats {
	if tree.root == nil {
		return radixTreeStats{}
	}
	var stats radixTreeStats
	tree.root.collectStats(0, &stats)
	return stats
}

func (node *radixTreeNode) put(key string, value interface{}) bool {
	common := commonPrefixLen(key, node.prefix)
	if common < len(node.prefix) {
		child := radixTreeNode{
			prefix:   node.prefix[common:],
			value:    node.value,
			hasValue: node.hasValue,
			children: node.children,
		}
		node.prefix = node.prefix[:common]
		node.value = nil
		node.hasValue = false
		node.children = []radixTreeNode{child}
	}

	if common == len(key) {
		added := !node.hasValue
		node.value = cloneValue(value)
		node.hasValue = true
		return added
	}

	remainder := key[common:]
	idx, found := node.childIndex(remainder[0])
	if found {
		return node.children[idx].put(remainder, value)
	}
	child := radixTreeNode{
		prefix:   remainder,
		value:    cloneValue(value),
		hasValue: true,
	}
	node.children = insertRadixChild(node.children, idx, child)
	return true
}

func (node *radixTreeNode) get(key string) (interface{}, bool) {
	common := commonPrefixLen(key, node.prefix)
	if common != len(node.prefix) {
		return nil, false
	}
	if common == len(key) {
		if !node.hasValue {
			return nil, false
		}
		return node.value, true
	}
	remainder := key[common:]
	idx, found := node.childIndex(remainder[0])
	if !found {
		return nil, false
	}
	return node.children[idx].get(remainder)
}

func (node *radixTreeNode) delete(key string) bool {
	common := commonPrefixLen(key, node.prefix)
	if common != len(node.prefix) {
		return false
	}
	if common == len(key) {
		if !node.hasValue {
			return false
		}
		node.value = nil
		node.hasValue = false
		return true
	}

	remainder := key[common:]
	idx, found := node.childIndex(remainder[0])
	if !found {
		return false
	}
	if !node.children[idx].delete(remainder) {
		return false
	}
	node.compactChild(idx)
	return true
}

func (node *radixTreeNode) compactChild(idx int) {
	child := &node.children[idx]
	if !child.hasValue && len(child.children) == 0 {
		node.children = deleteRadixChild(node.children, idx)
		return
	}
	if !child.hasValue && len(child.children) == 1 {
		grandchild := child.children[0]
		child.prefix += grandchild.prefix
		child.value = grandchild.value
		child.hasValue = grandchild.hasValue
		child.children = grandchild.children
	}
}

func (node *radixTreeNode) collectPrefix(prefix string, path string, items *[]RadixTreeItem) {
	common := commonPrefixLen(prefix, node.prefix)
	if common < len(prefix) && common < len(node.prefix) {
		return
	}
	nextPath := path + node.prefix
	if common == len(prefix) {
		node.collect(nextPath, items)
		return
	}
	remainder := prefix[common:]
	idx, found := node.childIndex(remainder[0])
	if !found {
		return
	}
	node.children[idx].collectPrefix(remainder, nextPath, items)
}

func (node *radixTreeNode) collect(path string, items *[]RadixTreeItem) {
	if node.hasValue {
		*items = append(*items, RadixTreeItem{
			Key:   path,
			Value: cloneValue(node.value),
		})
	}
	for idx := range node.children {
		child := &node.children[idx]
		child.collect(path+child.prefix, items)
	}
}

func (node *radixTreeNode) collectStats(depth uint64, stats *radixTreeStats) {
	stats.nodes++
	stats.edges += uint64(len(node.children))
	stats.labelBytes += uint64(len(node.prefix))
	nextDepth := depth + uint64(len(node.prefix))
	if node.hasValue {
		stats.valueBytes += radixTreeValueSize(node.value)
		if nextDepth > stats.maxDepth {
			stats.maxDepth = nextDepth
		}
	}
	for idx := range node.children {
		node.children[idx].collectStats(nextDepth, stats)
	}
}

func (node *radixTreeNode) childIndex(first byte) (int, bool) {
	idx := sort.Search(len(node.children), func(idx int) bool {
		return node.children[idx].prefix[0] >= first
	})
	return idx, idx < len(node.children) && node.children[idx].prefix[0] == first
}

func commonPrefixLen(left string, right string) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	idx := 0
	for idx < limit && left[idx] == right[idx] {
		idx++
	}
	return idx
}

func insertRadixChild(children []radixTreeNode, idx int, child radixTreeNode) []radixTreeNode {
	children = append(children, radixTreeNode{})
	copy(children[idx+1:], children[idx:])
	children[idx] = child
	return children
}

func deleteRadixChild(children []radixTreeNode, idx int) []radixTreeNode {
	copy(children[idx:], children[idx+1:])
	last := len(children) - 1
	children[last] = radixTreeNode{}
	children = children[:last]
	if cap(children) > 16 && len(children)*4 < cap(children) {
		next := make([]radixTreeNode, len(children))
		copy(next, children)
		children = next
	}
	return children
}

func radixTreeValueSize(value interface{}) uint64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return uint64(len(typed))
	case []byte:
		return uint64(len(typed))
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return 0
		}
		return uint64(len(data))
	}
}

// RadixTreeStorage stores compressed radix tree values outside the trie.
type RadixTreeStorage struct {
	array     []radixTreeData
	reusables reusableIndexes
}

func CreateRadixTreeStorage() *RadixTreeStorage {
	return &RadixTreeStorage{
		array: []radixTreeData{},
	}
}

func (store *RadixTreeStorage) PutData(idx int32, value radixTreeData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *RadixTreeStorage) AppendData(value radixTreeData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *RadixTreeStorage) AddData(value radixTreeData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *RadixTreeStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = radixTreeData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertRadixTree(key string) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsRadixTree() {
		ht.radixTrees.PutData(hval.Index, newRadixTreeData())
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.radixTrees.AddData(newRadixTreeData())
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RADIX_TREE}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) PutRadixTree(key string, subkey string, val interface{}) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsRadixTree() {
		added := ht.radixTrees.array[hval.Index].Put(subkey, val)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return added
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newRadixTreeData()
	added := data.Put(subkey, val)
	idx := ht.radixTrees.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RADIX_TREE}.toValue()
	ht.recordWriteLocked(key)
	return added
}

func (ht *HatTrie) PutRadixTreeEntries(key string, entries Map) int {
	if len(entries) == 0 {
		return 0
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsRadixTree() {
		added := ht.radixTrees.array[hval.Index].PutEntries(entries)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return added
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newRadixTreeData()
	added := data.PutEntries(entries)
	idx := ht.radixTrees.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RADIX_TREE}.toValue()
	ht.recordWriteLocked(key)
	return added
}

func (ht *HatTrie) GetRadixTree(key string, subkey string) (interface{}, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsRadixTree() {
		ht.recordReadLocked(false, key)
		return nil, false
	}
	value, ok := ht.radixTrees.array[hval.Index].Get(subkey)
	ht.recordReadLocked(ok, key)
	return value, ok
}

func (ht *HatTrie) DeleteRadixTree(key string, subkey string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsRadixTree() {
		ht.recordReadLocked(false, key)
		return false
	}
	deleted := ht.radixTrees.array[hval.Index].Delete(subkey)
	ht.recordReadLocked(deleted, key)
	if deleted {
		ht.recordWriteLocked(key)
	}
	return deleted
}

func (ht *HatTrie) HasRadixTree(key string, subkey string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsRadixTree() {
		ht.recordReadLocked(false, key)
		return false
	}
	hit := ht.radixTrees.array[hval.Index].Contains(subkey)
	ht.recordReadLocked(hit, key)
	return hit
}

func (ht *HatTrie) ScanRadixTree(key string, prefix string) ([]RadixTreeItem, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsRadixTree() {
		ht.recordReadLocked(false, key)
		return nil, false
	}
	ht.recordReadLocked(true, key)
	return ht.radixTrees.array[hval.Index].ItemsWithPrefix(prefix), true
}

func (ht *HatTrie) RadixTreeInfo(key string) (RadixTreeInfo, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsRadixTree() {
		ht.recordReadLocked(false, key)
		return RadixTreeInfo{}, false
	}
	ht.recordReadLocked(true, key)
	return ht.radixTrees.array[hval.Index].Info(), true
}
