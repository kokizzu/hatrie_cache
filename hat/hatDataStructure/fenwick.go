// Package hatDataStructure provides reusable compact data structures that do
// not depend on the cache server.
package hatDataStructure

import (
	"errors"
	"strconv"

	json "github.com/goccy/go-json"
)

const (
	DefaultFenwickTreeSize = uint64(1024)
	MaxFenwickTreeSize     = uint64(1 << 20)
	maxFenwickTreeSize     = MaxFenwickTreeSize
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

// FenwickTreeSnapshot is a portable, validated tree representation. Tree is
// deep-copied at both snapshot and restore boundaries.
type FenwickTreeSnapshot struct {
	Size    uint64  `json:"size"`
	Updates uint64  `json:"updates,omitempty"`
	Total   int64   `json:"total"`
	Tree    []int64 `json:"tree"`
}

// FenwickTree stores signed point values with logarithmic point updates and
// prefix/range queries. Its zero backing is retained lazily until first use.
type FenwickTree struct {
	size    uint64
	updates uint64
	total   int64
	tree    []int64
}

// Internal aliases preserve focused algorithm tests without exposing cache
// adapters through the public API.
type fenwickTreeData = FenwickTree

func newFenwickTreeData(size uint64) (fenwickTreeData, error) {
	return NewFenwickTree(size)
}

func newDefaultFenwickTreeData() fenwickTreeData {
	return NewDefaultFenwickTree()
}

func NewFenwickTree(size uint64) (FenwickTree, error) {
	if err := validateFenwickTreeSize(size); err != nil {
		return FenwickTree{}, err
	}
	return FenwickTree{size: size}, nil
}

func NewDefaultFenwickTree() FenwickTree {
	tree, err := NewFenwickTree(DefaultFenwickTreeSize)
	if err != nil {
		panic(err)
	}
	return tree
}

func ValidateFenwickTreeSnapshot(snapshot FenwickTreeSnapshot) error {
	if err := validateFenwickTreeSize(snapshot.Size); err != nil {
		return err
	}
	if len(snapshot.Tree) == 0 {
		if snapshot.Total != 0 {
			return errors.New("hatriecache: empty fenwick tree snapshot has nonzero total")
		}
		return nil
	}
	if len(snapshot.Tree) != int(snapshot.Size)+1 {
		return errors.New("hatriecache: fenwick tree snapshot size does not match tree length")
	}
	if snapshot.Tree[0] != 0 {
		return errors.New("hatriecache: fenwick tree snapshot sentinel must be zero")
	}
	tree := FenwickTree{
		size:    snapshot.Size,
		updates: snapshot.Updates,
		total:   snapshot.Total,
		tree:    snapshot.Tree,
	}
	total, ok := tree.PrefixSum(snapshot.Size - 1)
	if !ok {
		return errors.New("hatriecache: fenwick tree snapshot totals overflow")
	}
	if total != snapshot.Total {
		return errors.New("hatriecache: fenwick tree snapshot total does not match tree")
	}
	return nil
}

func NewFenwickTreeFromSnapshot(snapshot FenwickTreeSnapshot) (FenwickTree, error) {
	if err := ValidateFenwickTreeSnapshot(snapshot); err != nil {
		return FenwickTree{}, err
	}
	tree := FenwickTree{
		size:    snapshot.Size,
		updates: snapshot.Updates,
		total:   snapshot.Total,
	}
	if len(snapshot.Tree) == 0 || fenwickTreeRawIsZero(snapshot.Tree) {
		return tree, nil
	}
	tree.tree = append([]int64(nil), snapshot.Tree...)
	return tree, nil
}

func (tree *FenwickTree) Add(index uint64, delta int64) (FenwickTreeUpdate, bool) {
	if tree == nil || delta == 0 || index >= tree.size || !tree.validShape() {
		return FenwickTreeUpdate{}, false
	}
	value, prefix, total, ok := tree.prepareAdd(index, delta)
	if !ok {
		return FenwickTreeUpdate{}, false
	}
	tree.ensureTree()
	for pos := index + 1; pos <= tree.size; pos += pos & -pos {
		tree.tree[pos] += delta
	}
	tree.total = total
	tree.updates = saturatingAddUint64(tree.updates, 1)
	update := FenwickTreeUpdate{
		Index:     index,
		Delta:     delta,
		Value:     value,
		PrefixSum: prefix,
		Total:     tree.total,
		Updates:   tree.updates,
	}
	tree.compactIfZero()
	return update, true
}

func (tree FenwickTree) Value(index uint64) (int64, bool) {
	return tree.RangeSum(index, index)
}

func (tree FenwickTree) PrefixSum(index uint64) (int64, bool) {
	if index >= tree.size || !tree.validShape() {
		return 0, false
	}
	if len(tree.tree) == 0 {
		return 0, true
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

func (tree FenwickTree) RangeSum(start uint64, end uint64) (int64, bool) {
	if start > end || end >= tree.size || !tree.validShape() {
		return 0, false
	}
	if len(tree.tree) == 0 {
		return 0, true
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

func (tree FenwickTree) Snapshot() FenwickTreeSnapshot {
	out := FenwickTreeSnapshot{
		Size:    tree.size,
		Updates: tree.updates,
		Total:   tree.total,
		Tree:    []int64{},
	}
	if len(tree.tree) > 0 && !fenwickTreeRawIsZero(tree.tree) {
		out.Tree = append([]int64(nil), tree.tree...)
	}
	return out
}

func (tree FenwickTree) Info() FenwickTreeInfo {
	info := FenwickTreeInfo{
		Size:      tree.size,
		Updates:   tree.updates,
		Total:     tree.total,
		TreeBytes: uint64(len(tree.tree)) * 8,
	}
	info.EncodedBytes = tree.EncodedSize()
	return info
}

func (tree FenwickTree) EncodedSize() int64 {
	data, err := json.Marshal(tree.Snapshot())
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (tree FenwickTree) BackingLength() int {
	return len(tree.tree)
}

func validateFenwickTreeSize(size uint64) error {
	if size == 0 || size > MaxFenwickTreeSize {
		return errors.New("hatriecache: fenwick tree size must be between 1 and " + strconv.FormatUint(MaxFenwickTreeSize, 10))
	}
	return nil
}

func (tree FenwickTree) prepareAdd(index uint64, delta int64) (int64, int64, int64, bool) {
	prefix, ok := tree.PrefixSum(index)
	if !ok {
		return 0, 0, 0, false
	}
	value := prefix
	if index > 0 {
		left, ok := tree.PrefixSum(index - 1)
		if !ok {
			return 0, 0, 0, false
		}
		value, ok = checkedSubFenwickInt64(prefix, left)
		if !ok {
			return 0, 0, 0, false
		}
	}
	value, ok = checkedAddFenwickInt64(value, delta)
	if !ok {
		return 0, 0, 0, false
	}
	prefix, ok = checkedAddFenwickInt64(prefix, delta)
	if !ok {
		return 0, 0, 0, false
	}
	total, ok := checkedAddFenwickInt64(tree.total, delta)
	if !ok {
		return 0, 0, 0, false
	}
	if len(tree.tree) == 0 {
		return value, prefix, total, true
	}
	for pos := index + 1; pos <= tree.size; pos += pos & -pos {
		if _, ok := checkedAddFenwickInt64(tree.tree[pos], delta); !ok {
			return 0, 0, 0, false
		}
	}
	return value, prefix, total, true
}

func (tree FenwickTree) validShape() bool {
	return len(tree.tree) == 0 || len(tree.tree) == int(tree.size)+1
}

func (tree *FenwickTree) ensureTree() {
	if tree != nil && len(tree.tree) == 0 && tree.size > 0 {
		tree.tree = make([]int64, int(tree.size)+1)
	}
}

func (tree *FenwickTree) compactIfZero() {
	if tree != nil && tree.total == 0 && fenwickTreeRawIsZero(tree.tree) {
		tree.tree = nil
	}
}

func fenwickTreeRawIsZero(values []int64) bool {
	for _, value := range values {
		if value != 0 {
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

func saturatingAddUint64(value uint64, delta uint64) uint64 {
	if ^uint64(0)-value < delta {
		return ^uint64(0)
	}
	return value + delta
}
