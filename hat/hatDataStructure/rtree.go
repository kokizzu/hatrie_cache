package hatDataStructure

import (
	"fmt"
	"math"
	"slices"
	"sync"
)

const (
	// DefaultRTreeMaxEntries keeps nodes small enough for cache-friendly scans
	// while avoiding the height of a very narrow tree.
	DefaultRTreeMaxEntries = 16
	minRTreeMaxEntries     = 4
	maxRTreeMaxEntries     = 256
)

// RTreeBounds is an axis-aligned rectangle in a caller-defined coordinate
// system. Its edges are inclusive for intersection and point queries.
type RTreeBounds struct {
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
}

// Validate rejects NaN, infinity, and inverted rectangles.
func (bounds RTreeBounds) Validate() error {
	if math.IsNaN(bounds.MinX) || math.IsNaN(bounds.MinY) || math.IsNaN(bounds.MaxX) || math.IsNaN(bounds.MaxY) {
		return fmt.Errorf("rtree bounds cannot contain NaN")
	}
	if math.IsInf(bounds.MinX, 0) || math.IsInf(bounds.MinY, 0) || math.IsInf(bounds.MaxX, 0) || math.IsInf(bounds.MaxY, 0) {
		return fmt.Errorf("rtree bounds must be finite")
	}
	if bounds.MinX > bounds.MaxX || bounds.MinY > bounds.MaxY {
		return fmt.Errorf("rtree bounds minimum exceeds maximum")
	}
	return nil
}

// Intersects reports whether two rectangles overlap, including their edges.
func (bounds RTreeBounds) Intersects(other RTreeBounds) bool {
	return bounds.MinX <= other.MaxX && bounds.MaxX >= other.MinX && bounds.MinY <= other.MaxY && bounds.MaxY >= other.MinY
}

// ContainsPoint reports whether the point lies inside the rectangle,
// including its edges.
func (bounds RTreeBounds) ContainsPoint(x, y float64) bool {
	return x >= bounds.MinX && x <= bounds.MaxX && y >= bounds.MinY && y <= bounds.MaxY
}

// RTree is a concurrent in-memory R-tree for unique uint64 IDs. Search
// results are sorted by ID so callers do not depend on tree insertion order.
// The zero value is ready for use and uses DefaultRTreeMaxEntries.
type RTree struct {
	mu         sync.RWMutex
	maxEntries int
	minEntries int
	root       *rtreeNode
	entries    map[uint64]RTreeBounds
}

type rtreeNode struct {
	leaf     bool
	parent   *rtreeNode
	hasBound bool
	bounds   RTreeBounds
	items    []rtreeItem
	children []*rtreeNode
}

type rtreeItem struct {
	id     uint64
	bounds RTreeBounds
}

// NewRTree creates an R-tree with the requested maximum entries per node. A
// zero maxEntries selects DefaultRTreeMaxEntries.
func NewRTree(maxEntries int) (*RTree, error) {
	if maxEntries == 0 {
		maxEntries = DefaultRTreeMaxEntries
	}
	if maxEntries < minRTreeMaxEntries || maxEntries > maxRTreeMaxEntries {
		return nil, fmt.Errorf("rtree max entries must be between %d and %d", minRTreeMaxEntries, maxRTreeMaxEntries)
	}
	tree := &RTree{maxEntries: maxEntries, minEntries: maxEntries / 2, entries: make(map[uint64]RTreeBounds)}
	tree.root = newRTreeNode(true, maxEntries)
	return tree, nil
}

// NewDefaultRTree creates an R-tree with the default node fanout.
func NewDefaultRTree() *RTree {
	tree, err := NewRTree(DefaultRTreeMaxEntries)
	if err != nil {
		panic(err)
	}
	return tree
}

// Upsert adds a rectangle or replaces the rectangle stored under id.
func (tree *RTree) Upsert(id uint64, bounds RTreeBounds) error {
	if tree == nil {
		return fmt.Errorf("rtree is nil")
	}
	if err := bounds.Validate(); err != nil {
		return err
	}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	tree.ensureInitializedLocked()
	if oldBounds, ok := tree.entries[id]; ok {
		tree.removeItemLocked(id, oldBounds)
	}
	tree.insertItemLocked(rtreeItem{id: id, bounds: bounds})
	tree.entries[id] = bounds
	return nil
}

// Delete removes id and reports whether it was present.
func (tree *RTree) Delete(id uint64) bool {
	if tree == nil {
		return false
	}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if tree.entries == nil {
		return false
	}
	bounds, ok := tree.entries[id]
	if !ok || !tree.removeItemLocked(id, bounds) {
		return false
	}
	delete(tree.entries, id)
	return true
}

// Search returns IDs whose rectangles intersect bounds in ascending order.
func (tree *RTree) Search(bounds RTreeBounds) ([]uint64, error) {
	return tree.SearchInto(nil, bounds)
}

// SearchInto appends IDs whose rectangles intersect bounds to dst in
// ascending order. Reusing dst avoids a result allocation on steady-state
// queries; any values already in dst are preserved and are not reordered.
func (tree *RTree) SearchInto(dst []uint64, bounds RTreeBounds) ([]uint64, error) {
	if tree == nil {
		return nil, fmt.Errorf("rtree is nil")
	}
	if err := bounds.Validate(); err != nil {
		return nil, err
	}
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	start := len(dst)
	ids := dst
	if tree.root != nil && tree.root.hasBound {
		ids = tree.searchLocked(tree.root, bounds, ids)
	}
	slices.Sort(ids[start:])
	return ids, nil
}

// SearchPoint returns IDs whose rectangles contain x,y in ascending order.
func (tree *RTree) SearchPoint(x, y float64) ([]uint64, error) {
	return tree.SearchPointInto(nil, x, y)
}

// SearchPointInto is the reusable-buffer form of SearchPoint.
func (tree *RTree) SearchPointInto(dst []uint64, x, y float64) ([]uint64, error) {
	if math.IsNaN(x) || math.IsNaN(y) || math.IsInf(x, 0) || math.IsInf(y, 0) {
		return nil, fmt.Errorf("rtree point must be finite")
	}
	return tree.SearchInto(dst, RTreeBounds{MinX: x, MinY: y, MaxX: x, MaxY: y})
}

// Len returns the number of rectangles in the tree.
func (tree *RTree) Len() int {
	if tree == nil {
		return 0
	}
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	return len(tree.entries)
}

func (tree *RTree) ensureInitializedLocked() {
	if tree.maxEntries == 0 {
		tree.maxEntries = DefaultRTreeMaxEntries
		tree.minEntries = DefaultRTreeMaxEntries / 2
	}
	if tree.entries == nil {
		tree.entries = make(map[uint64]RTreeBounds)
	}
	if tree.root == nil {
		tree.root = newRTreeNode(true, tree.maxEntries)
	}
}

func newRTreeNode(leaf bool, maxEntries int) *rtreeNode {
	node := &rtreeNode{leaf: leaf}
	if leaf {
		node.items = make([]rtreeItem, 0, maxEntries+1)
	} else {
		node.children = make([]*rtreeNode, 0, maxEntries+1)
	}
	return node
}

func (tree *RTree) insertItemLocked(item rtreeItem) {
	leaf := tree.chooseLeafLocked(tree.root, item.bounds)
	leaf.items = append(leaf.items, item)
	tree.recalculateToRootLocked(leaf)
	if len(leaf.items) > tree.maxEntries {
		tree.splitNodeLocked(leaf)
	}
}

func (tree *RTree) chooseLeafLocked(node *rtreeNode, bounds RTreeBounds) *rtreeNode {
	for !node.leaf {
		best := node.children[0]
		bestEnlargement := rtreeEnlargement(best.bounds, bounds)
		bestArea := rtreeArea(best.bounds)
		for _, child := range node.children[1:] {
			enlargement := rtreeEnlargement(child.bounds, bounds)
			area := rtreeArea(child.bounds)
			if enlargement < bestEnlargement || enlargement == bestEnlargement && (area < bestArea || area == bestArea && lenRTreeNodeEntries(child) < lenRTreeNodeEntries(best)) {
				best = child
				bestEnlargement = enlargement
				bestArea = area
			}
		}
		node = best
	}
	return node
}

func (tree *RTree) splitNodeLocked(node *rtreeNode) {
	left, right := tree.splitContentsLocked(node)
	parent := node.parent
	if parent == nil {
		root := newRTreeNode(false, tree.maxEntries)
		root.children = append(root.children, left, right)
		left.parent = root
		right.parent = root
		tree.root = root
		tree.recalculateNodeLocked(root)
		return
	}
	for index, child := range parent.children {
		if child == node {
			parent.children[index] = left
			break
		}
	}
	left.parent = parent
	right.parent = parent
	parent.children = append(parent.children, right)
	tree.recalculateNodeLocked(parent)
	if len(parent.children) > tree.maxEntries {
		tree.splitNodeLocked(parent)
	} else {
		tree.recalculateToRootLocked(parent)
	}
}

func (tree *RTree) splitContentsLocked(node *rtreeNode) (*rtreeNode, *rtreeNode) {
	left := newRTreeNode(node.leaf, tree.maxEntries)
	right := newRTreeNode(node.leaf, tree.maxEntries)
	count := lenRTreeNodeEntries(node)
	seedLeft, seedRight := rtreeSplitSeeds(node)
	used := make([]bool, count)
	used[seedLeft] = true
	used[seedRight] = true
	tree.addRTreeEntry(left, node, seedLeft)
	tree.addRTreeEntry(right, node, seedRight)
	remaining := count - 2
	for remaining > 0 {
		if lenRTreeNodeEntries(left)+remaining == tree.minEntries {
			for index := range used {
				if !used[index] {
					tree.addRTreeEntry(left, node, index)
					used[index] = true
					remaining--
				}
			}
			break
		}
		if lenRTreeNodeEntries(right)+remaining == tree.minEntries {
			for index := range used {
				if !used[index] {
					tree.addRTreeEntry(right, node, index)
					used[index] = true
					remaining--
				}
			}
			break
		}
		next := -1
		bestDifference := float64(-1)
		for index := range used {
			if used[index] {
				continue
			}
			bounds := rtreeNodeEntryBounds(node, index)
			difference := math.Abs(rtreeEnlargement(left.bounds, bounds) - rtreeEnlargement(right.bounds, bounds))
			if next < 0 || difference > bestDifference {
				next = index
				bestDifference = difference
			}
		}
		bounds := rtreeNodeEntryBounds(node, next)
		if rtreeEnlargement(left.bounds, bounds) < rtreeEnlargement(right.bounds, bounds) || rtreeEnlargement(left.bounds, bounds) == rtreeEnlargement(right.bounds, bounds) && (rtreeArea(left.bounds) < rtreeArea(right.bounds) || rtreeArea(left.bounds) == rtreeArea(right.bounds) && lenRTreeNodeEntries(left) <= lenRTreeNodeEntries(right)) {
			tree.addRTreeEntry(left, node, next)
		} else {
			tree.addRTreeEntry(right, node, next)
		}
		used[next] = true
		remaining--
	}
	return left, right
}

func (tree *RTree) addRTreeEntry(target, source *rtreeNode, index int) {
	if source.leaf {
		target.items = append(target.items, source.items[index])
	} else {
		child := source.children[index]
		target.children = append(target.children, child)
		child.parent = target
	}
	tree.recalculateNodeLocked(target)
}

func rtreeSplitSeeds(node *rtreeNode) (int, int) {
	left, right := 0, 1
	bestWaste := float64(-1)
	for first := 0; first < lenRTreeNodeEntries(node); first++ {
		for second := first + 1; second < lenRTreeNodeEntries(node); second++ {
			firstBounds := rtreeNodeEntryBounds(node, first)
			secondBounds := rtreeNodeEntryBounds(node, second)
			waste := rtreeArea(rtreeUnionBounds(firstBounds, secondBounds)) - rtreeArea(firstBounds) - rtreeArea(secondBounds)
			if math.IsNaN(waste) {
				waste = 0
			}
			if waste > bestWaste {
				left, right, bestWaste = first, second, waste
			}
		}
	}
	return left, right
}

func (tree *RTree) searchLocked(node *rtreeNode, bounds RTreeBounds, ids []uint64) []uint64 {
	if !node.hasBound || !node.bounds.Intersects(bounds) {
		return ids
	}
	if node.leaf {
		for _, item := range node.items {
			if item.bounds.Intersects(bounds) {
				ids = append(ids, item.id)
			}
		}
		return ids
	}
	for _, child := range node.children {
		ids = tree.searchLocked(child, bounds, ids)
	}
	return ids
}

func (tree *RTree) removeItemLocked(id uint64, bounds RTreeBounds) bool {
	leaf, index, ok := findRTreeItem(tree.root, id, bounds)
	if !ok {
		return false
	}
	last := len(leaf.items) - 1
	leaf.items[index] = leaf.items[last]
	leaf.items = leaf.items[:last]
	orphans := tree.condenseLocked(leaf)
	for _, item := range orphans {
		tree.insertItemLocked(item)
	}
	return true
}

func findRTreeItem(node *rtreeNode, id uint64, bounds RTreeBounds) (*rtreeNode, int, bool) {
	if node == nil || !node.hasBound || !node.bounds.Intersects(bounds) {
		return nil, 0, false
	}
	if node.leaf {
		for index, item := range node.items {
			if item.id == id {
				return node, index, true
			}
		}
		return nil, 0, false
	}
	for _, child := range node.children {
		if leaf, index, ok := findRTreeItem(child, id, bounds); ok {
			return leaf, index, true
		}
	}
	return nil, 0, false
}

func (tree *RTree) condenseLocked(node *rtreeNode) []rtreeItem {
	var orphans []rtreeItem
	for node != nil && node != tree.root {
		parent := node.parent
		if lenRTreeNodeEntries(node) < tree.minEntries {
			for index, child := range parent.children {
				if child == node {
					parent.children = append(parent.children[:index], parent.children[index+1:]...)
					break
				}
			}
			orphans = collectRTreeItems(node, orphans)
			node.parent = nil
		} else {
			tree.recalculateNodeLocked(node)
		}
		node = parent
	}
	if tree.root != nil {
		tree.recalculateNodeLocked(tree.root)
		for !tree.root.leaf && len(tree.root.children) == 1 {
			tree.root = tree.root.children[0]
			tree.root.parent = nil
		}
		if !tree.root.leaf && len(tree.root.children) == 0 {
			tree.root = newRTreeNode(true, tree.maxEntries)
		}
	}
	return orphans
}

func collectRTreeItems(node *rtreeNode, items []rtreeItem) []rtreeItem {
	if node.leaf {
		return append(items, node.items...)
	}
	for _, child := range node.children {
		items = collectRTreeItems(child, items)
	}
	return items
}

func (tree *RTree) recalculateToRootLocked(node *rtreeNode) {
	for node != nil {
		tree.recalculateNodeLocked(node)
		node = node.parent
	}
}

func (tree *RTree) recalculateNodeLocked(node *rtreeNode) {
	if node.leaf {
		if len(node.items) == 0 {
			node.hasBound = false
			node.bounds = RTreeBounds{}
			return
		}
		node.bounds = node.items[0].bounds
		node.hasBound = true
		for _, item := range node.items[1:] {
			node.bounds = rtreeUnionBounds(node.bounds, item.bounds)
		}
		return
	}
	if len(node.children) == 0 {
		node.hasBound = false
		node.bounds = RTreeBounds{}
		return
	}
	first := node.children[0]
	if !first.hasBound {
		node.hasBound = false
		node.bounds = RTreeBounds{}
		return
	}
	node.bounds = first.bounds
	node.hasBound = true
	for _, child := range node.children[1:] {
		if child.hasBound {
			node.bounds = rtreeUnionBounds(node.bounds, child.bounds)
		}
	}
}

func lenRTreeNodeEntries(node *rtreeNode) int {
	if node.leaf {
		return len(node.items)
	}
	return len(node.children)
}

func rtreeNodeEntryBounds(node *rtreeNode, index int) RTreeBounds {
	if node.leaf {
		return node.items[index].bounds
	}
	return node.children[index].bounds
}

func rtreeUnionBounds(left, right RTreeBounds) RTreeBounds {
	return RTreeBounds{
		MinX: math.Min(left.MinX, right.MinX),
		MinY: math.Min(left.MinY, right.MinY),
		MaxX: math.Max(left.MaxX, right.MaxX),
		MaxY: math.Max(left.MaxY, right.MaxY),
	}
}

func rtreeArea(bounds RTreeBounds) float64 {
	width := bounds.MaxX - bounds.MinX
	height := bounds.MaxY - bounds.MinY
	if width <= 0 || height <= 0 {
		return 0
	}
	area := width * height
	if math.IsInf(area, 0) {
		return math.MaxFloat64
	}
	return area
}

func rtreeEnlargement(current, addition RTreeBounds) float64 {
	union := rtreeUnionBounds(current, addition)
	enlargement := rtreeArea(union) - rtreeArea(current)
	if math.IsNaN(enlargement) || enlargement < 0 {
		return 0
	}
	return enlargement
}
