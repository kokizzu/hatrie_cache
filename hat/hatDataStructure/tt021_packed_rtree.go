package hatDataStructure

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	// DefaultPackedRTreeLeafSize is the default number of entries or child
	// nodes grouped under one packed node.
	DefaultPackedRTreeLeafSize = 16
)

var (
	// ErrPackedRTreeNil reports a method call on a nil tree.
	ErrPackedRTreeNil = errors.New("hatriecache: packed R-tree is nil")
	// ErrPackedRTreeOptionsInvalid reports an invalid tree build option.
	ErrPackedRTreeOptionsInvalid = errors.New("hatriecache: packed R-tree options are invalid")
	// ErrSpatialBoxInvalid reports a box with non-finite or reversed bounds.
	ErrSpatialBoxInvalid = errors.New("hatriecache: spatial box is invalid")
	// ErrPackedRTreeVisitorRequired reports a nil query visitor.
	ErrPackedRTreeVisitorRequired = errors.New("hatriecache: packed R-tree visitor is required")
)

// SpatialBox is an inclusive two-dimensional axis-aligned bounding box.
// Coordinates must be finite and Min must not exceed Max on either axis.
type SpatialBox struct {
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
}

// Valid reports whether the box can be indexed or queried.
func (box SpatialBox) Valid() bool {
	return finiteSpatialCoordinate(box.MinX) &&
		finiteSpatialCoordinate(box.MinY) &&
		finiteSpatialCoordinate(box.MaxX) &&
		finiteSpatialCoordinate(box.MaxY) &&
		box.MinX <= box.MaxX && box.MinY <= box.MaxY
}

// Intersects reports inclusive intersection with other. Invalid boxes never
// intersect; constructors and query methods return ErrSpatialBoxInvalid for
// invalid input.
func (box SpatialBox) Intersects(other SpatialBox) bool {
	return box.Valid() && other.Valid() && box.intersects(other)
}

func (box SpatialBox) intersects(other SpatialBox) bool {
	return box.MaxX >= other.MinX && box.MinX <= other.MaxX &&
		box.MaxY >= other.MinY && box.MinY <= other.MaxY
}

// SpatialEntry associates one immutable value with an indexed bounding box.
type SpatialEntry[T any] struct {
	Bounds SpatialBox
	Value  T
}

// PackedRTreeOptions controls immutable packed-tree construction.
type PackedRTreeOptions struct {
	// LeafSize bounds both leaf entries and internal child fan-out. Zero uses
	// DefaultPackedRTreeLeafSize.
	LeafSize int
}

type packedRTreeNode struct {
	bounds SpatialBox
	first  int
	count  int
	leaf   bool
}

// PackedRTree is an immutable, bulk-built R-tree. Build-time sorting creates
// spatially coherent leaves; queries then traverse compact integer ranges
// without per-node pointers or per-query heap state in the common case.
type PackedRTree[T any] struct {
	entries  []SpatialEntry[T]
	nodes    []packedRTreeNode
	children []int
	root     int
	leafSize int
}

// NewPackedRTree copies and bulk-builds entries into an immutable packed
// spatial index. Rebuild the tree when the indexed set changes.
func NewPackedRTree[T any](entries []SpatialEntry[T], options PackedRTreeOptions) (*PackedRTree[T], error) {
	copyEntries := append([]SpatialEntry[T](nil), entries...)
	return newPackedRTreeOwned(copyEntries, options)
}

func newPackedRTreeOwned[T any](entries []SpatialEntry[T], options PackedRTreeOptions) (*PackedRTree[T], error) {
	leafSize := options.LeafSize
	if leafSize == 0 {
		leafSize = DefaultPackedRTreeLeafSize
	}
	if leafSize < 1 {
		return nil, ErrPackedRTreeOptionsInvalid
	}
	for index := range entries {
		if !entries[index].Bounds.Valid() {
			return nil, fmt.Errorf("%w: entry %d", ErrSpatialBoxInvalid, index)
		}
	}
	tree := &PackedRTree[T]{
		entries:  entries,
		root:     -1,
		leafSize: leafSize,
	}
	if len(entries) == 0 {
		return tree, nil
	}
	tree.build()
	return tree, nil
}

// Len returns the number of indexed entries.
func (tree *PackedRTree[T]) Len() int {
	if tree == nil {
		return 0
	}
	return len(tree.entries)
}

// Query returns values whose boxes intersect query. Result order is the
// deterministic packed traversal order, not the caller's input order.
func (tree *PackedRTree[T]) Query(query SpatialBox) ([]T, error) {
	return tree.QueryInto(query, nil)
}

// QueryInto appends matching values into a reused destination after first
// clearing its length. It avoids result allocation when destination capacity
// is sufficient.
func (tree *PackedRTree[T]) QueryInto(query SpatialBox, destination []T) ([]T, error) {
	if tree == nil {
		return nil, ErrPackedRTreeNil
	}
	if !query.Valid() {
		return nil, ErrSpatialBoxInvalid
	}
	destination = destination[:0]
	if tree.root < 0 {
		return destination, nil
	}
	var stackStorage [64]int
	stack := stackStorage[:1]
	stack[0] = tree.root
	for len(stack) > 0 {
		last := len(stack) - 1
		nodeIndex := stack[last]
		stack = stack[:last]
		node := tree.nodes[nodeIndex]
		if !node.bounds.intersects(query) {
			continue
		}
		if node.leaf {
			end := node.first + node.count
			for index := node.first; index < end; index++ {
				entry := tree.entries[index]
				if entry.Bounds.intersects(query) {
					destination = append(destination, entry.Value)
				}
			}
			continue
		}
		end := node.first + node.count
		for index := end - 1; index >= node.first; index-- {
			stack = append(stack, tree.children[index])
		}
	}
	return destination, nil
}

// Visit invokes visitor for each matching value until the visitor returns
// false. It avoids result-slice allocation and returns the number delivered.
func (tree *PackedRTree[T]) Visit(query SpatialBox, visitor func(T) bool) (int, error) {
	if tree == nil {
		return 0, ErrPackedRTreeNil
	}
	if !query.Valid() {
		return 0, ErrSpatialBoxInvalid
	}
	if visitor == nil {
		return 0, ErrPackedRTreeVisitorRequired
	}
	if tree.root < 0 {
		return 0, nil
	}
	var stackStorage [64]int
	stack := stackStorage[:1]
	stack[0] = tree.root
	visited := 0
	for len(stack) > 0 {
		last := len(stack) - 1
		nodeIndex := stack[last]
		stack = stack[:last]
		node := tree.nodes[nodeIndex]
		if !node.bounds.intersects(query) {
			continue
		}
		if node.leaf {
			end := node.first + node.count
			for index := node.first; index < end; index++ {
				entry := tree.entries[index]
				if entry.Bounds.intersects(query) {
					visited++
					if !visitor(entry.Value) {
						return visited, nil
					}
				}
			}
			continue
		}
		end := node.first + node.count
		for index := end - 1; index >= node.first; index-- {
			stack = append(stack, tree.children[index])
		}
	}
	return visited, nil
}

func (tree *PackedRTree[T]) build() {
	leafCount := (len(tree.entries) + tree.leafSize - 1) / tree.leafSize
	sliceCount := 1
	for sliceCount*sliceCount < leafCount {
		sliceCount++
	}
	sliceSize := (len(tree.entries) + sliceCount - 1) / sliceCount
	sort.SliceStable(tree.entries, func(left, right int) bool {
		leftX, leftY := spatialBoxCenter(tree.entries[left].Bounds)
		rightX, rightY := spatialBoxCenter(tree.entries[right].Bounds)
		if leftX != rightX {
			return leftX < rightX
		}
		return leftY < rightY
	})
	for start := 0; start < len(tree.entries); start += sliceSize {
		end := start + sliceSize
		if end > len(tree.entries) {
			end = len(tree.entries)
		}
		sort.SliceStable(tree.entries[start:end], func(left, right int) bool {
			leftX, leftY := spatialBoxCenter(tree.entries[start+left].Bounds)
			rightX, rightY := spatialBoxCenter(tree.entries[start+right].Bounds)
			if leftY != rightY {
				return leftY < rightY
			}
			return leftX < rightX
		})
	}

	level := make([]int, 0, leafCount)
	for first := 0; first < len(tree.entries); first += tree.leafSize {
		end := first + tree.leafSize
		if end > len(tree.entries) {
			end = len(tree.entries)
		}
		bounds := tree.entries[first].Bounds
		for index := first + 1; index < end; index++ {
			bounds = bounds.union(tree.entries[index].Bounds)
		}
		tree.nodes = append(tree.nodes, packedRTreeNode{bounds: bounds, first: first, count: end - first, leaf: true})
		level = append(level, len(tree.nodes)-1)
	}
	for len(level) > 1 {
		next := make([]int, 0, (len(level)+tree.leafSize-1)/tree.leafSize)
		for first := 0; first < len(level); first += tree.leafSize {
			end := first + tree.leafSize
			if end > len(level) {
				end = len(level)
			}
			childStart := len(tree.children)
			tree.children = append(tree.children, level[first:end]...)
			bounds := tree.nodes[level[first]].bounds
			for index := first + 1; index < end; index++ {
				bounds = bounds.union(tree.nodes[level[index]].bounds)
			}
			tree.nodes = append(tree.nodes, packedRTreeNode{
				bounds: bounds,
				first:  childStart,
				count:  end - first,
			})
			next = append(next, len(tree.nodes)-1)
		}
		level = next
	}
	tree.root = level[0]
}

func (box SpatialBox) union(other SpatialBox) SpatialBox {
	if other.MinX < box.MinX {
		box.MinX = other.MinX
	}
	if other.MinY < box.MinY {
		box.MinY = other.MinY
	}
	if other.MaxX > box.MaxX {
		box.MaxX = other.MaxX
	}
	if other.MaxY > box.MaxY {
		box.MaxY = other.MaxY
	}
	return box
}

func spatialBoxCenter(box SpatialBox) (float64, float64) {
	return (box.MinX + box.MaxX) / 2, (box.MinY + box.MaxY) / 2
}

func finiteSpatialCoordinate(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
