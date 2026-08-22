package hatDataStructure

import (
	"reflect"
	"testing"
)

var benchmarkFenwickTreeUpdateSink FenwickTreeUpdate

func TestFenwickTreeAddResponseMatchesPostUpdateQueries(t *testing.T) {
	tree, err := newFenwickTreeData(17)
	if err != nil {
		t.Fatal(err)
	}
	updates := []struct {
		index uint64
		delta int64
	}{
		{index: 0, delta: 5},
		{index: 16, delta: -3},
		{index: 7, delta: 11},
		{index: 3, delta: -2},
		{index: 7, delta: -4},
		{index: 1, delta: 9},
	}
	for sequence, operation := range updates {
		update, ok := tree.Add(operation.index, operation.delta)
		if !ok {
			t.Fatalf("Add(%d, %d) failed", operation.index, operation.delta)
		}
		value, ok := tree.Value(operation.index)
		if !ok {
			t.Fatalf("Value(%d) failed", operation.index)
		}
		prefix, ok := tree.PrefixSum(operation.index)
		if !ok {
			t.Fatalf("PrefixSum(%d) failed", operation.index)
		}
		if update.Index != operation.index || update.Delta != operation.delta || update.Value != value || update.PrefixSum != prefix || update.Total != tree.total || update.Updates != uint64(sequence+1) {
			t.Fatalf("Add(%d, %d) = %#v, post value/prefix/total = %d/%d/%d", operation.index, operation.delta, update, value, prefix, tree.total)
		}
	}

	overflow, err := newFenwickTreeData(4)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := overflow.Add(1, maxFenwickTreeInt64); !ok {
		t.Fatal("initial maximum update failed")
	}
	before := overflow.Snapshot()
	if _, ok := overflow.Add(1, 1); ok {
		t.Fatal("overflow update succeeded")
	}
	if after := overflow.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("failed update mutated tree: after=%#v before=%#v", after, before)
	}
}

func TestFenwickTreeAddMatchesTraversalReference(t *testing.T) {
	got, err := newFenwickTreeData(257)
	if err != nil {
		t.Fatal(err)
	}
	want := got
	state := uint64(0x9e3779b97f4a7c15)
	for iteration := 0; iteration < 10000; iteration++ {
		state = state*6364136223846793005 + 1442695040888963407
		index := state % got.size
		delta := int64((state>>32)%2001) - 1000
		if delta == 0 {
			delta = 1
		}
		gotUpdate, gotOK := got.Add(index, delta)
		wantUpdate, wantOK := fenwickTreeAddTraversalReference(&want, index, delta)
		if gotOK != wantOK || gotUpdate != wantUpdate {
			t.Fatalf("operation %d Add(%d, %d) = %#v/%v, want %#v/%v", iteration, index, delta, gotUpdate, gotOK, wantUpdate, wantOK)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("operation %d Add(%d, %d) state differs", iteration, index, delta)
		}
	}

	for _, delta := range []int64{maxFenwickTreeInt64, 1, minFenwickTreeInt64, -1} {
		gotUpdate, gotOK := got.Add(0, delta)
		wantUpdate, wantOK := fenwickTreeAddTraversalReference(&want, 0, delta)
		if gotOK != wantOK || gotUpdate != wantUpdate || !reflect.DeepEqual(got, want) {
			t.Fatalf("boundary Add(0, %d) = %#v/%v, want %#v/%v", delta, gotUpdate, gotOK, wantUpdate, wantOK)
		}
	}
}

func BenchmarkFenwickTreeAddTraversal(b *testing.B) {
	for _, size := range []uint64{1024, maxFenwickTreeSize} {
		b.Run(fenwickTreeBenchmarkName(size), func(b *testing.B) {
			tree, err := newFenwickTreeData(size)
			if err != nil {
				b.Fatal(err)
			}
			if _, ok := tree.Add(0, 1); !ok {
				b.Fatal("initial update failed")
			}
			index := size/2 - 1
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				delta := int64(1)
				if iteration&1 != 0 {
					delta = -1
				}
				var ok bool
				benchmarkFenwickTreeUpdateSink, ok = tree.Add(index, delta)
				if !ok {
					b.Fatal("update failed")
				}
			}
		})
	}
}

func BenchmarkFenwickTreeFirstAdd(b *testing.B) {
	b.Run("TraversalReference", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			tree := newDefaultFenwickTreeData()
			var ok bool
			benchmarkFenwickTreeUpdateSink, ok = fenwickTreeAddTraversalReference(&tree, 511, 1)
			if !ok {
				b.Fatal("update failed")
			}
		}
	})
	b.Run("PreparedResult", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			tree := newDefaultFenwickTreeData()
			var ok bool
			benchmarkFenwickTreeUpdateSink, ok = tree.Add(511, 1)
			if !ok {
				b.Fatal("update failed")
			}
		}
	})
}

func fenwickTreeBenchmarkName(size uint64) string {
	if size == maxFenwickTreeSize {
		return "Size1M"
	}
	return "Size1K"
}

func fenwickTreeAddTraversalReference(tree *fenwickTreeData, index uint64, delta int64) (FenwickTreeUpdate, bool) {
	if tree == nil || delta == 0 || index >= tree.size || !tree.validShape() {
		return FenwickTreeUpdate{}, false
	}
	if !fenwickTreeCanAddTraversalReference(*tree, index, delta) {
		return FenwickTreeUpdate{}, false
	}
	tree.ensureTree()
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

func fenwickTreeCanAddTraversalReference(tree fenwickTreeData, index uint64, delta int64) bool {
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
	if len(tree.tree) == 0 {
		return true
	}
	for pos := index + 1; pos <= tree.size; pos += pos & -pos {
		if _, ok := checkedAddFenwickInt64(tree.tree[pos], delta); !ok {
			return false
		}
	}
	return true
}
