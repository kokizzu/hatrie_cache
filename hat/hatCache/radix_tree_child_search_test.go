package hatCache

import (
	"fmt"
	"testing"
)

func TestRadixTreeChildIndexMatchesLinearReference(t *testing.T) {
	for count := 0; count <= 256; count++ {
		children := make([]radixTreeNode, count)
		for index := range children {
			children[index].prefix = string([]byte{byte(index)})
		}
		node := radixTreeNode{children: children}
		for value := 0; value <= 255; value++ {
			wantIndex, wantFound := radixTreeLinearChildIndex(children, byte(value))
			gotIndex, gotFound := node.childIndex(byte(value))
			if gotIndex != wantIndex || gotFound != wantFound {
				t.Fatalf("count %d childIndex(%d) = (%d, %t), want (%d, %t)", count, value, gotIndex, gotFound, wantIndex, wantFound)
			}
		}
	}
}

func BenchmarkRadixTreeChildIndexFanout(b *testing.B) {
	for _, count := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256} {
		children := make([]radixTreeNode, count)
		for index := range children {
			children[index].prefix = string([]byte{byte(index * 256 / count)})
		}
		node := radixTreeNode{children: children}

		b.Run(fmt.Sprintf("Fanout%d/Binary", count), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkRadixTreeChildIndexSink, benchmarkBoolSink = node.childIndex(byte(iteration * 2654435761))
			}
		})
		b.Run(fmt.Sprintf("Fanout%d/Linear", count), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				benchmarkRadixTreeChildIndexSink, benchmarkBoolSink = radixTreeLinearChildIndex(children, byte(iteration*2654435761))
			}
		})
	}
}

func radixTreeLinearChildIndex(children []radixTreeNode, first byte) (int, bool) {
	for index := range children {
		childFirst := children[index].prefix[0]
		if childFirst >= first {
			return index, childFirst == first
		}
	}
	return len(children), false
}

var benchmarkRadixTreeChildIndexSink int
