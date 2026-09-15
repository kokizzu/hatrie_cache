package hatDataStructure

import (
	"fmt"
	"testing"
)

func BenchmarkSecondaryIndexPostingBuildC204(b *testing.B) {
	const rowCount = 10000
	for _, distinct := range []int{rowCount, 1000, 100, 10} {
		b.Run(fmt.Sprintf("functional-distinct-%d", distinct), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				index, err := NewFunctionalIndex(func(value int) int { return value % distinct }, rowCount)
				if err != nil {
					b.Fatal(err)
				}
				for value := 0; value < rowCount; value++ {
					if err := index.Upsert(uint64(value+1), value); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		b.Run(fmt.Sprintf("hash-distinct-%d", distinct), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				index, err := NewHashIndex(func(value int) int { return value % distinct }, HashIndexOptions{Capacity: rowCount})
				if err != nil {
					b.Fatal(err)
				}
				for value := 0; value < rowCount; value++ {
					if err := index.Upsert(uint64(value+1), value); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
