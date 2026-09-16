package hatSql

import (
	"fmt"
	"testing"
)

var chg02IndexSink int
var chg02MapSink map[string]int

func BenchmarkCHG02GroupedIndexLookup(b *testing.B) {
	for _, groups := range []int{1, 4, 8, 16, 64} {
		keys := make([]string, groups)
		for index := range keys {
			keys[index] = fmt.Sprintf("int64:%d", index)
		}
		b.Run(fmt.Sprintf("map/groups=%d", groups), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				indexes := make(map[string]int)
				for index, key := range keys {
					indexes[key] = index
				}
				sum := 0
				for index := 0; index < groups*4; index++ {
					value, ok := indexes[keys[index%groups]]
					if ok {
						sum += value
					}
				}
				chg02MapSink = indexes
				chg02IndexSink = sum
			}
		})
		b.Run(fmt.Sprintf("small-index/groups=%d", groups), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				indexes := newSQLHashGroupAggregateIndexes()
				for index, key := range keys {
					indexes.add(key, index)
				}
				sum := 0
				for index := 0; index < groups*4; index++ {
					value, ok := indexes.find(keys[index%groups])
					if ok {
						sum += value
					}
				}
				chg02IndexSink = sum
			}
		})
	}
}
