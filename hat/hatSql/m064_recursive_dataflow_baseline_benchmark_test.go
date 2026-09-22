package hatSql

import "testing"

var m064RecursiveDataflowBaselineSink []int

func BenchmarkM064BaselineRecursiveDataflow(b *testing.B) {
	const nodeCount = 1024
	edges := make([][]int, nodeCount)
	for index := 0; index < nodeCount; index++ {
		if index+1 < nodeCount {
			edges[index] = append(edges[index], index+1)
		}
		if index+2 < nodeCount {
			edges[index] = append(edges[index], index+2)
		}
	}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		seen := map[int]struct{}{0: {}}
		all := []int{0}
		for {
			var next []int
			for _, source := range all {
				for _, target := range edges[source] {
					if _, ok := seen[target]; ok {
						continue
					}
					seen[target] = struct{}{}
					next = append(next, target)
				}
			}
			if len(next) == 0 {
				break
			}
			all = append(all, next...)
		}
		m064RecursiveDataflowBaselineSink = all
	}
}
