package hatriecache

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func BenchmarkRadixTreePlainStringPutPaths(b *testing.B) {
	b.Run("DuplicateGeneric", func(b *testing.B) {
		tree := newRadixTreeData()
		tree.Put("user:100/profile", "active")
		values := [...]string{"active", "active"}
		b.ReportAllocs()
		b.ResetTimer()
		for idx := 0; idx < b.N; idx++ {
			tree.Put("user:100/profile", values[idx&1])
		}
	})
	b.Run("DuplicateTyped", func(b *testing.B) {
		tree := newRadixTreeData()
		tree.PutPlainString("user:100/profile", "active")
		values := [...]string{"active", "active"}
		b.ReportAllocs()
		b.ResetTimer()
		for idx := 0; idx < b.N; idx++ {
			tree.PutPlainString("user:100/profile", values[idx&1])
		}
	})
	b.Run("ReplacementGeneric", func(b *testing.B) {
		tree := newRadixTreeData()
		tree.Put("user:100/profile", "active")
		values := [...]string{"active", "idle"}
		b.ReportAllocs()
		b.ResetTimer()
		for idx := 0; idx < b.N; idx++ {
			tree.Put("user:100/profile", values[idx&1])
		}
	})
	b.Run("ReplacementTyped", func(b *testing.B) {
		tree := newRadixTreeData()
		tree.PutPlainString("user:100/profile", "active")
		values := [...]string{"active", "idle"}
		b.ReportAllocs()
		b.ResetTimer()
		for idx := 0; idx < b.N; idx++ {
			tree.PutPlainString("user:100/profile", values[idx&1])
		}
	})
	b.Run("Build128Generic", func(b *testing.B) {
		keys, values := radixTreeBenchmarkEntries(128)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			tree := newRadixTreeData()
			for idx, key := range keys {
				tree.Put(key, values[idx])
			}
		}
	})
	b.Run("Build128Typed", func(b *testing.B) {
		keys, values := radixTreeBenchmarkEntries(128)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			tree := newRadixTreeData()
			for idx, key := range keys {
				tree.PutPlainString(key, values[idx])
			}
		}
	})
}

func BenchmarkRadixTreePlainStringPutLookupControl(b *testing.B) {
	trees := [...]radixTreeData{newRadixTreeData(), newRadixTreeData()}
	keys := radixTreeBenchmarkKeys(128)
	for _, key := range keys {
		trees[0].Put(key, "active")
		trees[1].PutPlainString(key, "active")
	}
	for idx, name := range [...]string{"GenericBuilt", "TypedBuilt"} {
		b.Run(name, func(b *testing.B) {
			tree := &trees[idx]
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if value, ok := tree.Get(keys[iteration&(len(keys)-1)]); !ok || value != "active" {
					b.Fatal("lookup failed")
				}
			}
		})
	}
}

func BenchmarkRadixTreePlainStringPutAlternating(b *testing.B) {
	const puts = 1 << 16
	for _, benchmark := range []struct {
		name      string
		replacing bool
	}{
		{name: "Duplicate"},
		{name: "Replacement", replacing: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate := newRadixTreeData()
			control := newRadixTreeData()
			candidate.PutPlainString("user:100/profile", "active")
			control.Put("user:100/profile", "active")
			var candidateDuration, controlDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				candidateFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if candidateFirst == (pass == 0) {
						for put := 0; put < puts; put++ {
							value := "active"
							if benchmark.replacing && put&1 != 0 {
								value = "idle"
							}
							candidate.PutPlainString("user:100/profile", value)
						}
						candidateDuration += time.Since(started)
					} else {
						for put := 0; put < puts; put++ {
							value := "active"
							if benchmark.replacing && put&1 != 0 {
								value = "idle"
							}
							control.Put("user:100/profile", value)
						}
						controlDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * puts)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/put")
			b.ReportMetric(float64(controlDuration.Nanoseconds())/operations, "control-ns/put")
		})
	}
}

func BenchmarkRadixTreePlainStringBuildAlternating(b *testing.B) {
	const builds = 64
	keys, values := radixTreeBenchmarkEntries(128)
	var candidateDuration, controlDuration time.Duration
	for iteration := 0; iteration < b.N; iteration++ {
		candidateFirst := iteration&1 != 0
		for pass := 0; pass < 2; pass++ {
			started := time.Now()
			if candidateFirst == (pass == 0) {
				for build := 0; build < builds; build++ {
					tree := newRadixTreeData()
					for idx, key := range keys {
						tree.PutPlainString(key, values[idx])
					}
				}
				candidateDuration += time.Since(started)
			} else {
				for build := 0; build < builds; build++ {
					tree := newRadixTreeData()
					for idx, key := range keys {
						tree.Put(key, values[idx])
					}
				}
				controlDuration += time.Since(started)
			}
		}
	}
	operations := float64(b.N * builds)
	b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/build")
	b.ReportMetric(float64(controlDuration.Nanoseconds())/operations, "control-ns/build")
}

func BenchmarkRadixTreePutEntriesOrderAlternating(b *testing.B) {
	for _, size := range []int{64, 4096} {
		b.Run(fmt.Sprintf("Build%d", size), func(b *testing.B) {
			entries := radixTreeBenchmarkMap(size)
			builds := 256
			if size >= 4096 {
				builds = 4
			}
			var directDuration, sortedDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				directFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if directFirst == (pass == 0) {
						for build := 0; build < builds; build++ {
							tree := newRadixTreeData()
							if added := tree.PutEntries(entries); added != len(entries) {
								b.Fatalf("direct added = %d, want %d", added, len(entries))
							}
						}
						directDuration += time.Since(started)
					} else {
						for build := 0; build < builds; build++ {
							tree := newRadixTreeData()
							if added := radixTreePutEntriesSorted(&tree, entries); added != len(entries) {
								b.Fatalf("sorted added = %d, want %d", added, len(entries))
							}
						}
						sortedDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * builds)
			b.ReportMetric(float64(directDuration.Nanoseconds())/operations, "direct-ns/build")
			b.ReportMetric(float64(sortedDuration.Nanoseconds())/operations, "sorted-ns/build")
		})
	}
}

func BenchmarkRadixTreePutEntriesReplacementAlternating(b *testing.B) {
	for _, size := range []int{64, 4096} {
		b.Run(fmt.Sprintf("Replace%d", size), func(b *testing.B) {
			entries := radixTreeBenchmarkMap(size)
			directTree := newRadixTreeData()
			sortedTree := newRadixTreeData()
			directTree.PutEntries(entries)
			radixTreePutEntriesSorted(&sortedTree, entries)
			updates := 256
			if size >= 4096 {
				updates = 4
			}
			var directDuration, sortedDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				directFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if directFirst == (pass == 0) {
						for update := 0; update < updates; update++ {
							if added := directTree.PutEntries(entries); added != 0 {
								b.Fatalf("direct replacement added = %d, want 0", added)
							}
						}
						directDuration += time.Since(started)
					} else {
						for update := 0; update < updates; update++ {
							if added := radixTreePutEntriesSorted(&sortedTree, entries); added != 0 {
								b.Fatalf("sorted replacement added = %d, want 0", added)
							}
						}
						sortedDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * updates)
			b.ReportMetric(float64(directDuration.Nanoseconds())/operations, "direct-ns/update")
			b.ReportMetric(float64(sortedDuration.Nanoseconds())/operations, "sorted-ns/update")
		})
	}
}

func BenchmarkRadixTreePutEntriesOrderAllocations(b *testing.B) {
	for _, size := range []int{64, 4096} {
		entries := radixTreeBenchmarkMap(size)
		b.Run(fmt.Sprintf("Sorted%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				tree := newRadixTreeData()
				radixTreePutEntriesSorted(&tree, entries)
			}
		})
		b.Run(fmt.Sprintf("Direct%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				tree := newRadixTreeData()
				tree.PutEntries(entries)
			}
		})
		directTree := newRadixTreeData()
		sortedTree := newRadixTreeData()
		directTree.PutEntries(entries)
		radixTreePutEntriesSorted(&sortedTree, entries)
		b.Run(fmt.Sprintf("ReplaceSorted%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				radixTreePutEntriesSorted(&sortedTree, entries)
			}
		})
		b.Run(fmt.Sprintf("ReplaceDirect%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				directTree.PutEntries(entries)
			}
		})
	}
}

func radixTreeBenchmarkMap(count int) Map {
	keys, values := radixTreeBenchmarkEntries(count)
	entries := make(Map, count)
	for index, key := range keys {
		entries[key] = values[index]
	}
	return entries
}

func radixTreePutEntriesSorted(tree *radixTreeData, entries Map) int {
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

func radixTreeBenchmarkKeys(count int) []string {
	keys := make([]string, count)
	for idx := range keys {
		keys[idx] = fmt.Sprintf("user:%06d/profile", idx)
	}
	return keys
}

func radixTreeBenchmarkEntries(count int) ([]string, []string) {
	keys := make([]string, count)
	values := make([]string, count)
	for idx := range keys {
		keys[idx] = fmt.Sprintf("user:%06d/profile", idx)
		values[idx] = fmt.Sprintf("state:%06d", idx)
	}
	return keys, values
}
