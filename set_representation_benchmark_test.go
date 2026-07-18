package hatriecache

import (
	"fmt"
	"testing"
)

var setRepresentationBenchmarkHit bool
var setRepresentationBenchmarkValues Set

type setRepresentationLinearEntry struct {
	key   string
	value string
}

func BenchmarkSetRepresentationSmallDuplicateAdd(b *testing.B) {
	for _, size := range []int{1, 2, 3, 4, 8, 16, 32, 64} {
		keys, entries := setRepresentationBenchmarkData(size)
		targetKey := keys[size-1]
		targetValue := entries[size-1].value

		b.Run(fmt.Sprintf("map/%02d", size), func(b *testing.B) {
			items := make(map[string]interface{}, size)
			for idx, key := range keys {
				items[key] = entries[idx].value
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, exists := items[targetKey]; exists {
					setRepresentationBenchmarkHit = true
					continue
				}
				items[targetKey] = targetValue
			}
		})

		b.Run(fmt.Sprintf("slice/%02d", size), func(b *testing.B) {
			items := append([]setRepresentationLinearEntry(nil), entries...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				hit := false
				for idx := range items {
					if items[idx].key == targetKey {
						hit = true
						break
					}
				}
				if hit {
					setRepresentationBenchmarkHit = true
					continue
				}
				items = append(items, setRepresentationLinearEntry{key: targetKey, value: targetValue})
			}
		})
	}
}

func BenchmarkSetRepresentationSmallLookup(b *testing.B) {
	for _, size := range []int{1, 2, 3, 4, 8, 16, 32, 64} {
		keys, entries := setRepresentationBenchmarkData(size)
		targetKey := keys[size-1]

		b.Run(fmt.Sprintf("map/%02d", size), func(b *testing.B) {
			items := make(map[string]interface{}, size)
			for idx, key := range keys {
				items[key] = entries[idx].value
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, setRepresentationBenchmarkHit = items[targetKey]
			}
		})

		b.Run(fmt.Sprintf("slice/%02d", size), func(b *testing.B) {
			items := append([]setRepresentationLinearEntry(nil), entries...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				hit := false
				for idx := range items {
					if items[idx].key == targetKey {
						hit = true
						break
					}
				}
				setRepresentationBenchmarkHit = hit
			}
		})
	}
}

func BenchmarkSetRepresentationSmallValues(b *testing.B) {
	set := setData{}
	set.addPlainString("beta")
	set.addPlainString("alpha")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		setRepresentationBenchmarkValues = set.Values()
	}
}

func setRepresentationBenchmarkData(size int) ([]string, []setRepresentationLinearEntry) {
	keys := make([]string, size)
	entries := make([]setRepresentationLinearEntry, size)
	for idx := 0; idx < size; idx++ {
		value := fmt.Sprintf("value-%02d", idx)
		key := `"` + value + `"`
		keys[idx] = key
		entries[idx] = setRepresentationLinearEntry{key: key, value: value}
	}
	return keys, entries
}
