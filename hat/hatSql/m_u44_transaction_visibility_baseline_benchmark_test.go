//go:build mu44baseline

package hatSql

import (
	"sync"
	"testing"
)

func BenchmarkSQLDataflowVisibilityBaselineOneLock4(b *testing.B) {
	var mu sync.Mutex
	var next uint64
	var versions [4]uint64
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mu.Lock()
		next++
		for versionIndex := range versions {
			versions[versionIndex] = next
		}
		mu.Unlock()
	}
}

func BenchmarkSQLDataflowVisibilityBaselineIndependentLocks4(b *testing.B) {
	var mutexes [4]sync.Mutex
	var next uint64
	var versions [4]uint64
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		next++
		for versionIndex := range versions {
			mutexes[versionIndex].Lock()
			versions[versionIndex] = next
			mutexes[versionIndex].Unlock()
		}
	}
}
