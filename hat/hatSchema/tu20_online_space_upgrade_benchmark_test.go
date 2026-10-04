package hatSchema

import (
	"context"
	"testing"
)

type tu20BenchmarkOld struct {
	Name string
	Age  int
}

type tu20BenchmarkNew struct {
	Name   string
	Age    int
	Active bool
}

// BenchmarkOnlineSpaceUpgradeBaseline measures the direct dual-write work
// without the coordinator's phase and callback contract.
func BenchmarkOnlineSpaceUpgradeBaseline(b *testing.B) {
	oldRows := make(map[string]tu20BenchmarkOld, 1024)
	newRows := make(map[string]tu20BenchmarkNew, 1024)
	old := tu20BenchmarkOld{Name: "Ada", Age: 37}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		key := "user-1"
		oldRows[key] = old
		newRows[key] = tu20BenchmarkNew{Name: old.Name, Age: old.Age, Active: true}
	}
}

// BenchmarkOnlineSpaceUpgradeWrite measures the opt-in coordinator write path
// using the same conversion and map-backed dual-write work as the baseline.
func BenchmarkOnlineSpaceUpgradeWrite(b *testing.B) {
	backend := newTU20Backend(map[string]tu20Legacy{"user-1": {Name: "Ada", Age: 37}})
	upgrade, err := newTU20Upgrade(backend, 256)
	if err != nil {
		b.Fatal(err)
	}
	old := tu20Legacy{Name: "Ada", Age: 37}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := upgrade.Write(ctx, "user-1", old); err != nil {
			b.Fatal(err)
		}
	}
}
