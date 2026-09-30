package hatSql

import (
	"fmt"
	"runtime"
	"testing"
)

const (
	m235BenchmarkObjectCount = 8192
	m235BenchmarkHotObjects  = 8
)

func newM235DependencyGraphBenchmarkFixture(b *testing.B) (*SQLDependencyGraph, []SQLMaintainedObject) {
	b.Helper()
	graph := NewSQLDependencyGraph()
	for index := 0; index < m235BenchmarkObjectCount; index++ {
		kind := SQLMaintainedObjectView
		if index%2 == 0 {
			kind = SQLMaintainedObjectIndex
		}
		dependency := fmt.Sprintf("source-%d", index)
		if index < m235BenchmarkHotObjects {
			dependency = "hot"
		}
		if err := graph.Register(kind, fmt.Sprintf("object-%05d", index), dependency); err != nil {
			b.Fatal(err)
		}
	}
	return graph, graph.Snapshot()
}

func BenchmarkM235LinearMaintainedObjectScan(b *testing.B) {
	_, objects := newM235DependencyGraphBenchmarkFixture(b)
	changed := "hot"
	affected := make([]SQLMaintainedObject, 0, m235BenchmarkHotObjects)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		affected = affected[:0]
		for _, object := range objects {
			for _, dependency := range object.Dependencies {
				if dependency != changed {
					continue
				}
				copyObject := object
				copyObject.Dependencies = append([]string(nil), object.Dependencies...)
				affected = append(affected, copyObject)
				break
			}
		}
		runtime.KeepAlive(affected)
	}
}

func BenchmarkM235IndexedDependencyGraphLookup(b *testing.B) {
	graph, _ := newM235DependencyGraphBenchmarkFixture(b)
	affected := make([]SQLMaintainedObject, 0, m235BenchmarkHotObjects)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		affected = graph.AffectedInto(affected, []string{"hot"})
		runtime.KeepAlive(affected)
	}
}

func BenchmarkM235IndexedDependencyGraphTwoSourceLookup(b *testing.B) {
	graph, _ := newM235DependencyGraphBenchmarkFixture(b)
	changed := []string{"hot", "source-4096"}
	affected := make([]SQLMaintainedObject, 0, m235BenchmarkHotObjects+1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		affected = graph.AffectedInto(affected, changed)
		runtime.KeepAlive(affected)
	}
}

func BenchmarkM235LinearMaintainedObjectTwoSourceScan(b *testing.B) {
	_, objects := newM235DependencyGraphBenchmarkFixture(b)
	affected := make([]SQLMaintainedObject, 0, m235BenchmarkHotObjects+1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		affected = affected[:0]
		for _, object := range objects {
			for _, dependency := range object.Dependencies {
				if dependency != "hot" && dependency != "source-4096" {
					continue
				}
				copyObject := object
				copyObject.Dependencies = append([]string(nil), object.Dependencies...)
				affected = append(affected, copyObject)
				break
			}
		}
		runtime.KeepAlive(affected)
	}
}
