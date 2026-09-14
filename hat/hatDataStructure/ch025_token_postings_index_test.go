package hatDataStructure

import (
	"reflect"
	"sync"
	"testing"
)

func TestCH025TokenPostingsIndexSearchAndUpdates(t *testing.T) {
	index := NewTokenPostingsIndex()
	index.Upsert(10, "Go fast, low-memory")
	index.Upsert(20, "Fast query execution")
	index.Upsert(30, "memory efficient query")

	if got := index.RowsForToken("FAST"); !reflect.DeepEqual(got, []uint32{10, 20}) {
		t.Fatalf("FAST rows = %v, want [10 20]", got)
	}
	if got := index.MatchAll("fast query"); !reflect.DeepEqual(got, []uint32{20}) {
		t.Fatalf("fast query rows = %v, want [20]", got)
	}
	if got := index.MatchAny("low efficient"); !reflect.DeepEqual(got, []uint32{10, 30}) {
		t.Fatalf("low efficient rows = %v, want [10 30]", got)
	}

	var visited []uint32
	index.VisitToken("memory", func(row uint32) bool {
		visited = append(visited, row)
		return true
	})
	if !reflect.DeepEqual(visited, []uint32{10, 30}) {
		t.Fatalf("visited memory rows = %v, want [10 30]", visited)
	}

	index.Upsert(10, "Go compact")
	if got := index.RowsForToken("fast"); !reflect.DeepEqual(got, []uint32{20}) {
		t.Fatalf("FAST rows after upsert = %v, want [20]", got)
	}
	if got := index.RowsForToken("compact"); !reflect.DeepEqual(got, []uint32{10}) {
		t.Fatalf("compact rows = %v, want [10]", got)
	}
	if got := index.Len(); got != 3 {
		t.Fatalf("Len after upsert = %d, want 3", got)
	}

	if !index.Delete(20) {
		t.Fatal("Delete(20) = false, want true")
	}
	if index.Delete(20) {
		t.Fatal("second Delete(20) = true, want false")
	}
	if got := index.RowsForToken("fast"); got != nil {
		t.Fatalf("FAST rows after delete = %v, want nil", got)
	}
}

func TestCH025TokenPostingsIndexNormalizationAndReusableBuffers(t *testing.T) {
	index := NewTokenPostingsIndex()
	index.Upsert(7, "Café café 123, mixed-case")
	index.Upsert(9, "cafe")

	if got := index.RowsForToken("CAFÉ"); !reflect.DeepEqual(got, []uint32{7}) {
		t.Fatalf("CAFÉ rows = %v, want [7]", got)
	}
	if got := index.RowsForToken("123"); !reflect.DeepEqual(got, []uint32{7}) {
		t.Fatalf("123 rows = %v, want [7]", got)
	}

	dst := []uint32{1}
	dst = index.RowsForTokenInto(dst, "cafe")
	if !reflect.DeepEqual(dst, []uint32{1, 9}) {
		t.Fatalf("reusable rows = %v, want [1 9]", dst)
	}

	dst = []uint32{2}
	dst = index.MatchAllInto(dst, "missing")
	if !reflect.DeepEqual(dst, []uint32{2}) {
		t.Fatalf("missing MatchAllInto = %v, want unchanged [2]", dst)
	}

	stopped := 0
	index.VisitToken("café", func(uint32) bool {
		stopped++
		return false
	})
	if stopped != 1 {
		t.Fatalf("early-stop visits = %d, want 1", stopped)
	}
}

func TestCH025TokenPostingsIndexInfoAndEmptyRows(t *testing.T) {
	index := NewTokenPostingsIndex()
	index.Upsert(1, "")
	index.Upsert(2, "!!!")
	if got := index.Len(); got != 0 {
		t.Fatalf("Len for empty documents = %d, want 0", got)
	}
	index.Upsert(1, "one two")
	info := index.Info()
	if info.Rows != 1 || info.Terms != 2 || info.Postings != 2 {
		t.Fatalf("Info = %+v, want one row/two terms/two postings", info)
	}
	index.Clear()
	if index.Len() != 0 || index.TermCount() != 0 {
		t.Fatalf("after Clear Len/TermCount = %d/%d, want 0/0", index.Len(), index.TermCount())
	}
}

func TestCH025TokenPostingsIndexZeroValueAndDensePosting(t *testing.T) {
	var index TokenPostingsIndex
	for row := uint32(0); row < 5000; row++ {
		index.Upsert(65000+row, "dense")
	}

	rows := index.RowsForToken("DENSE")
	if len(rows) != 5000 || rows[0] != 65000 || rows[len(rows)-1] != 69999 {
		t.Fatalf("dense rows length/edges = %d/%d/%d, want 5000/65000/69999", len(rows), rows[0], rows[len(rows)-1])
	}
	visited := 0
	if !index.VisitToken("dense", func(row uint32) bool {
		if row != 65000+uint32(visited) {
			t.Fatalf("dense visit row %d at position %d", row, visited)
		}
		visited++
		return true
	}) {
		t.Fatal("dense VisitToken returned false, want true")
	}
	if visited != 5000 {
		t.Fatalf("dense visited = %d, want 5000", visited)
	}
}

func TestCH025TokenPostingsIndexConcurrentMixedOperations(t *testing.T) {
	index := NewTokenPostingsIndex()
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			for iteration := 0; iteration < 100; iteration++ {
				row := uint32(worker*1000 + iteration)
				index.Upsert(row, "shared token")
				index.RowsForToken("shared")
				index.MatchAll("shared token")
				index.VisitToken("shared", func(uint32) bool { return true })
				if iteration%2 == 0 {
					index.Delete(row)
				}
			}
		}(worker)
	}
	waitGroup.Wait()
	if index.Len() == 0 {
		t.Fatal("Len after concurrent updates = 0, want retained rows")
	}
}

func TestCH025TokenPostingsIndexMemoryReport(t *testing.T) {
	index := buildCH025TokenPostingsIndex(ch025BenchmarkDocuments()[:20_000])
	info := index.Info()
	t.Logf("rows=%d terms=%d postings=%d roaring_payload_bytes=%d", info.Rows, info.Terms, info.Postings, info.EncodedBytes)
}
