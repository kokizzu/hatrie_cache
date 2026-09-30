package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestLogicalCompactionRecordsIntoMatchesRecordsAndReusesDestination(t *testing.T) {
	compaction := NewLogicalCompaction[string]()
	for _, update := range []struct {
		data string
		time uint64
		diff int64
	}{{"late", 30, 1}, {"early", 10, 2}, {"middle", 20, -1}} {
		if err := compaction.Add(update.data, update.time, update.diff); err != nil {
			t.Fatal(err)
		}
	}

	destination := make([]DifferentialRecord[string], 0, compaction.Len())
	backing := &destination[:cap(destination)][0]
	got := compaction.RecordsInto(destination)
	if want := compaction.Records(); !reflect.DeepEqual(got, want) {
		t.Fatalf("RecordsInto() = %#v, want %#v", got, want)
	}
	if &got[:cap(got)][0] != backing {
		t.Fatal("RecordsInto() did not reuse the destination backing array")
	}

	if err := compaction.Add("latest", 40, 1); err != nil {
		t.Fatal(err)
	}
	got = compaction.RecordsInto(got)
	if want := compaction.Records(); !reflect.DeepEqual(got, want) {
		t.Fatalf("RecordsInto() after mutation = %#v, want %#v", got, want)
	}

	empty := NewLogicalCompaction[string]()
	stale := make([]DifferentialRecord[string], 1, 4)
	got = empty.RecordsInto(stale)
	if len(got) != 0 || cap(got) != cap(stale) {
		t.Fatalf("empty RecordsInto() = len %d cap %d, want len 0 cap %d", len(got), cap(got), cap(stale))
	}
}
