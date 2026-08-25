package hatJournal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestListSegmentsReturnsOrderedValidatedSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	dir := SegmentDirectory(path)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, bounds := range [][2]uint64{{11, 20}, {1, 10}} {
		if err := os.WriteFile(SegmentPath(path, bounds[0], bounds[1]), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ListSegments(path)
	if err != nil {
		t.Fatalf("ListSegments() error = %v", err)
	}
	want := []Segment{
		{Path: SegmentPath(path, 1, 10), Start: 1, End: 10},
		{Path: SegmentPath(path, 11, 20), Start: 11, End: 20},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListSegments() = %#v, want %#v", got, want)
	}
}
