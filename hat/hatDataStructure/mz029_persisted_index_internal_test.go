package hatDataStructure

import "testing"

func TestMZ029PersistedIndexLoadsFromSidecar(t *testing.T) {
	directory := t.TempDir()
	options := SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
		MaxDiskBytes:     1 << 20,
	}
	source, err := NewSpillableArrangement(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Set("key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := source.Flush(); err != nil {
		t.Fatal(err)
	}
	path := source.SpillPath()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSpillableArrangement(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.persistedIndex {
		t.Fatal("reopen fell back to record scan")
	}
}
