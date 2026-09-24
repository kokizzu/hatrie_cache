package hatDataStructure_test

import (
	"fmt"
	"os"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestMZ029PersistedIndexIsWrittenAndReopened(t *testing.T) {
	directory := t.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
		MaxDiskBytes:     16 << 20,
	}
	source, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		t.Fatalf("NewSpillableArrangement() error = %v", err)
	}
	for index := 0; index < 32; index++ {
		if err := source.Set(fmt.Sprintf("key-%02d", index), []byte("value")); err != nil {
			t.Fatalf("Set(%d) error = %v", index, err)
		}
	}
	if err := source.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	path := source.SpillPath()
	indexPath := path + ".idx"
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("persisted index stat error = %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("Close(source) error = %v", err)
	}
	reopened, err := hatDataStructure.OpenSpillableArrangement(path, options)
	if err != nil {
		t.Fatalf("OpenSpillableArrangement() error = %v", err)
	}
	defer reopened.Close()
	if value, found, err := reopened.Get("key-17"); err != nil || !found || string(value) != "value" {
		t.Fatalf("Get(key-17) = %q/%t/%v, want value/true/nil", value, found, err)
	}
}

func BenchmarkMZ029ReopenSpillableArrangement(b *testing.B) {
	directory := b.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
		MaxDiskBytes:     64 << 20,
	}
	source, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		if err := source.Set(fmt.Sprintf("key-%04d", index), []byte("value-payload")); err != nil {
			b.Fatal(err)
		}
	}
	if err := source.Flush(); err != nil {
		b.Fatal(err)
	}
	path := source.SpillPath()
	if err := source.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		arrangement, err := hatDataStructure.OpenSpillableArrangement(path, options)
		if err != nil {
			b.Fatal(err)
		}
		value, found, err := arrangement.Get("key-2048")
		if err != nil || !found || string(value) != "value-payload" {
			b.Fatalf("Get(key-2048) = %q/%t/%v", value, found, err)
		}
		if err := arrangement.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
