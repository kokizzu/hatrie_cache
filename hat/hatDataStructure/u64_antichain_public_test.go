package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestU64AntichainPublicAPI(t *testing.T) {
	antichain, err := hatDataStructure.NewU64AntichainWithOptions(hatDataStructure.U64AntichainOptions{Dimensions: 2, MaxEntries: 8})
	if err != nil {
		t.Fatalf("NewU64AntichainWithOptions() error = %v", err)
	}
	if changed, err := antichain.Add([]uint64{10, 20}); err != nil || !changed {
		t.Fatalf("Add() = (%v, %v), want (true, nil)", changed, err)
	}
	covered, err := antichain.Covers([]uint64{11, 20})
	if err != nil || !covered {
		t.Fatalf("Covers() = (%v, %v), want (true, nil)", covered, err)
	}
}
