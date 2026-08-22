package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestHyperLogLogIsUsableByImporters(t *testing.T) {
	hll, err := hatDataStructure.NewHyperLogLog(10)
	if err != nil {
		t.Fatalf("NewHyperLogLog() error = %v", err)
	}
	if got := hll.AddBytes([]byte("alpha"), []byte("beta")); got == 0 {
		t.Fatal("AddBytes() changed no register")
	}
	if !hll.AddJSONString("gamma") {
		t.Fatal("AddJSONString() = false, want a changed register")
	}
	if info := hll.Info(); info.Observations != 3 || info.RegisterCount != 1024 || info.Estimate == 0 {
		t.Fatalf("Info() = %#v, want three observations", info)
	}
	restored, err := hatDataStructure.NewHyperLogLogFromSnapshot(hll.Snapshot())
	if err != nil {
		t.Fatalf("NewHyperLogLogFromSnapshot() error = %v", err)
	}
	if got, want := restored.Count(), hll.Count(); got != want {
		t.Fatalf("restored Count() = %d, want %d", got, want)
	}
}
