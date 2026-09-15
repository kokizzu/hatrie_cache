package hatDataStructure

import (
	"bytes"
	"errors"
	"testing"
)

func TestHyperLogLogMergeCombinesPartitionStates(t *testing.T) {
	left, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"a", "b", "c"} {
		left.AddJSONString(value)
	}
	for _, value := range []string{"c", "d", "e"} {
		right.AddJSONString(value)
	}
	want, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"a", "b", "c", "c", "d", "e"} {
		want.AddJSONString(value)
	}

	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if left.Observations() != want.Observations() {
		t.Fatalf("merged observations = %d, want %d", left.Observations(), want.Observations())
	}
	if !bytes.Equal(left.RawRegisters(), want.RawRegisters()) {
		t.Fatal("merged registers differ from one-pass registers")
	}
	if left.Count() != want.Count() {
		t.Fatalf("merged count = %d, want %d", left.Count(), want.Count())
	}
}

func TestHyperLogLogMergeAdoptsZeroValueAndLeavesEmptyStateStable(t *testing.T) {
	part, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	part.AddJSONString("one")
	part.AddJSONString("two")

	var merged HyperLogLog
	if err := merged.Merge(part); err != nil {
		t.Fatalf("zero-value Merge() error = %v", err)
	}
	if !bytes.Equal(merged.RawRegisters(), part.RawRegisters()) || merged.Observations() != part.Observations() {
		t.Fatalf("zero-value merge = %#v, want %#v", merged.Info(), part.Info())
	}
	empty, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	before := append([]uint8(nil), merged.RawRegisters()...)
	if err := merged.Merge(empty); err != nil {
		t.Fatalf("empty Merge() error = %v", err)
	}
	if !bytes.Equal(merged.RawRegisters(), before) || merged.Observations() != part.Observations() {
		t.Fatal("merging an empty state changed the receiver")
	}
}

func TestHyperLogLogMergeRejectsPrecisionMismatchWithoutMutation(t *testing.T) {
	receiver, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	receiver.AddJSONString("stable")
	before := append([]uint8(nil), receiver.RawRegisters()...)
	other, err := NewHyperLogLog(11)
	if err != nil {
		t.Fatal(err)
	}
	other.AddJSONString("different")
	if err := receiver.Merge(other); !errors.Is(err, ErrHyperLogLogPrecisionMismatch) {
		t.Fatalf("precision mismatch error = %v, want ErrHyperLogLogPrecisionMismatch", err)
	}
	if !bytes.Equal(receiver.RawRegisters(), before) || receiver.Observations() != 1 {
		t.Fatal("precision mismatch mutated the receiver")
	}
}

func TestHyperLogLogMergeRejectsInvalidStateWithoutMutation(t *testing.T) {
	receiver, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatal(err)
	}
	receiver.AddJSONString("stable")
	before := append([]uint8(nil), receiver.RawRegisters()...)
	invalid := HyperLogLog{
		precision:    10,
		observations: 1,
		registers:    make([]uint8, hyperLogLogRegisterCount(10)),
	}
	invalid.registers[0] = hyperLogLogMaxRank(10) + 1
	if err := receiver.Merge(invalid); !errors.Is(err, ErrHyperLogLogStateInvalid) {
		t.Fatalf("invalid state error = %v, want ErrHyperLogLogStateInvalid", err)
	}
	if !bytes.Equal(receiver.RawRegisters(), before) || receiver.Observations() != 1 {
		t.Fatal("invalid state mutated the receiver")
	}
}

func TestHyperLogLogMergeNilReceiver(t *testing.T) {
	var receiver *HyperLogLog
	if err := receiver.Merge(NewDefaultHyperLogLog()); !errors.Is(err, ErrHyperLogLogNil) {
		t.Fatalf("nil receiver error = %v, want ErrHyperLogLogNil", err)
	}
}
