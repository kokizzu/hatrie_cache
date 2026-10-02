package hatDataStructure

import (
	"errors"
	"testing"
)

func TestDefaultValueColumnSuppressesDefaultsAndPreservesUpdates(t *testing.T) {
	column, err := NewDefaultValueColumn[int64](0, 4)
	if err != nil {
		t.Fatalf("NewDefaultValueColumn() error = %v", err)
	}
	for _, value := range []int64{0, 11, 0, 0} {
		if err := column.Append(value); err != nil {
			t.Fatalf("Append(%d) error = %v", value, err)
		}
	}
	if column.Len() != 4 || column.NonDefaultCount() != 1 || column.Dense() {
		t.Fatalf("sparse layout = len %d non-defaults %d dense %t", column.Len(), column.NonDefaultCount(), column.Dense())
	}
	if value, ok := column.Get(1); !ok || value != 11 {
		t.Fatalf("Get(1) = %d/%t, want 11/true", value, ok)
	}
	if value, ok := column.Get(3); !ok || value != 0 {
		t.Fatalf("Get(3) = %d/%t, want 0/true", value, ok)
	}
	if err := column.Set(1, 0); err != nil {
		t.Fatalf("Set(default) error = %v", err)
	}
	if column.NonDefaultCount() != 0 {
		t.Fatalf("non-default count after clearing = %d, want 0", column.NonDefaultCount())
	}
	if err := column.Set(2, 7); err != nil {
		t.Fatalf("Set(non-default) error = %v", err)
	}
	values := column.Values(nil)
	if want := []int64{0, 0, 7, 0}; len(values) != len(want) || values[2] != want[2] {
		t.Fatalf("Values() = %#v, want %#v", values, want)
	}
}

func TestDefaultValueColumnUsesDenseFallbackAtHighDensity(t *testing.T) {
	column, err := NewDefaultValueColumnWithOptions(DefaultValueColumnOptions[int64]{
		Default:        0,
		Capacity:       64,
		DenseThreshold: 0.5,
	})
	if err != nil {
		t.Fatalf("NewDefaultValueColumnWithOptions() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		value := int64(0)
		if index < 32 {
			value = int64(index + 1)
		}
		if err := column.Append(value); err != nil {
			t.Fatalf("Append(%d) error = %v", value, err)
		}
	}
	if !column.Dense() {
		t.Fatal("column stayed sparse at the configured density threshold")
	}
	if column.NonDefaultCount() != 32 {
		t.Fatalf("dense non-default count = %d, want 32", column.NonDefaultCount())
	}
	if value, ok := column.Get(1); !ok || value != 2 {
		t.Fatalf("dense Get(1) = %d/%t, want 2/true", value, ok)
	}
}

func TestDefaultValueColumnRejectsInvalidOptionsAndIndexes(t *testing.T) {
	if _, err := NewDefaultValueColumnWithOptions(DefaultValueColumnOptions[int64]{DenseThreshold: 0}); err == nil {
		t.Fatal("zero dense threshold was accepted")
	}
	if _, err := NewDefaultValueColumnWithOptions(DefaultValueColumnOptions[int64]{DenseThreshold: 1.1}); err == nil {
		t.Fatal("dense threshold above one was accepted")
	}
	column, err := NewDefaultValueColumn[int64](0, 0)
	if err != nil {
		t.Fatalf("NewDefaultValueColumn() error = %v", err)
	}
	if err := column.Set(0, 1); !errors.Is(err, ErrDefaultValueColumnIndex) {
		t.Fatalf("Set(out of range) error = %v, want ErrDefaultValueColumnIndex", err)
	}
	if value, ok := column.Get(0); ok || value != 0 {
		t.Fatalf("Get(out of range) = %d/%t, want zero/false", value, ok)
	}
}

func TestDefaultValueColumnMaintainsPackedRanksAcrossWords(t *testing.T) {
	column, err := NewDefaultValueColumn[int](0, 130)
	if err != nil {
		t.Fatalf("NewDefaultValueColumn() error = %v", err)
	}
	want := make([]int, 130)
	for index := range want {
		if err := column.Append(0); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}
	for index, value := range map[int]int{1: 11, 64: 64, 129: 129} {
		if err := column.Set(index, value); err != nil {
			t.Fatalf("Set(%d, %d) error = %v", index, value, err)
		}
		want[index] = value
	}
	if err := column.Set(64, 0); err != nil {
		t.Fatalf("Set(64, default) error = %v", err)
	}
	want[64] = 0
	if err := column.Set(2, 22); err != nil {
		t.Fatalf("Set(2, 22) error = %v", err)
	}
	want[2] = 22
	got := column.Values(nil)
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("Values()[%d] = %d, want %d", index, got[index], want[index])
		}
	}
	if column.NonDefaultCount() != 3 {
		t.Fatalf("NonDefaultCount() = %d, want 3", column.NonDefaultCount())
	}
}

func TestDefaultValueColumnDenseSetMaintainsCount(t *testing.T) {
	column, err := NewDefaultValueColumnWithOptions(DefaultValueColumnOptions[int]{
		DenseThreshold: 0.5,
		Capacity:       64,
	})
	if err != nil {
		t.Fatalf("NewDefaultValueColumnWithOptions() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		value := 0
		if index < 32 {
			value = index + 1
		}
		if err := column.Append(value); err != nil {
			t.Fatalf("Append(%d) error = %v", value, err)
		}
	}
	if !column.Dense() || column.NonDefaultCount() != 32 {
		t.Fatalf("dense state = %t, non-defaults = %d; want true/32", column.Dense(), column.NonDefaultCount())
	}
	if err := column.Set(1, 0); err != nil {
		t.Fatalf("Set(default) error = %v", err)
	}
	if err := column.Set(40, 9); err != nil {
		t.Fatalf("Set(non-default) error = %v", err)
	}
	if column.NonDefaultCount() != 32 {
		t.Fatalf("NonDefaultCount() after dense replacement = %d, want 32", column.NonDefaultCount())
	}
}
