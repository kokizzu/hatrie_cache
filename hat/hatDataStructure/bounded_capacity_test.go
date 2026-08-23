package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestBoundedCapacityValidationIsUsableByImporters(t *testing.T) {
	if err := hatDataStructure.ValidateTopKCapacity(1); err != nil {
		t.Fatalf("ValidateTopKCapacity(1) error = %v", err)
	}
	if err := hatDataStructure.ValidateReservoirSampleCapacity(1); err != nil {
		t.Fatalf("ValidateReservoirSampleCapacity(1) error = %v", err)
	}
	if err := hatDataStructure.ValidateTopKCapacity(0); err == nil {
		t.Fatal("ValidateTopKCapacity(0) succeeded")
	}
	if err := hatDataStructure.ValidateReservoirSampleCapacity(hatDataStructure.MaxReservoirSampleCapacity + 1); err == nil {
		t.Fatal("ValidateReservoirSampleCapacity(over-max) succeeded")
	}
}
