package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestXorFilterExpectedItemsValidationIsUsableByImporters(t *testing.T) {
	if err := hatDataStructure.ValidateXorFilterExpectedItems(1); err != nil {
		t.Fatalf("ValidateXorFilterExpectedItems(1) error = %v", err)
	}
	if err := hatDataStructure.ValidateXorFilterExpectedItems(0); err == nil {
		t.Fatal("ValidateXorFilterExpectedItems(0) succeeded")
	}
}
