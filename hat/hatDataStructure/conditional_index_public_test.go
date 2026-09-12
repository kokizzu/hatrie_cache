package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestConditionalFunctionalIndexPublicAPI(t *testing.T) {
	type key struct {
		Tenant string
		Kind   uint8
	}
	index, err := hatDataStructure.NewConditionalFunctionalIndex(
		func(value struct {
			Tenant string
			Kind   uint8
			Live   bool
		}) key {
			return key{Tenant: value.Tenant, Kind: value.Kind}
		},
		func(value struct {
			Tenant string
			Kind   uint8
			Live   bool
		}) bool {
			return value.Live
		},
		0,
	)
	if err != nil {
		t.Fatalf("NewConditionalFunctionalIndex() error = %v", err)
	}
	value := struct {
		Tenant string
		Kind   uint8
		Live   bool
	}{Tenant: "west", Kind: 1, Live: true}
	if err := index.Upsert(7, value); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if !index.Contains(key{Tenant: "west", Kind: 1}, 7) {
		t.Fatal("Contains() = false, want true")
	}
}
