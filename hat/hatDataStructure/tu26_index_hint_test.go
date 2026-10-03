package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestTU26IndexStrategyCatalogResolvesHintsAndRanksCandidates(t *testing.T) {
	catalog, err := NewIndexStrategyCatalog(IndexStrategyCatalogOptions{Capacity: 8})
	if err != nil {
		t.Fatalf("NewIndexStrategyCatalog() error = %v", err)
	}
	descriptors := []IndexStrategyDescriptor{
		{
			Name:                 "orders_by_id",
			Kind:                 IndexStrategyHash,
			Fields:               []string{"id"},
			Unique:               true,
			Capabilities:         IndexStrategyCapabilities{Equality: true},
			EstimatedCardinality: 1_000_000,
			EstimatedBytes:       64 << 20,
		},
		{
			Name:                 "orders_by_created_at",
			Kind:                 IndexStrategyOrdered,
			Fields:               []string{"created_at"},
			Capabilities:         IndexStrategyCapabilities{Range: true, Ordered: true},
			EstimatedCardinality: 1_000_000,
			EstimatedBytes:       24 << 20,
		},
		{
			Name:                 "orders_by_created_at_hash",
			Kind:                 IndexStrategyHash,
			Fields:               []string{"created_at"},
			Capabilities:         IndexStrategyCapabilities{Equality: true},
			EstimatedCardinality: 1_000_000,
			EstimatedBytes:       48 << 20,
		},
	}
	for _, descriptor := range descriptors {
		if err := catalog.Register(descriptor); err != nil {
			t.Fatalf("register descriptor: %v", err)
		}
	}

	resolved, err := catalog.Resolve(IndexStrategyHint{Name: "orders_by_id", Field: "id", Operation: IndexOperationEquality})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.Kind != IndexStrategyHash || !resolved.Unique || !reflect.DeepEqual(resolved.Fields, []string{"id"}) {
		t.Fatalf("resolved descriptor = %#v", resolved)
	}
	resolved.Fields[0] = "mutated"
	again, err := catalog.Resolve(IndexStrategyHint{Name: "orders_by_id", Field: "id", Operation: IndexOperationEquality})
	if err != nil || again.Fields[0] != "id" {
		t.Fatalf("Resolve() did not isolate descriptor fields: %#v, %v", again, err)
	}

	if _, err := catalog.Resolve(IndexStrategyHint{Name: "orders_by_id", Field: "id", Operation: IndexOperationRange}); !errors.Is(err, ErrIndexStrategyCapabilityMismatch) {
		t.Fatalf("unsupported operation error = %v, want ErrIndexStrategyCapabilityMismatch", err)
	}
	if _, err := catalog.Resolve(IndexStrategyHint{Name: "missing", Operation: IndexOperationEquality}); !errors.Is(err, ErrIndexStrategyNotFound) {
		t.Fatalf("missing strategy error = %v, want ErrIndexStrategyNotFound", err)
	}

	candidates, err := catalog.Suggest(IndexStrategyHint{Field: "created_at", Operation: IndexOperationRange})
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if len(candidates) != 1 || candidates[0].Name != "orders_by_created_at" {
		t.Fatalf("range candidates = %#v, want ordered candidate", candidates)
	}
	scratch := make([]IndexStrategyDescriptor, 0, 2)
	reused, err := catalog.SuggestInto(scratch, IndexStrategyHint{Field: "created_at", Operation: IndexOperationRange})
	if err != nil || len(reused) != 1 || reused[0].Name != "orders_by_created_at" {
		t.Fatalf("SuggestInto() = %#v, %v", reused, err)
	}
	firstAddress := &reused[0]
	reused, err = catalog.SuggestInto(reused, IndexStrategyHint{Field: "created_at", Operation: IndexOperationRange})
	if err != nil || len(reused) != 1 || &reused[0] != firstAddress {
		t.Fatalf("SuggestInto() did not reuse candidate storage: %#v, %v", reused, err)
	}

	snapshot := catalog.Snapshot()
	if len(snapshot) != 3 || snapshot[0].Name != "orders_by_created_at" || snapshot[1].Name != "orders_by_created_at_hash" || snapshot[2].Name != "orders_by_id" {
		t.Fatalf("snapshot order = %#v", snapshot)
	}
	snapshot[0].Fields[0] = "changed"
	if catalog.Snapshot()[0].Fields[0] != "created_at" {
		t.Fatal("Snapshot() exposed mutable descriptor fields")
	}
}

func TestTU26IndexStrategyCatalogBoundsAndAtomicRegistration(t *testing.T) {
	if _, err := NewIndexStrategyCatalog(IndexStrategyCatalogOptions{Capacity: 0}); err != nil {
		t.Fatalf("zero capacity should use default: %v", err)
	}
	catalog, err := NewIndexStrategyCatalog(IndexStrategyCatalogOptions{Capacity: 1})
	if err != nil {
		t.Fatalf("NewIndexStrategyCatalog() error = %v", err)
	}
	valid := IndexStrategyDescriptor{Name: "id", Kind: IndexStrategyHash, Fields: []string{"id"}, Capabilities: IndexStrategyCapabilities{Equality: true}}
	if err := catalog.Register(valid); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := catalog.Register(valid); !errors.Is(err, ErrIndexStrategyNameExists) {
		t.Fatalf("duplicate registration error = %v, want ErrIndexStrategyNameExists", err)
	}
	if err := catalog.Register(IndexStrategyDescriptor{Name: "created_at", Kind: IndexStrategyOrdered, Fields: []string{"created_at"}, Capabilities: IndexStrategyCapabilities{Range: true}}); !errors.Is(err, ErrIndexStrategyCapacity) {
		t.Fatalf("capacity registration error = %v, want ErrIndexStrategyCapacity", err)
	}
	if catalog.Len() != 1 {
		t.Fatalf("Len() = %d, want 1 after rejected registration", catalog.Len())
	}
	if err := catalog.Register(IndexStrategyDescriptor{Name: "", Kind: IndexStrategyHash}); !errors.Is(err, ErrIndexStrategyDescriptorInvalid) {
		t.Fatalf("invalid registration error = %v, want ErrIndexStrategyDescriptorInvalid", err)
	}
}

func TestTU26IndexStrategyCatalogConcurrentReads(t *testing.T) {
	catalog := NewDefaultIndexStrategyCatalog()
	for _, name := range []string{"a", "b", "c", "d"} {
		if err := catalog.Register(IndexStrategyDescriptor{Name: name, Kind: IndexStrategyHash, Fields: []string{name}, Capabilities: IndexStrategyCapabilities{Equality: true}}); err != nil {
			t.Fatalf("Register(%q) error = %v", name, err)
		}
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				name := []string{"a", "b", "c", "d"}[(worker+iteration)%4]
				if _, err := catalog.Resolve(IndexStrategyHint{Name: name, Field: name, Operation: IndexOperationEquality}); err != nil {
					t.Errorf("Resolve(%q) error = %v", name, err)
					return
				}
				if _, err := catalog.Suggest(IndexStrategyHint{Field: name, Operation: IndexOperationEquality}); err != nil {
					t.Errorf("Suggest(%q) error = %v", name, err)
					return
				}
			}
		}(worker)
	}
	group.Wait()
}
