package hatDataStructure

import (
	"errors"
	"sync"
	"testing"
)

type tu24Record struct {
	Tenant string
	Active bool
	Value  int
}

func TestTU24ConditionalIndexCatalogAdmissionAndMetadata(t *testing.T) {
	catalog := NewConditionalIndexCatalog[tu24Record, string]()
	definition := ConditionalIndexDefinition[tu24Record, string]{
		Name:          "active-by-tenant",
		ExtractorName: "tenant",
		PredicateName: "active = true",
		Extractor:     func(record tu24Record) string { return record.Tenant },
		Predicate:     func(record tu24Record) bool { return record.Active },
	}
	if err := catalog.Create(definition); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Create(definition); !errors.Is(err, ErrConditionalIndexCatalogExists) {
		t.Fatalf("duplicate create error = %v", err)
	}
	if err := catalog.Upsert("active-by-tenant", 1, tu24Record{Tenant: "acme", Active: true, Value: 10}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Upsert("active-by-tenant", 2, tu24Record{Tenant: "acme", Active: false, Value: 20}); err != nil {
		t.Fatal(err)
	}
	if got, err := catalog.LookupIDs("active-by-tenant", "acme"); err != nil || len(got) != 1 || got[0] != 1 {
		t.Fatalf("lookup = %v, %v", got, err)
	}
	metadata, err := catalog.Metadata("active-by-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.State != ConditionalIndexReady || metadata.Generation != 1 || metadata.Entries != 1 || metadata.DistinctKeys != 1 || metadata.PredicateName != "active = true" {
		t.Fatalf("metadata = %+v", metadata)
	}
	metadata.PredicateName = "caller-mutated"
	again, err := catalog.Metadata("active-by-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if again.PredicateName != "active = true" {
		t.Fatal("metadata leaked internal state")
	}
}

func TestTU24ConditionalIndexCatalogAtomicRebuild(t *testing.T) {
	catalog := NewConditionalIndexCatalog[tu24Record, string]()
	definition := ConditionalIndexDefinition[tu24Record, string]{
		Name:          "active-by-tenant",
		ExtractorName: "tenant",
		PredicateName: "active",
		Extractor:     func(record tu24Record) string { return record.Tenant },
		Predicate:     func(record tu24Record) bool { return record.Active },
	}
	if err := catalog.Create(definition); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Rebuild("active-by-tenant", []ConditionalIndexRow[tu24Record]{
		{ID: 1, Value: tu24Record{Tenant: "old", Active: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Rebuild("active-by-tenant", []ConditionalIndexRow[tu24Record]{
		{ID: 2, Value: tu24Record{Tenant: "new", Active: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := catalog.LookupIDs("active-by-tenant", "old"); err != nil || len(got) != 0 {
		t.Fatalf("old lookup after rebuild = %v, %v", got, err)
	}
	if got, err := catalog.LookupIDs("active-by-tenant", "new"); err != nil || len(got) != 1 || got[0] != 2 {
		t.Fatalf("new lookup after rebuild = %v, %v", got, err)
	}
	metadata, err := catalog.Metadata("active-by-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Generation != 3 || metadata.State != ConditionalIndexReady || metadata.Entries != 1 {
		t.Fatalf("rebuild metadata = %+v", metadata)
	}
}

func TestTU24ConditionalIndexCatalogFencesWritesDuringRebuild(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	catalog := NewConditionalIndexCatalog[tu24Record, string]()
	definition := ConditionalIndexDefinition[tu24Record, string]{
		Name:          "active-by-tenant",
		ExtractorName: "tenant",
		PredicateName: "active",
		Extractor: func(record tu24Record) string {
			once.Do(func() {
				close(started)
				<-release
			})
			return record.Tenant
		},
		Predicate: func(record tu24Record) bool { return record.Active },
	}
	if err := catalog.Create(definition); err != nil {
		t.Fatal(err)
	}
	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- catalog.Rebuild("active-by-tenant", []ConditionalIndexRow[tu24Record]{
			{ID: 1, Value: tu24Record{Tenant: "acme", Active: true}},
		})
	}()
	<-started
	metadata, err := catalog.Metadata("active-by-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.State != ConditionalIndexRebuilding {
		t.Fatalf("rebuilding metadata = %+v", metadata)
	}
	if err := catalog.Upsert("active-by-tenant", 2, tu24Record{Tenant: "acme", Active: true}); !errors.Is(err, ErrConditionalIndexCatalogRebuilding) {
		t.Fatalf("write during rebuild error = %v", err)
	}
	close(release)
	if err := <-rebuildDone; err != nil {
		t.Fatal(err)
	}
	if got, err := catalog.LookupIDs("active-by-tenant", "acme"); err != nil || len(got) != 1 || got[0] != 1 {
		t.Fatalf("post-rebuild lookup = %v, %v", got, err)
	}
}

func TestTU24ConditionalIndexCatalogValidationDropAndNil(t *testing.T) {
	catalog := NewConditionalIndexCatalog[tu24Record, string]()
	invalid := ConditionalIndexDefinition[tu24Record, string]{
		Name:      "missing-identities",
		Extractor: func(record tu24Record) string { return record.Tenant },
		Predicate: func(record tu24Record) bool { return record.Active },
	}
	if err := catalog.Create(invalid); !errors.Is(err, ErrConditionalIndexCatalogDefinition) {
		t.Fatalf("invalid definition error = %v", err)
	}
	if err := catalog.Upsert("missing", 1, tu24Record{}); !errors.Is(err, ErrConditionalIndexCatalogNotFound) {
		t.Fatalf("missing upsert error = %v", err)
	}
	if catalog.Drop("missing") {
		t.Fatal("missing drop succeeded")
	}
	definition := ConditionalIndexDefinition[tu24Record, string]{
		Name:          "drop-me",
		ExtractorName: "tenant",
		PredicateName: "active",
		Extractor:     func(record tu24Record) string { return record.Tenant },
		Predicate:     func(record tu24Record) bool { return record.Active },
	}
	if err := catalog.Create(definition); err != nil {
		t.Fatal(err)
	}
	if !catalog.Drop("drop-me") || catalog.Drop("drop-me") {
		t.Fatal("drop result mismatch")
	}
	var nilCatalog *ConditionalIndexCatalog[tu24Record, string]
	if err := nilCatalog.Create(definition); !errors.Is(err, ErrConditionalIndexCatalogNil) {
		t.Fatalf("nil create error = %v", err)
	}
	if _, err := nilCatalog.Metadata("missing"); !errors.Is(err, ErrConditionalIndexCatalogNil) {
		t.Fatalf("nil metadata error = %v", err)
	}
}
