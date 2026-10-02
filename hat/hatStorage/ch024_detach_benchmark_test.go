package hatStorage

import (
	"context"
	"testing"
)

func BenchmarkCH024AttachmentCatalogLookup(b *testing.B) {
	catalog := NewRemotePartAttachmentCatalog()
	reference := newCH024RemotePartReference(b, "lookup", 1024)
	if _, err := catalog.Attach(context.Background(), "part-1", reference, func(context.Context, RemotePartReference) error { return nil }); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		entry, ok := catalog.Lookup("part-1")
		if !ok || entry.Quarantined || entry.Reference.ObjectURI() != reference.ObjectURI() {
			b.Fatal("catalog lookup lost active part")
		}
	}
}

func BenchmarkCH024AttachmentCatalogDetachReplace(b *testing.B) {
	catalog := NewRemotePartAttachmentCatalog()
	reference := newCH024RemotePartReference(b, "replace", 1024)
	verify := func(context.Context, RemotePartReference) error { return nil }
	if _, err := catalog.Attach(context.Background(), "part-1", reference, verify); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		entry, ok := catalog.Lookup("part-1")
		if !ok {
			b.Fatal("catalog entry disappeared")
		}
		quarantined, err := catalog.Detach(ctx, "part-1", entry.Generation, "benchmark")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := catalog.AttachReplacement(ctx, "part-1", quarantined.Generation, reference, verify); err != nil {
			b.Fatal(err)
		}
	}
}
