package hatStorage

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
)

func newCH024RemotePartReference(t testing.TB, name string, size uint64) RemotePartReference {
	t.Helper()
	reference, err := NewRemotePartReference(
		"s3://bucket/parts/"+name,
		"parts/"+name+".json",
		"sha256:"+name,
		size,
	)
	if err != nil {
		t.Fatalf("NewRemotePartReference(%q) error = %v", name, err)
	}
	return reference
}

func TestCH024DetachAndVerifiedReplacementAreGenerationFenced(t *testing.T) {
	catalog := NewRemotePartAttachmentCatalog()
	original := newCH024RemotePartReference(t, "original", 100)
	replacement := newCH024RemotePartReference(t, "replacement", 120)
	ctx := context.Background()
	verifyCalls := 0
	verify := func(_ context.Context, reference RemotePartReference) error {
		verifyCalls++
		if reference.SizeBytes() == 0 {
			t.Fatal("verifier received empty reference")
		}
		return nil
	}

	attached, err := catalog.Attach(ctx, " part-1 ", original, verify)
	if err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	if attached.Key != "part-1" || attached.Quarantined || attached.Generation != 1 || attached.Reference.ObjectURI() != original.ObjectURI() {
		t.Fatalf("attached = %#v", attached)
	}
	if verifyCalls != 1 {
		t.Fatalf("initial verifier calls = %d, want 1", verifyCalls)
	}

	quarantined, err := catalog.Detach(ctx, "part-1", attached.Generation, "checksum mismatch")
	if err != nil {
		t.Fatalf("Detach() error = %v", err)
	}
	if !quarantined.Quarantined || quarantined.Reason != "checksum mismatch" || quarantined.Generation != 2 {
		t.Fatalf("quarantined = %#v", quarantined)
	}

	stale, err := catalog.AttachReplacement(ctx, "part-1", attached.Generation, replacement, verify)
	if !errors.Is(err, ErrRemotePartAttachmentStale) || stale != (RemotePartAttachment{}) {
		t.Fatalf("stale replacement = %#v, err = %v", stale, err)
	}
	before, ok := catalog.Lookup("part-1")
	if !ok || !reflect.DeepEqual(before, quarantined) {
		t.Fatalf("stale replacement changed state: %#v, want %#v", before, quarantined)
	}

	active, err := catalog.AttachReplacement(ctx, "part-1", quarantined.Generation, replacement, verify)
	if err != nil {
		t.Fatalf("AttachReplacement() error = %v", err)
	}
	if active.Quarantined || active.Reason != "" || active.Generation != 3 || active.Reference.ObjectURI() != replacement.ObjectURI() {
		t.Fatalf("active replacement = %#v", active)
	}
	if verifyCalls != 2 {
		t.Fatalf("replacement verifier calls = %d, want 2", verifyCalls)
	}
	got, ok := catalog.Lookup("part-1")
	if !ok || !reflect.DeepEqual(got, active) {
		t.Fatalf("Lookup() = %#v, want %#v", got, active)
	}
}

func TestCH024VerificationFailureDoesNotPublishReplacement(t *testing.T) {
	catalog := NewRemotePartAttachmentCatalog()
	original := newCH024RemotePartReference(t, "original", 100)
	replacement := newCH024RemotePartReference(t, "replacement", 120)
	ctx := context.Background()
	if _, err := catalog.Attach(ctx, "part-1", original, func(context.Context, RemotePartReference) error { return nil }); err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	quarantined, err := catalog.Detach(ctx, "part-1", 1, "operator quarantine")
	if err != nil {
		t.Fatalf("Detach() error = %v", err)
	}
	wantErr := errors.New("replacement is unreadable")
	if _, err := catalog.AttachReplacement(ctx, "part-1", quarantined.Generation, replacement, func(context.Context, RemotePartReference) error {
		return wantErr
	}); !errors.Is(err, ErrRemotePartAttachmentVerificationFailed) || !errors.Is(err, wantErr) {
		t.Fatalf("verification error = %v", err)
	}
	got, ok := catalog.Lookup("part-1")
	if !ok || !reflect.DeepEqual(got, quarantined) {
		t.Fatalf("failed replacement changed state: %#v, want %#v", got, quarantined)
	}
}

func TestCH024AttachDetachValidationAndIdempotency(t *testing.T) {
	catalog := NewRemotePartAttachmentCatalog()
	reference := newCH024RemotePartReference(t, "one", 10)
	verify := func(context.Context, RemotePartReference) error { return nil }

	if _, err := catalog.Attach(nil, "part", reference, verify); !errors.Is(err, ErrRemotePartAttachmentContextRequired) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := catalog.Attach(context.Background(), "part", reference, nil); !errors.Is(err, ErrRemotePartAttachmentVerifierRequired) {
		t.Fatalf("nil verifier error = %v", err)
	}
	if _, err := catalog.Attach(context.Background(), "", reference, verify); !errors.Is(err, ErrRemotePartAttachmentInvalid) {
		t.Fatalf("empty key error = %v", err)
	}
	if _, err := catalog.Detach(context.Background(), "missing", 0, "operator request"); !errors.Is(err, ErrRemotePartAttachmentNotFound) {
		t.Fatalf("missing detach error = %v", err)
	}

	attached, err := catalog.Attach(context.Background(), "part", reference, verify)
	if err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	if _, err := catalog.Attach(context.Background(), "part", reference, verify); !errors.Is(err, ErrRemotePartAttachmentAlreadyAttached) {
		t.Fatalf("duplicate attach error = %v", err)
	}
	first, err := catalog.Detach(context.Background(), "part", attached.Generation, "operator request")
	if err != nil {
		t.Fatalf("first Detach() error = %v", err)
	}
	second, err := catalog.Detach(context.Background(), "part", first.Generation, "operator request")
	if err != nil || !reflect.DeepEqual(second, first) {
		t.Fatalf("idempotent Detach() = %#v, err = %v; want %#v", second, err, first)
	}
	if _, err := catalog.Attach(context.Background(), "part", reference, verify); !errors.Is(err, ErrRemotePartAttachmentQuarantined) {
		t.Fatalf("quarantined attach error = %v", err)
	}
}

func TestCH024QuarantinedSnapshotIsSortedAndDetached(t *testing.T) {
	catalog := NewRemotePartAttachmentCatalog()
	verify := func(context.Context, RemotePartReference) error { return nil }
	for _, name := range []string{"beta", "alpha", "gamma"} {
		if _, err := catalog.Attach(context.Background(), name, newCH024RemotePartReference(t, name, 10), verify); err != nil {
			t.Fatalf("Attach(%q) error = %v", name, err)
		}
	}
	for _, name := range []string{"gamma", "alpha"} {
		entry, ok := catalog.Lookup(name)
		if !ok {
			t.Fatalf("Lookup(%q) missing", name)
		}
		if _, err := catalog.Detach(context.Background(), name, entry.Generation, "maintenance"); err != nil {
			t.Fatalf("Detach(%q) error = %v", name, err)
		}
	}
	quarantined := catalog.Quarantined()
	keys := make([]string, 0, len(quarantined))
	for _, entry := range quarantined {
		keys = append(keys, entry.Key)
		if !entry.Quarantined {
			t.Fatalf("snapshot contains active entry: %#v", entry)
		}
	}
	if !sort.StringsAreSorted(keys) || !reflect.DeepEqual(keys, []string{"alpha", "gamma"}) {
		t.Fatalf("quarantined keys = %v", keys)
	}
	quarantined[0].Reason = "mutated"
	again := catalog.Quarantined()
	if again[0].Reason == "mutated" {
		t.Fatal("Quarantined() leaked mutable state")
	}
}
