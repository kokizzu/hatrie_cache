package hatMerkle_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatMerkle"
)

func TestCH020SharedPartRegistryRetainsMetadataOnlyReferencesAndLeaseSafety(t *testing.T) {
	registry, err := hatMerkle.NewSharedPartRegistry(hatMerkle.SharedPartRegistryOptions{MaxEntries: 1, MaxLeases: 2})
	if err != nil {
		t.Fatalf("NewSharedPartRegistry() error = %v", err)
	}
	descriptor := ch020SharedPartDescriptor()
	verifyCalls := 0
	registered, err := registry.Register(descriptor, func(candidate hatMerkle.SharedPartDescriptor) error {
		verifyCalls++
		if candidate.Entry.Location != descriptor.Entry.Location {
			t.Fatalf("verifier location = %q, want %q", candidate.Entry.Location, descriptor.Entry.Location)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if verifyCalls != 1 || registered.RegistrationGeneration != 1 {
		t.Fatalf("registered = %#v, verifier calls = %d", registered, verifyCalls)
	}

	descriptor.Entry.Manifest.Columns[0].Name = "mutated-after-register"
	first, err := registry.Acquire("share-1")
	if err != nil {
		t.Fatalf("first Acquire() error = %v", err)
	}
	second, err := registry.Acquire("share-1")
	if err != nil {
		t.Fatalf("second Acquire() error = %v", err)
	}
	if first.Reference.Entry.Manifest.Columns[0].Name != "payload" {
		t.Fatalf("stored metadata was aliased: %#v", first.Reference)
	}
	if _, err := registry.Acquire("share-1"); !errors.Is(err, hatMerkle.ErrSharedPartRegistryLeaseCapacity) {
		t.Fatalf("third Acquire() error = %v, want lease capacity", err)
	}

	if _, err := registry.Retire("share-1"); err != nil {
		t.Fatalf("Retire() error = %v", err)
	}
	if _, err := registry.Acquire("share-1"); !errors.Is(err, hatMerkle.ErrSharedPartRegistryRetired) {
		t.Fatalf("Acquire(retired) error = %v", err)
	}
	if err := registry.Remove("share-1"); !errors.Is(err, hatMerkle.ErrSharedPartRegistryLeased) {
		t.Fatalf("Remove(leased) error = %v", err)
	}
	if err := registry.Release(first.ID); err != nil {
		t.Fatalf("Release(first) error = %v", err)
	}
	if err := registry.Remove("share-1"); !errors.Is(err, hatMerkle.ErrSharedPartRegistryLeased) {
		t.Fatalf("Remove(one lease) error = %v", err)
	}
	if err := registry.Release(second.ID); err != nil {
		t.Fatalf("Release(second) error = %v", err)
	}
	if err := registry.Release(second.ID); !errors.Is(err, hatMerkle.ErrSharedPartRegistryLeaseNotFound) {
		t.Fatalf("duplicate Release() error = %v", err)
	}
	if err := registry.Remove("share-1"); err != nil {
		t.Fatalf("Remove(unleased) error = %v", err)
	}
	if snapshot := registry.Snapshot(); len(snapshot.Entries) != 0 || snapshot.ActiveLeases != 0 {
		t.Fatalf("final snapshot = %#v", snapshot)
	}
}

func TestCH020SharedPartRegistryIsIdempotentAndRejectsConflicts(t *testing.T) {
	registry, err := hatMerkle.NewSharedPartRegistry(hatMerkle.SharedPartRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSharedPartRegistry() error = %v", err)
	}
	descriptor := ch020SharedPartDescriptor()
	verifier := func(hatMerkle.SharedPartDescriptor) error { return nil }
	first, err := registry.Register(descriptor, verifier)
	if err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	second, err := registry.Register(descriptor, verifier)
	if err != nil {
		t.Fatalf("idempotent Register() error = %v", err)
	}
	if second.RegistrationGeneration != first.RegistrationGeneration || second.ShareID != first.ShareID || !second.Entry.Manifest.Equal(first.Entry.Manifest) || len(registry.Snapshot().Entries) != 1 {
		t.Fatalf("idempotent registration = %#v/%#v, snapshot = %#v", first, second, registry.Snapshot())
	}

	conflict := descriptor
	conflict.Entry.Location = "shared://node-a/parts/other"
	if _, err := registry.Register(conflict, verifier); !errors.Is(err, hatMerkle.ErrSharedPartRegistryConflict) {
		t.Fatalf("conflicting Register() error = %v", err)
	}
	if _, err := registry.Register(descriptor, nil); !errors.Is(err, hatMerkle.ErrSharedPartRegistryVerifierRequired) {
		t.Fatalf("nil verifier error = %v", err)
	}
	if _, err := registry.Register(descriptor, func(hatMerkle.SharedPartDescriptor) error { return errors.New("untrusted") }); err == nil {
		t.Fatal("verifier failure was accepted")
	}
}

func TestCH020SharedPartRegistryValidatesBoundsAndDescriptors(t *testing.T) {
	if _, err := hatMerkle.NewSharedPartRegistry(hatMerkle.SharedPartRegistryOptions{MaxEntries: -1}); !errors.Is(err, hatMerkle.ErrSharedPartRegistryOptionsInvalid) {
		t.Fatalf("invalid entry bound error = %v", err)
	}
	if _, err := hatMerkle.NewSharedPartRegistry(hatMerkle.SharedPartRegistryOptions{MaxLeases: -1}); !errors.Is(err, hatMerkle.ErrSharedPartRegistryOptionsInvalid) {
		t.Fatalf("invalid lease bound error = %v", err)
	}
	registry, err := hatMerkle.NewSharedPartRegistry(hatMerkle.SharedPartRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSharedPartRegistry() error = %v", err)
	}
	invalid := ch020SharedPartDescriptor()
	invalid.ShareID = ""
	if _, err := registry.Register(invalid, func(hatMerkle.SharedPartDescriptor) error { return nil }); !errors.Is(err, hatMerkle.ErrSharedPartRegistryInvalid) {
		t.Fatalf("invalid descriptor error = %v", err)
	}
}

func ch020SharedPartDescriptor() hatMerkle.SharedPartDescriptor {
	return hatMerkle.SharedPartDescriptor{
		ShareID:       "share-1",
		SourceReplica: "node-a",
		Entry: hatMerkle.PartCatalogEntry{
			Name:     "part-0001",
			Location: "shared://node-a/parts/part-0001",
			Manifest: hatMerkle.PartManifest{
				Checksum: hatMerkle.ChecksumPart([]byte("immutable-payload")),
				Columns: []hatMerkle.PartColumnChecksum{{
					Name:     "payload",
					Offset:   0,
					Size:     17,
					Checksum: hatMerkle.ChecksumPart([]byte("immutable-payload")),
				}},
			},
		},
	}
}
