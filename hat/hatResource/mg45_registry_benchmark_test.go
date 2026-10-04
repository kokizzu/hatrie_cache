package hatResource_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatResource"
)

var mg45BenchmarkSecret hatResource.ResolvedSecret
var mg45BenchmarkConnection hatResource.Connection

func BenchmarkMG45RegistryCreateAndRegister(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := registry.PutSecret(hatResource.SecretSpec{
			Ref:   hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"},
			Owner: "tenant-a",
			Value: "password-a",
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMG45RegistryResolveSecret(b *testing.B) {
	registry := mustMG45Registry(b)
	ref := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
	b.ReportAllocs()
	for b.Loop() {
		secret, err := registry.ResolveSecret(ref, "tenant-a")
		if err != nil {
			b.Fatal(err)
		}
		mg45BenchmarkSecret = secret
	}
}

func BenchmarkMG45RegistryVerifySecret(b *testing.B) {
	registry := mustMG45Registry(b)
	ref := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
	now := time.Unix(100, 0).UTC()
	b.ReportAllocs()
	for b.Loop() {
		verified, err := registry.VerifySecret(ref, "tenant-a", "password-a", now)
		if err != nil || !verified {
			b.Fatalf("VerifySecret() = %v, %v", verified, err)
		}
	}
}

func BenchmarkMG45RegistryResolveConnection(b *testing.B) {
	registry := mustMG45Registry(b)
	ref := hatResource.ResourceRef{Namespace: "tenant-a", Name: "primary"}
	b.ReportAllocs()
	for b.Loop() {
		connection, err := registry.ResolveConnection(ref, "tenant-a")
		if err != nil {
			b.Fatal(err)
		}
		mg45BenchmarkConnection = connection
	}
}

func mustMG45Registry(b testing.TB) *hatResource.Registry {
	b.Helper()
	registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	secretRef := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: secretRef, Owner: "tenant-a", Value: "password-a"}); err != nil {
		b.Fatal(err)
	}
	if _, err := registry.PutConnection(hatResource.ConnectionSpec{
		Ref:      hatResource.ResourceRef{Namespace: "tenant-a", Name: "primary"},
		Owner:    "tenant-a",
		Driver:   "postgres",
		Endpoint: "db.internal:5432",
		Secret:   secretRef,
	}); err != nil {
		b.Fatal(err)
	}
	return registry
}
