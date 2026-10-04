package hatResource_test

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatResource"
)

func TestMG45SecretRegistryScopesAndRedacts(t *testing.T) {
	registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	created, err := registry.PutSecret(hatResource.SecretSpec{
		Ref:   hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"},
		Owner: "tenant-a",
		Value: "password-a",
	})
	if err != nil {
		t.Fatalf("PutSecret() error = %v", err)
	}
	if created.Version != 1 || created.Owner != "tenant-a" {
		t.Fatalf("created metadata = %#v", created)
	}

	resolved, err := registry.ResolveSecret(hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}, "tenant-a")
	if err != nil {
		t.Fatalf("ResolveSecret() error = %v", err)
	}
	if resolved.Value != "password-a" || resolved.Version != 1 {
		t.Fatalf("resolved secret = %#v", resolved)
	}
	resolvedJSON, err := json.Marshal(resolved)
	if err != nil {
		t.Fatalf("Marshal(resolved) error = %v", err)
	}
	if strings.Contains(string(resolvedJSON), "password-a") {
		t.Fatalf("resolved secret JSON contains secret value: %s", resolvedJSON)
	}
	if _, err := registry.ResolveSecret(hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}, "tenant-b"); !errors.Is(err, hatResource.ErrResourceForbidden) {
		t.Fatalf("cross-owner ResolveSecret() error = %v, want ErrResourceForbidden", err)
	}

	snapshot := registry.Snapshot()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("Marshal(snapshot) error = %v", err)
	}
	if strings.Contains(string(encoded), "password-a") {
		t.Fatalf("snapshot contains secret value: %s", encoded)
	}
	if len(snapshot.Secrets) != 1 || snapshot.Secrets[0].Ref.Name != "database" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestMG45SecretRotationGraceAndExpiry(t *testing.T) {
	registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	ref := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: ref, Owner: "tenant-a", Value: "old"}); err != nil {
		t.Fatalf("PutSecret() error = %v", err)
	}
	now := time.Unix(100, 0).UTC()
	rotated, err := registry.RotateSecret(ref, "tenant-a", "new", now, 10*time.Second)
	if err != nil {
		t.Fatalf("RotateSecret() error = %v", err)
	}
	if rotated.Version != 2 || rotated.PreviousVersion != 1 || !rotated.PreviousExpiresAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("rotated metadata = %#v", rotated)
	}
	for _, test := range []struct {
		name      string
		candidate string
		at        time.Time
		want      bool
	}{
		{name: "new", candidate: "new", at: now, want: true},
		{name: "old during grace", candidate: "old", at: now.Add(9 * time.Second), want: true},
		{name: "old at expiry", candidate: "old", at: now.Add(10 * time.Second), want: false},
		{name: "wrong", candidate: "wrong", at: now, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := registry.VerifySecret(ref, "tenant-a", test.candidate, test.at)
			if err != nil {
				t.Fatalf("VerifySecret() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("VerifySecret() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestMG45ConnectionReferencesOwnedSecret(t *testing.T) {
	registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	secretRef := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: secretRef, Owner: "tenant-a", Value: "password-a"}); err != nil {
		t.Fatalf("PutSecret() error = %v", err)
	}
	connectionRef := hatResource.ResourceRef{Namespace: "tenant-a", Name: "primary"}
	if _, err := registry.PutConnection(hatResource.ConnectionSpec{
		Ref:      connectionRef,
		Owner:    "tenant-a",
		Driver:   "postgres",
		Endpoint: "db.internal:5432",
		Secret:   secretRef,
		Options:  map[string]string{"sslmode": "verify-full"},
	}); err != nil {
		t.Fatalf("PutConnection() error = %v", err)
	}

	connection, err := registry.ResolveConnection(connectionRef, "tenant-a")
	if err != nil {
		t.Fatalf("ResolveConnection() error = %v", err)
	}
	if connection.Driver != "postgres" || connection.Secret != secretRef || connection.SecretVersion != 1 || connection.Options["sslmode"] != "verify-full" {
		t.Fatalf("connection = %#v", connection)
	}
	connection.Options["sslmode"] = "disabled"
	again, err := registry.ResolveConnection(connectionRef, "tenant-a")
	if err != nil {
		t.Fatalf("second ResolveConnection() error = %v", err)
	}
	if again.Options["sslmode"] != "verify-full" {
		t.Fatal("connection options were not copied")
	}

	secret, err := registry.ResolveConnectionSecret(connectionRef, "tenant-a")
	if err != nil {
		t.Fatalf("ResolveConnectionSecret() error = %v", err)
	}
	if secret.Value != "password-a" || secret.Version != 1 {
		t.Fatalf("connection secret = %#v", secret)
	}
	if _, err := registry.ResolveConnection(connectionRef, "tenant-b"); !errors.Is(err, hatResource.ErrResourceForbidden) {
		t.Fatalf("cross-owner ResolveConnection() error = %v, want ErrResourceForbidden", err)
	}
}

func TestMG45ConnectionRejectsCrossOwnerSecret(t *testing.T) {
	registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	secretRef := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: secretRef, Owner: "tenant-a", Value: "password-a"}); err != nil {
		t.Fatalf("PutSecret() error = %v", err)
	}
	_, err = registry.PutConnection(hatResource.ConnectionSpec{
		Ref:      hatResource.ResourceRef{Namespace: "tenant-b", Name: "primary"},
		Owner:    "tenant-b",
		Driver:   "postgres",
		Endpoint: "db.internal:5432",
		Secret:   secretRef,
	})
	if !errors.Is(err, hatResource.ErrResourceForbidden) {
		t.Fatalf("cross-owner PutConnection() error = %v, want ErrResourceForbidden", err)
	}
}

func TestMG45RegistryBoundsAndValidatesInput(t *testing.T) {
	if _, err := hatResource.NewRegistry(hatResource.RegistryOptions{MaxSecrets: -1}); !errors.Is(err, hatResource.ErrInvalidOptions) {
		t.Fatalf("negative MaxSecrets error = %v, want ErrInvalidOptions", err)
	}
	registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{MaxSecrets: 1, MaxConnections: 1, MaxSecretValueBytes: 4})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: hatResource.ResourceRef{Namespace: "tenant-a", Name: "too-long"}, Owner: "tenant-a", Value: "12345"}); !errors.Is(err, hatResource.ErrSecretValueTooLarge) {
		t.Fatalf("oversized secret error = %v, want ErrSecretValueTooLarge", err)
	}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: hatResource.ResourceRef{Namespace: "tenant-a", Name: "one"}, Owner: "tenant-a", Value: "1234"}); err != nil {
		t.Fatalf("first PutSecret() error = %v", err)
	}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: hatResource.ResourceRef{Namespace: "tenant-a", Name: "two"}, Owner: "tenant-a", Value: "1234"}); !errors.Is(err, hatResource.ErrResourceLimit) {
		t.Fatalf("secret capacity error = %v, want ErrResourceLimit", err)
	}
}

func TestMG45RegistryConcurrentResolution(t *testing.T) {
	registry, err := hatResource.NewRegistry(hatResource.RegistryOptions{})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	ref := hatResource.ResourceRef{Namespace: "tenant-a", Name: "database"}
	if _, err := registry.PutSecret(hatResource.SecretSpec{Ref: ref, Owner: "tenant-a", Value: "password-a"}); err != nil {
		t.Fatalf("PutSecret() error = %v", err)
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 100; j++ {
				if _, err := registry.ResolveSecret(ref, "tenant-a"); err != nil {
					t.Errorf("ResolveSecret() error = %v", err)
					return
				}
			}
		}()
	}
	group.Wait()
}
