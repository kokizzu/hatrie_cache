package hatAuth_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	hatAuth "hatrie_cache/hat/hatAuth"
)

func TestResourceRegistryScopesRedactsAndRotatesConnections(t *testing.T) {
	registry := newMU020Registry(t)
	if _, err := registry.CreateSecret(hatAuth.SecretSpec{
		Name:    " db/password ",
		Owner:   "ops",
		Readers: []string{"orders"},
		Value:   []byte("secret-v1"),
	}); err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	connection, err := registry.CreateConnection(hatAuth.ConnectionSpec{
		Name:       " orders-db ",
		Owner:      "ops",
		Readers:    []string{"orders"},
		Driver:     "postgres",
		Endpoint:   "db.internal:5432",
		Database:   "orders",
		SecretName: "db/password",
		TLS:        true,
	})
	if err != nil {
		t.Fatalf("CreateConnection() error = %v", err)
	}
	if connection.Name != "orders-db" || connection.SecretName != "db/password" || connection.SecretVersion != 1 || !connection.TLS {
		t.Fatalf("connection metadata = %#v, want normalized redacted metadata", connection)
	}

	resolved, err := registry.ResolveConnection("orders-db", "orders")
	if err != nil {
		t.Fatalf("ResolveConnection() error = %v", err)
	}
	if string(resolved.SecretValue()) != "secret-v1" || resolved.Metadata.SecretVersion != 1 {
		t.Fatalf("resolved connection = %#v, want version 1 secret", resolved)
	}
	secretCopy := resolved.SecretValue()
	secretCopy[0] = 'X'
	resolvedAgain, err := registry.ResolveConnection("orders-db", "orders")
	if err != nil || string(resolvedAgain.SecretValue()) != "secret-v1" {
		t.Fatalf("secret copy mutated registry: %#v/%v", resolvedAgain, err)
	}

	for _, rendered := range []string{
		fmt.Sprintf("%v", resolved),
		fmt.Sprintf("%+v", resolved),
		fmt.Sprintf("%#v", resolved),
		fmt.Sprintf("%v", registry),
		fmt.Sprintf("%#v", registry),
		fmt.Sprintf("%+v", hatAuth.SecretSpec{Value: []byte("secret-v1")}),
		fmt.Sprintf("%#v", hatAuth.SecretSpec{Value: []byte("secret-v1")}),
	} {
		if strings.Contains(rendered, "secret-v1") {
			t.Fatalf("resolved connection rendering leaked secret: %q", rendered)
		}
	}
	snapshotJSON, err := json.Marshal(registry.Snapshot())
	if err != nil {
		t.Fatalf("Snapshot JSON error = %v", err)
	}
	if strings.Contains(string(snapshotJSON), "secret-v1") {
		t.Fatalf("metadata snapshot leaked secret: %s", snapshotJSON)
	}

	if _, err := registry.RotateSecret("db/password", "ops", 0, []byte("secret-v2")); !errors.Is(err, hatAuth.ErrResourceVersionConflict) {
		t.Fatalf("wrong-version RotateSecret() error = %v, want version conflict", err)
	}
	rotated, err := registry.RotateSecret("db/password", "ops", 1, []byte("secret-v2"))
	if err != nil || rotated.Version != 2 {
		t.Fatalf("RotateSecret() = %#v/%v, want version 2", rotated, err)
	}
	resolved, err = registry.ResolveConnection("orders-db", "orders")
	if err != nil || string(resolved.SecretValue()) != "secret-v2" || resolved.Metadata.SecretVersion != 2 {
		t.Fatalf("reloaded connection = %#v/%v, want rotated secret", resolved, err)
	}

	if _, err := registry.ResolveConnection("orders-db", "intruder"); !errors.Is(err, hatAuth.ErrResourceAccessDenied) {
		t.Fatalf("unauthorized ResolveConnection() error = %v, want access denied", err)
	}
	if _, err := registry.ReadSecret("db/password", "intruder"); !errors.Is(err, hatAuth.ErrResourceAccessDenied) {
		t.Fatalf("unauthorized ReadSecret() error = %v, want access denied", err)
	}
	if _, err := registry.RotateSecret("db/password", "orders", 2, []byte("secret-v3")); !errors.Is(err, hatAuth.ErrResourceAccessDenied) {
		t.Fatalf("reader RotateSecret() error = %v, want access denied", err)
	}
}

func TestResourceRegistryRejectsInvalidInputAndProtectsLifecycle(t *testing.T) {
	registry, err := hatAuth.NewResourceRegistry(hatAuth.ResourceRegistryOptions{
		MaxSecrets:        1,
		MaxConnections:    1,
		MaxSecretBytes:    4,
		MaxNameBytes:      8,
		MaxPrincipalBytes: 8,
		MaxReaders:        2,
	})
	if err != nil {
		t.Fatalf("NewResourceRegistry() error = %v", err)
	}
	invalid := []hatAuth.SecretSpec{
		{Name: "", Owner: "ops", Value: []byte("x")},
		{Name: "secret", Owner: "", Value: []byte("x")},
		{Name: "secret", Owner: "ops", Readers: []string{"ops", "ops"}, Value: []byte("x")},
		{Name: "secret", Owner: "ops", Value: []byte("12345")},
		{Name: "secret\x00x", Owner: "ops", Value: []byte("x")},
		{Name: "secret\nlog", Owner: "ops", Value: []byte("x")},
	}
	for index, spec := range invalid {
		if _, err := registry.CreateSecret(spec); !errors.Is(err, hatAuth.ErrResourceInvalid) {
			t.Fatalf("invalid secret %d error = %v, want invalid", index, err)
		}
	}
	if _, err := registry.CreateSecret(hatAuth.SecretSpec{Name: "secret", Owner: "ops", Value: []byte("abc")}); err != nil {
		t.Fatalf("valid CreateSecret() error = %v", err)
	}
	if _, err := registry.CreateSecret(hatAuth.SecretSpec{Name: "other", Owner: "ops", Value: []byte("abc")}); !errors.Is(err, hatAuth.ErrResourceLimit) {
		t.Fatalf("secret limit error = %v, want resource limit", err)
	}
	if _, err := hatAuth.NewResourceRegistry(hatAuth.ResourceRegistryOptions{MaxSecrets: -1}); !errors.Is(err, hatAuth.ErrResourceInvalid) {
		t.Fatalf("negative option error = %v, want invalid", err)
	}
	if _, err := registry.CreateConnection(hatAuth.ConnectionSpec{
		Name: "conn", Owner: "other", Driver: "postgres", Endpoint: "db", SecretName: "secret",
	}); !errors.Is(err, hatAuth.ErrResourceAccessDenied) {
		t.Fatalf("connection secret-scope error = %v, want access denied", err)
	}
	if _, err := registry.CreateConnection(hatAuth.ConnectionSpec{
		Name: "conn", Owner: "ops", Readers: []string{"intruder"}, Driver: "postgres", Endpoint: "db", SecretName: "secret",
	}); !errors.Is(err, hatAuth.ErrResourceAccessDenied) {
		t.Fatalf("connection reader-scope error = %v, want access denied", err)
	}
	if _, err := registry.CreateConnection(hatAuth.ConnectionSpec{
		Name: "conn", Owner: "ops", Driver: "postgres", Endpoint: "postgres://u:password@db", SecretName: "secret",
	}); !errors.Is(err, hatAuth.ErrResourceInvalid) {
		t.Fatalf("credential-bearing URL error = %v, want invalid", err)
	}
	if _, err := registry.CreateConnection(hatAuth.ConnectionSpec{
		Name: "conn", Owner: "ops", Driver: "postgres", Endpoint: "db?password=secret", SecretName: "secret",
	}); !errors.Is(err, hatAuth.ErrResourceInvalid) {
		t.Fatalf("credential-bearing endpoint error = %v, want invalid", err)
	}
	if _, err := registry.CreateConnection(hatAuth.ConnectionSpec{
		Name: "conn", Owner: "ops", Driver: "postgres", Endpoint: "db", SecretName: "secret",
	}); err != nil {
		t.Fatalf("valid CreateConnection() error = %v", err)
	}
	if _, err := registry.CreateConnection(hatAuth.ConnectionSpec{
		Name: "other", Owner: "ops", Driver: "postgres", Endpoint: "db", SecretName: "secret",
	}); !errors.Is(err, hatAuth.ErrResourceLimit) {
		t.Fatalf("connection limit error = %v, want resource limit", err)
	}
	if err := registry.DeleteSecret("secret", "ops", 1); !errors.Is(err, hatAuth.ErrResourceInUse) {
		t.Fatalf("in-use DeleteSecret() error = %v, want resource in use", err)
	}
	if err := registry.DeleteConnection("conn", "ops", 0); !errors.Is(err, hatAuth.ErrResourceVersionConflict) {
		t.Fatalf("wrong-version DeleteConnection() error = %v, want version conflict", err)
	}
	if err := registry.DeleteConnection("conn", "ops", 1); err != nil {
		t.Fatalf("DeleteConnection() error = %v", err)
	}
	if err := registry.DeleteSecret("secret", "ops", 1); err != nil {
		t.Fatalf("DeleteSecret() error = %v", err)
	}
}

func TestResourceRegistrySnapshotsAreMetadataOnlyAndIndependent(t *testing.T) {
	registry := newMU020Registry(t)
	if _, err := registry.CreateSecret(hatAuth.SecretSpec{Name: "secret", Owner: "ops", Readers: []string{"reader"}, Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.CreateConnection(hatAuth.ConnectionSpec{Name: "conn", Owner: "ops", Readers: []string{"reader"}, Driver: "postgres", Endpoint: "db", SecretName: "secret"}); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot()
	if len(snapshot.Secrets) != 1 || len(snapshot.Connections) != 1 || snapshot.Secrets[0].Version != 1 || snapshot.Connections[0].SecretVersion != 1 {
		t.Fatalf("Snapshot() = %#v, want metadata for both resources", snapshot)
	}
	snapshot.Secrets[0].Readers[0] = "mutated"
	snapshot.Connections[0].Readers[0] = "mutated"
	fresh := registry.Snapshot()
	if fresh.Secrets[0].Readers[0] != "reader" || fresh.Connections[0].Readers[0] != "reader" {
		t.Fatalf("snapshot mutation changed registry: %#v", fresh)
	}
}

func newMU020Registry(t *testing.T) *hatAuth.ResourceRegistry {
	t.Helper()
	registry, err := hatAuth.NewResourceRegistry(hatAuth.ResourceRegistryOptions{})
	if err != nil {
		t.Fatalf("NewResourceRegistry() error = %v", err)
	}
	return registry
}
