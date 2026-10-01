package hatAuth

import (
	"context"
	"errors"
	"testing"
)

func newTU33RoleCatalog(t *testing.T) (*RoleCatalog, RoleGrant) {
	t.Helper()
	catalog, err := NewRoleCatalog(RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("owner", RoleSpec{Name: "operators", Owner: "owner"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("owner", "alice", "operators"); err != nil {
		t.Fatal(err)
	}
	grant, err := catalog.Grant("owner", RoleGrantSpec{
		Role: "operators",
		Rule: Rule{
			Commands: []string{StoredFunctionOperationRegister, StoredFunctionOperationCall},
			Objects:  []string{"function:echo"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog, grant
}

func TestTU33RoleCatalogStoredFunctionAuthorizerUsesFunctionObjectGrants(t *testing.T) {
	catalog, grant := newTU33RoleCatalog(t)
	authorizer := RoleCatalogStoredFunctionAuthorizer(catalog)
	if !authorizer("alice", StoredFunctionOperationRegister, "echo") {
		t.Fatal("role catalog denied permitted function registration")
	}
	if !authorizer("alice", StoredFunctionOperationCall, "echo") {
		t.Fatal("role catalog denied permitted function call")
	}
	if authorizer("alice", StoredFunctionOperationCall, "other") {
		t.Fatal("role catalog permitted a different function")
	}
	if authorizer("alice", "DROP", "echo") {
		t.Fatal("role catalog permitted an unsupported operation")
	}
	if authorizer("", StoredFunctionOperationCall, "echo") {
		t.Fatal("role catalog permitted an empty principal")
	}

	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{Authorize: authorizer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register("alice", StoredFunctionSpec{
		Name:    "echo",
		Version: 1,
		Handler: func(_ context.Context, input []byte) ([]byte, error) { return input, nil },
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	output, err := registry.Call(context.Background(), "alice", "echo", 0, []byte("ok"))
	if err != nil || string(output) != "ok" {
		t.Fatalf("Call() = %q/%v, want ok/nil", output, err)
	}

	if err := catalog.RevokeGrant("owner", grant.ID, catalog.Version()); err != nil {
		t.Fatalf("RevokeGrant() error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "alice", "echo", 0, []byte("blocked")); !errors.Is(err, ErrStoredFunctionAccessDenied) {
		t.Fatalf("revoked function grant error = %v, want access denied", err)
	}
}

func TestTU33RoleCatalogStoredFunctionAuthorizerFailsClosed(t *testing.T) {
	authorizer := RoleCatalogStoredFunctionAuthorizer(nil)
	if authorizer("alice", StoredFunctionOperationCall, "echo") {
		t.Fatal("nil role catalog authorized a function")
	}
	if authorizer("alice", StoredFunctionOperationRegister, "echo") {
		t.Fatal("nil role catalog authorized registration")
	}
}
