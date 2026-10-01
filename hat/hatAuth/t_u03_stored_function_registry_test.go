package hatAuth

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestTU03StoredFunctionRegistryAuthorizesVersionsAndCopiesPayloads(t *testing.T) {
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{
		MaxFunctions:   8,
		MaxNameBytes:   64,
		MaxInputBytes:  64,
		MaxOutputBytes: 64,
		Authorize: func(principal, operation, function string) bool {
			if operation == StoredFunctionOperationRegister {
				return principal == "admin"
			}
			return operation == StoredFunctionOperationCall && principal == "alice" && function == "echo"
		},
	})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}

	var received []byte
	if _, err := registry.Register("admin", StoredFunctionSpec{
		Name:    "echo",
		Version: 1,
		Handler: func(_ context.Context, input []byte) ([]byte, error) {
			received = append([]byte(nil), input...)
			input[0] = 'X'
			return append([]byte("v1:"), input...), nil
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	input := []byte("hello")
	output, err := registry.Call(context.Background(), "alice", "echo", 1, input)
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if !bytes.Equal(output, []byte("v1:Xello")) || !bytes.Equal(input, []byte("hello")) || !bytes.Equal(received, []byte("hello")) {
		t.Fatalf("Call() output=%q input=%q received=%q", output, input, received)
	}
	output[0] = 'X'
	outputAgain, err := registry.Call(context.Background(), "alice", "echo", 1, []byte("hello"))
	if err != nil || !bytes.Equal(outputAgain, []byte("v1:Xello")) {
		t.Fatalf("output ownership output=%q err=%v", outputAgain, err)
	}

	if _, err := registry.Register("bob", StoredFunctionSpec{Name: "other", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { return nil, nil }}); !errors.Is(err, ErrStoredFunctionAccessDenied) {
		t.Fatalf("unauthorized Register() error = %v, want ErrStoredFunctionAccessDenied", err)
	}
	if _, err := registry.Call(context.Background(), "bob", "echo", 1, nil); !errors.Is(err, ErrStoredFunctionAccessDenied) {
		t.Fatalf("unauthorized Call() error = %v, want ErrStoredFunctionAccessDenied", err)
	}
	if _, err := registry.Call(context.Background(), "alice", "missing", 0, nil); !errors.Is(err, ErrStoredFunctionAccessDenied) {
		t.Fatalf("missing unauthorized Call() error = %v, want ErrStoredFunctionAccessDenied", err)
	}

	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "echo", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { return nil, nil }}); !errors.Is(err, ErrStoredFunctionVersionConflict) {
		t.Fatalf("same-version Register() error = %v, want ErrStoredFunctionVersionConflict", err)
	}
	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "echo", Version: 2, Handler: func(_ context.Context, input []byte) ([]byte, error) { return append([]byte("v2:"), input...), nil }}); err != nil {
		t.Fatalf("versioned Register() error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "alice", "echo", 1, []byte("x")); !errors.Is(err, ErrStoredFunctionVersionMismatch) {
		t.Fatalf("stale Call() error = %v, want ErrStoredFunctionVersionMismatch", err)
	}
	latest, err := registry.Call(context.Background(), "alice", "echo", 0, []byte("x"))
	if err != nil || !bytes.Equal(latest, []byte("v2:x")) {
		t.Fatalf("latest Call() output=%q err=%v", latest, err)
	}
}

func TestTU03StoredFunctionRegistryPanicContextAndLimits(t *testing.T) {
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{
		MaxInputBytes:  4,
		MaxOutputBytes: 4,
		Authorize:      func(_, _, _ string) bool { return true },
	})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}
	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "panic", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { panic("secret") }}); err != nil {
		t.Fatalf("Register(panic) error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "caller", "panic", 0, nil); !errors.Is(err, ErrStoredFunctionPanic) {
		t.Fatalf("panic Call() error = %v, want ErrStoredFunctionPanic", err)
	}
	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "large", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { return []byte("12345"), nil }}); err != nil {
		t.Fatalf("Register(large) error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "caller", "large", 0, []byte("12345")); !errors.Is(err, ErrStoredFunctionInputLimit) {
		t.Fatalf("input limit error = %v, want ErrStoredFunctionInputLimit", err)
	}
	if _, err := registry.Call(context.Background(), "caller", "large", 0, []byte("1234")); !errors.Is(err, ErrStoredFunctionOutputLimit) {
		t.Fatalf("output limit error = %v, want ErrStoredFunctionOutputLimit", err)
	}

	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "cancel", Version: 1, Handler: func(ctx context.Context, _ []byte) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }}); err != nil {
		t.Fatalf("Register(cancel) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Call(ctx, "caller", "cancel", 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Call() error = %v, want context.Canceled", err)
	}
}

func TestTU03StoredFunctionRegistryListsAndPolicyAuthorizer(t *testing.T) {
	policy := Policy{
		Principals: map[string][]string{"alice": {"reader"}, "admin": {"operator"}},
		Roles: []Role{
			{Name: "reader", Rules: []Rule{{Commands: []string{"CALL"}, Objects: []string{"function:echo"}}}},
			{Name: "operator", Rules: []Rule{{Commands: []string{"REGISTER"}, Objects: []string{"function:*"}}}},
		},
	}
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{Authorize: PolicyStoredFunctionAuthorizer(policy)})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}
	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "zeta", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { return []byte("z"), nil }}); err != nil {
		t.Fatalf("Register(zeta) error = %v", err)
	}
	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "echo", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { return []byte("e"), nil }}); err != nil {
		t.Fatalf("Register(echo) error = %v", err)
	}
	functions := registry.List()
	if len(functions) != 2 {
		t.Fatalf("List() length = %d, want 2", len(functions))
	}
	if !sort.SliceIsSorted(functions, func(i, j int) bool { return functions[i].Name < functions[j].Name }) {
		t.Fatalf("List() is not sorted: %#v", functions)
	}
	if _, err := registry.Call(context.Background(), "alice", "echo", 0, nil); err != nil {
		t.Fatalf("policy-authorized echo Call() error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "alice", "zeta", 0, nil); !errors.Is(err, ErrStoredFunctionAccessDenied) {
		t.Fatalf("policy-denied zeta Call() error = %v", err)
	}
	if !reflect.DeepEqual(functions[0].Name, "echo") {
		t.Fatalf("first listed function = %#v, want echo", functions[0])
	}
}

func TestTU03StoredFunctionRegistryRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{}); !errors.Is(err, ErrStoredFunctionRegistryInvalid) {
		t.Fatalf("nil authorizer error = %v, want ErrStoredFunctionRegistryInvalid", err)
	}
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{Authorize: func(_, _, _ string) bool { return true }})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "caller", "missing", 0, nil); !errors.Is(err, ErrStoredFunctionNotFound) {
		t.Fatalf("missing authorized Call() error = %v, want ErrStoredFunctionNotFound", err)
	}
}
