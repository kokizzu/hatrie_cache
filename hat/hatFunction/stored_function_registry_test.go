package hatFunction

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestStoredFunctionRegistryRegistersCallsAndListsDeterministically(t *testing.T) {
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{MaxFunctions: 4, MaxArguments: 4})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}
	input := []any{int64(2), int64(3)}
	if err := registry.Register(StoredFunction{
		Name:          "ADD",
		Version:       2,
		Arity:         2,
		Deterministic: true,
		Handler: func(_ context.Context, args []any) (any, error) {
			args[0] = int64(99)
			return args[0].(int64) + args[1].(int64), nil
		},
	}); err != nil {
		t.Fatalf("Register(add) error = %v", err)
	}
	if err := registry.Register(StoredFunction{
		Name:    "echo",
		Version: 1,
		Arity:   -1,
		Handler: func(_ context.Context, args []any) (any, error) { return args, nil },
	}); err != nil {
		t.Fatalf("Register(echo) error = %v", err)
	}
	result, err := registry.Call(context.Background(), " add ", input)
	if err != nil {
		t.Fatalf("Call(add) error = %v", err)
	}
	if result != int64(102) {
		t.Fatalf("Call(add) = %#v, want 102", result)
	}
	if !reflect.DeepEqual(input, []any{int64(2), int64(3)}) {
		t.Fatalf("handler mutated caller args: %#v", input)
	}
	got, err := registry.Call(context.Background(), "ECHO", []any{"a", "b"})
	if err != nil || !reflect.DeepEqual(got, []any{"a", "b"}) {
		t.Fatalf("Call(echo) = %#v, error = %v", got, err)
	}
	infos := registry.List()
	if len(infos) != 2 || infos[0].Name != "add" || infos[1].Name != "echo" {
		t.Fatalf("List() = %#v", infos)
	}
	if info, ok := registry.Lookup("ADD"); !ok || info.Version != 2 || info.Arity != 2 || !info.Deterministic {
		t.Fatalf("Lookup(add) = %#v/%t", info, ok)
	}
	if !sort.SliceIsSorted(infos, func(left, right int) bool { return infos[left].Name < infos[right].Name }) {
		t.Fatal("List() is not sorted")
	}
}

func TestStoredFunctionRegistryRejectsInvalidCallsAndIsolatesPanics(t *testing.T) {
	var zero StoredFunctionRegistry
	if err := zero.Register(StoredFunction{Name: "zero", Version: 1, Arity: 0, Handler: func(context.Context, []any) (any, error) { return nil, nil }}); !errors.Is(err, ErrStoredFunctionInvalid) {
		t.Fatalf("zero-value Register() error = %v, want invalid", err)
	}
	if _, err := zero.Call(context.Background(), "zero", nil); !errors.Is(err, ErrStoredFunctionInvalid) {
		t.Fatalf("zero-value Call() error = %v, want invalid", err)
	}
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{MaxFunctions: 1, MaxArguments: 2})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}
	invalid := []StoredFunction{
		{Name: "", Version: 1, Arity: 0, Handler: func(context.Context, []any) (any, error) { return nil, nil }},
		{Name: "bad name", Version: 1, Arity: 0, Handler: func(context.Context, []any) (any, error) { return nil, nil }},
		{Name: "zero-version", Version: 0, Arity: 0, Handler: func(context.Context, []any) (any, error) { return nil, nil }},
		{Name: "bad-arity", Version: 1, Arity: -2, Handler: func(context.Context, []any) (any, error) { return nil, nil }},
		{Name: "nil-handler", Version: 1, Arity: 0},
	}
	for index, function := range invalid {
		if err := registry.Register(function); !errors.Is(err, ErrStoredFunctionInvalid) {
			t.Fatalf("invalid function %d error = %v, want invalid", index, err)
		}
	}
	panicFunction := StoredFunction{
		Name:    "panic_fn",
		Version: 1,
		Arity:   0,
		Handler: func(context.Context, []any) (any, error) { panic("handler failure") },
	}
	if err := registry.Register(panicFunction); err != nil {
		t.Fatalf("Register(panic_fn) error = %v", err)
	}
	if _, err := registry.RegisteredFunction("missing"); !errors.Is(err, ErrStoredFunctionNotFound) {
		t.Fatalf("RegisteredFunction(missing) error = %v, want not found", err)
	}
	if err := registry.Register(panicFunction); !errors.Is(err, ErrStoredFunctionExists) {
		t.Fatalf("duplicate Register() error = %v, want exists", err)
	}
	if _, err := registry.Call(context.Background(), "panic_fn", nil); !errors.Is(err, ErrStoredFunctionPanic) {
		t.Fatalf("panic Call() error = %v, want panic error", err)
	}
	if _, err := registry.Call(context.Background(), "missing", nil); !errors.Is(err, ErrStoredFunctionNotFound) {
		t.Fatalf("missing Call() error = %v, want not found", err)
	}
	if err := registry.Unregister("panic_fn"); !err {
		t.Fatal("Unregister(panic_fn) = false")
	}
	if registry.Unregister("panic_fn") {
		t.Fatal("second Unregister(panic_fn) = true")
	}
}

func TestStoredFunctionRegistryEnforcesContextArityAndCapacity(t *testing.T) {
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{MaxFunctions: 1, MaxArguments: 1})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}
	called := false
	if err := registry.Register(StoredFunction{
		Name:    "one",
		Version: 1,
		Arity:   1,
		Handler: func(context.Context, []any) (any, error) { called = true; return nil, nil },
	}); err != nil {
		t.Fatalf("Register(one) error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "one", nil); !errors.Is(err, ErrStoredFunctionArity) {
		t.Fatalf("missing argument error = %v, want arity", err)
	}
	if _, err := registry.Call(context.Background(), "one", []any{1, 2}); !errors.Is(err, ErrStoredFunctionArgumentLimit) {
		t.Fatalf("argument limit error = %v, want argument limit", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Call(ctx, "one", []any{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("canceled call invoked handler")
	}
	if err := registry.Register(StoredFunction{Name: "two", Version: 1, Arity: 0, Handler: func(context.Context, []any) (any, error) { return nil, nil }}); !errors.Is(err, ErrStoredFunctionCapacity) {
		t.Fatalf("capacity error = %v, want capacity", err)
	}
}

func TestStoredFunctionRegistryConcurrentCallsAndRegistration(t *testing.T) {
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{MaxFunctions: 32, MaxArguments: 2})
	if err != nil {
		t.Fatalf("NewStoredFunctionRegistry() error = %v", err)
	}
	if err := registry.Register(StoredFunction{Name: "identity", Version: 1, Arity: 1, Handler: func(_ context.Context, args []any) (any, error) { return args[0], nil }}); err != nil {
		t.Fatalf("Register(identity) error = %v", err)
	}
	const workers = 16
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wait.Done()
			for call := 0; call < 32; call++ {
				value, err := registry.Call(context.Background(), "identity", []any{worker})
				if err != nil || value != worker {
					t.Errorf("Call(identity) = %#v, error = %v", value, err)
					return
				}
			}
		}(worker)
	}
	wait.Wait()
}

func BenchmarkStoredFunctionDirectBaseline(b *testing.B) {
	handler := func(value int64) int64 { return value + 1 }
	var result int64
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result = handler(int64(index))
	}
	_ = result
}

func BenchmarkStoredFunctionRegistryCall(b *testing.B) {
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{MaxFunctions: 4, MaxArguments: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(StoredFunction{Name: "increment", Version: 1, Arity: 1, Handler: func(_ context.Context, args []any) (any, error) { return args[0].(int64) + 1, nil }}); err != nil {
		b.Fatal(err)
	}
	var result any
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var err error
		result, err = registry.Call(context.Background(), "increment", []any{int64(index)})
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = result
}

func BenchmarkStoredFunctionDirectOwnedArgsBaseline(b *testing.B) {
	handler := func(args []any) (any, error) { return args[0].(int64) + 1, nil }
	input := []any{int64(1)}
	var result any
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		owned := append([]any(nil), input...)
		var err error
		result, err = handler(owned)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = result
}
