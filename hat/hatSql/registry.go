package hatSql

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// FunctionCompiler normalizes and compiles a function definition.
type FunctionCompiler func(FunctionDefinition) (FunctionDefinition, FunctionRuntime, error)

// Registry owns concurrent function replacement and evaluation. Persistence
// and language-specific compilation are injected by the embedding package.
type Registry struct {
	mu          sync.RWMutex
	compiler    FunctionCompiler
	functions   map[string]FunctionRuntime
	definitions map[string]FunctionDefinition
}

// NewRegistry creates an empty registry using compiler.
func NewRegistry(compiler FunctionCompiler) *Registry {
	return &Registry{compiler: compiler, functions: make(map[string]FunctionRuntime), definitions: make(map[string]FunctionDefinition)}
}

// Register compiles and atomically installs a function after persist accepts
// the complete replacement definition set.
func (registry *Registry) Register(definition FunctionDefinition, persist func([]FunctionDefinition) error) error {
	if registry == nil || registry.compiler == nil {
		return fmt.Errorf("SQL function registry is required")
	}
	normalized, runtime, err := registry.compiler(definition)
	if err != nil {
		return err
	}
	key := strings.ToUpper(normalized.Name)
	registry.mu.Lock()
	definitions := cloneDefinitions(registry.definitions)
	definitions[key] = normalized
	if persist != nil {
		if err := persist(sortedDefinitions(definitions)); err != nil {
			registry.mu.Unlock()
			closeRuntime(runtime)
			return err
		}
	}
	previous := registry.functions[key]
	registry.functions[key] = runtime
	registry.definitions = definitions
	registry.mu.Unlock()
	closeRuntime(previous)
	return nil
}

// Replace compiles and atomically replaces every function definition.
func (registry *Registry) Replace(definitions []FunctionDefinition) error {
	if registry == nil || registry.compiler == nil {
		return fmt.Errorf("SQL function registry is required")
	}
	functions := make(map[string]FunctionRuntime, len(definitions))
	stored := make(map[string]FunctionDefinition, len(definitions))
	for index, definition := range definitions {
		normalized, runtime, err := registry.compiler(definition)
		if err != nil {
			closeRuntimes(functions)
			return fmt.Errorf("load SQL function definition %d: %w", index, err)
		}
		key := strings.ToUpper(normalized.Name)
		if _, exists := functions[key]; exists {
			closeRuntime(runtime)
			closeRuntimes(functions)
			return fmt.Errorf("load SQL function definitions: duplicate function %q", normalized.Name)
		}
		functions[key], stored[key] = runtime, normalized
	}
	registry.mu.Lock()
	previous := registry.functions
	registry.functions, registry.definitions = functions, stored
	registry.mu.Unlock()
	closeRuntimes(previous)
	return nil
}

// EvaluateSQLFunction implements FunctionResolver.
func (registry *Registry) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	if registry == nil {
		return nil, fmt.Errorf("SQL function registry is required")
	}
	registry.mu.RLock()
	function := registry.functions[strings.ToUpper(strings.TrimSpace(name))]
	registry.mu.RUnlock()
	if function == nil {
		return nil, fmt.Errorf("unknown SQL function %q", name)
	}
	return function.Evaluate(calls)
}

// Close releases every installed runtime.
func (registry *Registry) Close() {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	functions := registry.functions
	registry.functions, registry.definitions = make(map[string]FunctionRuntime), make(map[string]FunctionDefinition)
	registry.mu.Unlock()
	closeRuntimes(functions)
}

func cloneDefinitions(source map[string]FunctionDefinition) map[string]FunctionDefinition {
	out := make(map[string]FunctionDefinition, len(source)+1)
	for key, value := range source {
		out[key] = value
	}
	return out
}

func sortedDefinitions(definitions map[string]FunctionDefinition) []FunctionDefinition {
	out := make([]FunctionDefinition, 0, len(definitions))
	for _, definition := range definitions {
		out = append(out, definition)
	}
	sort.Slice(out, func(left, right int) bool { return out[left].Name < out[right].Name })
	return out
}

func closeRuntime(runtime FunctionRuntime) {
	if closer, ok := runtime.(interface{ Close() }); ok {
		closer.Close()
	}
}
func closeRuntimes(functions map[string]FunctionRuntime) {
	for _, runtime := range functions {
		closeRuntime(runtime)
	}
}
