package hatSandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

const (
	// DefaultSandboxMaxModuleBytes limits the untrusted WebAssembly binary to 1 MiB.
	DefaultSandboxMaxModuleBytes uint64 = 1 << 20
	// DefaultSandboxMaxMemoryPages limits linear memory to 16 MiB (64 KiB pages).
	DefaultSandboxMaxMemoryPages   uint32 = 256
	DefaultSandboxMaxParameters           = 16
	DefaultSandboxMaxResults              = 4
	DefaultSandboxExecutionTimeout        = 100 * time.Millisecond
)

var (
	ErrSandboxInvalidOptions    = errors.New("hatSandbox: invalid options")
	ErrSandboxInvalidModule     = errors.New("hatSandbox: invalid module")
	ErrSandboxModuleTooLarge    = errors.New("hatSandbox: module too large")
	ErrSandboxImportsNotAllowed = errors.New("hatSandbox: imports are not allowed")
	ErrSandboxExportNotFound    = errors.New("hatSandbox: export not found")
	ErrSandboxLimitExceeded     = errors.New("hatSandbox: execution limit exceeded")
	ErrSandboxClosed            = errors.New("hatSandbox: closed")
)

// SandboxOptions bounds compilation and execution of an untrusted module.
// Zero values use the corresponding DefaultSandboxOptions value.
type SandboxOptions struct {
	MaxModuleBytes   uint64
	MaxMemoryPages   uint32
	MaxParameters    int
	MaxResults       int
	ExecutionTimeout time.Duration
}

// DefaultSandboxOptions returns conservative limits for short, pure functions.
func DefaultSandboxOptions() SandboxOptions {
	return SandboxOptions{
		MaxModuleBytes:   DefaultSandboxMaxModuleBytes,
		MaxMemoryPages:   DefaultSandboxMaxMemoryPages,
		MaxParameters:    DefaultSandboxMaxParameters,
		MaxResults:       DefaultSandboxMaxResults,
		ExecutionTimeout: DefaultSandboxExecutionTimeout,
	}
}

// Sandbox is a compiled WebAssembly module with no host imports.
//
// Calls are serialized so a module cannot race through shared linear memory.
// Callers should close a Sandbox when it is no longer needed.
type Sandbox struct {
	mu      sync.Mutex
	runtime wazero.Runtime
	module  api.Module
	options SandboxOptions
	closed  bool
}

// New compiles and instantiates a WebAssembly module under options.
// Modules that require host functions or host memory are rejected.
func New(ctx context.Context, wasm []byte, options SandboxOptions) (*Sandbox, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrSandboxInvalidOptions)
	}
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	if len(wasm) == 0 {
		return nil, fmt.Errorf("%w: empty module", ErrSandboxInvalidModule)
	}
	if uint64(len(wasm)) > options.MaxModuleBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", ErrSandboxModuleTooLarge, len(wasm), options.MaxModuleBytes)
	}

	config := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(options.MaxMemoryPages).
		WithCloseOnContextDone(true)
	runtime := wazero.NewRuntimeWithConfig(ctx, config)
	compiled, err := runtime.CompileModule(ctx, wasm)
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("%w: %v", ErrSandboxInvalidModule, err)
	}
	if len(compiled.ImportedFunctions()) != 0 || len(compiled.ImportedMemories()) != 0 {
		_ = compiled.Close(ctx)
		_ = runtime.Close(ctx)
		return nil, ErrSandboxImportsNotAllowed
	}

	module, err := runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
	if err != nil {
		_ = compiled.Close(ctx)
		_ = runtime.Close(ctx)
		return nil, fmt.Errorf("%w: %v", ErrSandboxInvalidModule, err)
	}
	return &Sandbox{runtime: runtime, module: module, options: options}, nil
}

// Call invokes an exported numeric WebAssembly function.
func (sandbox *Sandbox) Call(ctx context.Context, export string, params ...uint64) ([]uint64, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrSandboxInvalidOptions)
	}
	if export == "" {
		return nil, fmt.Errorf("%w: empty export name", ErrSandboxExportNotFound)
	}

	sandbox.mu.Lock()
	defer sandbox.mu.Unlock()
	if sandbox.closed || sandbox.module == nil {
		return nil, ErrSandboxClosed
	}
	if len(params) > sandbox.options.MaxParameters {
		return nil, fmt.Errorf("%w: %d parameters exceeds %d", ErrSandboxLimitExceeded, len(params), sandbox.options.MaxParameters)
	}
	function := sandbox.module.ExportedFunction(export)
	if function == nil {
		return nil, fmt.Errorf("%w: %s", ErrSandboxExportNotFound, export)
	}
	if results := len(function.Definition().ResultTypes()); results > sandbox.options.MaxResults {
		return nil, fmt.Errorf("%w: %d results exceeds %d", ErrSandboxLimitExceeded, results, sandbox.options.MaxResults)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	callCtx, cancel := context.WithTimeout(ctx, sandbox.options.ExecutionTimeout)
	defer cancel()
	results, err := function.Call(callCtx, params...)
	if err != nil {
		if callErr := callCtx.Err(); callErr != nil {
			return nil, callErr
		}
		return nil, err
	}
	return results, nil
}

// Close releases the runtime. It is safe to call more than once.
func (sandbox *Sandbox) Close() error {
	if sandbox == nil {
		return nil
	}
	sandbox.mu.Lock()
	defer sandbox.mu.Unlock()
	if sandbox.closed {
		return nil
	}
	sandbox.closed = true
	if sandbox.runtime == nil {
		return nil
	}
	err := sandbox.runtime.Close(context.Background())
	sandbox.module = nil
	sandbox.runtime = nil
	return err
}

func normalizeOptions(options SandboxOptions) (SandboxOptions, error) {
	defaults := DefaultSandboxOptions()
	if options.MaxModuleBytes == 0 {
		options.MaxModuleBytes = defaults.MaxModuleBytes
	}
	if options.MaxMemoryPages == 0 {
		options.MaxMemoryPages = defaults.MaxMemoryPages
	}
	if options.MaxParameters == 0 {
		options.MaxParameters = defaults.MaxParameters
	}
	if options.MaxResults == 0 {
		options.MaxResults = defaults.MaxResults
	}
	if options.ExecutionTimeout == 0 {
		options.ExecutionTimeout = defaults.ExecutionTimeout
	}
	if options.MaxParameters < 0 || options.MaxResults < 0 || options.ExecutionTimeout < 0 {
		return SandboxOptions{}, fmt.Errorf("%w: negative limit", ErrSandboxInvalidOptions)
	}
	if options.MaxMemoryPages > 65536 {
		return SandboxOptions{}, fmt.Errorf("%w: memory limit exceeds WebAssembly maximum", ErrSandboxInvalidOptions)
	}
	return options, nil
}
