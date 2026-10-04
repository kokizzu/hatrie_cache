package hatSandbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

var tg50PlusOneModule = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
	0x01, 0x06, 0x01, 0x60, 0x01, 0x7e, 0x01, 0x7e,
	0x03, 0x02, 0x01, 0x00,
	0x07, 0x0c, 0x01, 0x08, 'p', 'l', 'u', 's', '_', 'o', 'n', 'e', 0x00, 0x00,
	0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x42, 0x01, 0x7c, 0x0b,
}

var tg50TwoResultsModule = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
	0x01, 0x06, 0x01, 0x60, 0x00, 0x02, 0x7e, 0x7e,
	0x03, 0x02, 0x01, 0x00,
	0x07, 0x07, 0x01, 0x03, 't', 'w', 'o', 0x00, 0x00,
	0x0a, 0x08, 0x01, 0x06, 0x00, 0x42, 0x29, 0x42, 0x01, 0x0b,
}

var tg50ImportedFunctionModule = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
	0x02, 0x09, 0x01, 0x03, 'e', 'n', 'v', 0x01, 'f', 0x00, 0x00,
}

func TestTG50SandboxCallsExportedFunction(t *testing.T) {
	sandbox, err := New(context.Background(), tg50PlusOneModule, DefaultSandboxOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer sandbox.Close()

	got, err := sandbox.Call(context.Background(), "plus_one", 41)
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if len(got) != 1 || got[0] != 42 {
		t.Fatalf("Call() = %v, want [42]", got)
	}
}

func TestTG50SandboxRejectsOversizedModule(t *testing.T) {
	options := DefaultSandboxOptions()
	options.MaxModuleBytes = uint64(len(tg50PlusOneModule) - 1)

	_, err := New(context.Background(), tg50PlusOneModule, options)
	if !errors.Is(err, ErrSandboxModuleTooLarge) {
		t.Fatalf("New() error = %v, want ErrSandboxModuleTooLarge", err)
	}
}

func TestTG50SandboxRejectsInvalidOptions(t *testing.T) {
	options := DefaultSandboxOptions()
	options.MaxParameters = -1
	if _, err := New(context.Background(), tg50PlusOneModule, options); !errors.Is(err, ErrSandboxInvalidOptions) {
		t.Fatalf("negative MaxParameters error = %v, want ErrSandboxInvalidOptions", err)
	}

	options = DefaultSandboxOptions()
	options.ExecutionTimeout = -time.Second
	if _, err := New(context.Background(), tg50PlusOneModule, options); !errors.Is(err, ErrSandboxInvalidOptions) {
		t.Fatalf("negative ExecutionTimeout error = %v, want ErrSandboxInvalidOptions", err)
	}
}

func TestTG50SandboxEnforcesCallLimits(t *testing.T) {
	options := DefaultSandboxOptions()
	options.MaxParameters = 1
	sandbox, err := New(context.Background(), tg50PlusOneModule, options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer sandbox.Close()

	if _, err := sandbox.Call(context.Background(), "plus_one", 1, 2); !errors.Is(err, ErrSandboxLimitExceeded) {
		t.Fatalf("too many parameters error = %v, want ErrSandboxLimitExceeded", err)
	}

	options = DefaultSandboxOptions()
	options.MaxResults = 1
	sandbox, err = New(context.Background(), tg50TwoResultsModule, options)
	if err != nil {
		t.Fatalf("New(two results) error = %v", err)
	}
	defer sandbox.Close()
	if _, err := sandbox.Call(context.Background(), "two"); !errors.Is(err, ErrSandboxLimitExceeded) {
		t.Fatalf("too many results error = %v, want ErrSandboxLimitExceeded", err)
	}
}

func TestTG50SandboxRejectsImports(t *testing.T) {
	_, err := New(context.Background(), tg50ImportedFunctionModule, DefaultSandboxOptions())
	if !errors.Is(err, ErrSandboxImportsNotAllowed) {
		t.Fatalf("New() error = %v, want ErrSandboxImportsNotAllowed", err)
	}
}

func TestTG50SandboxCloseIsIdempotent(t *testing.T) {
	sandbox, err := New(context.Background(), tg50PlusOneModule, DefaultSandboxOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := sandbox.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := sandbox.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := sandbox.Call(context.Background(), "plus_one", 41); !errors.Is(err, ErrSandboxClosed) {
		t.Fatalf("Call() after Close error = %v, want ErrSandboxClosed", err)
	}
}

func TestTG50SandboxRejectsCanceledCall(t *testing.T) {
	sandbox, err := New(context.Background(), tg50PlusOneModule, DefaultSandboxOptions())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer sandbox.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = sandbox.Call(ctx, "plus_one", 41)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Call() error = %v, want context.Canceled", err)
	}
}
