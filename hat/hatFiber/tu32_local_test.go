package hatFiber

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU32FiberLocalIsolationAndReuse(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	local, err := NewLocal[string](scheduler)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	if _, ok, err := local.Get(); !errors.Is(err, ErrFiberNotRunning) || ok {
		t.Fatalf("Get() outside callback = ok %v, error %v", ok, err)
	}
	if err := local.Set("outside"); !errors.Is(err, ErrFiberNotRunning) {
		t.Fatalf("Set() outside callback error = %v", err)
	}

	values := make([]string, 0, 2)
	for _, expected := range []string{"first", "second"} {
		value := expected
		identifier, err := scheduler.Spawn(func(context.Context) (Step, error) {
			if _, ok, err := local.Get(); err != nil {
				return 0, err
			} else if ok {
				return 0, errors.New("new fiber inherited a local value")
			}
			if err := local.Set(value); err != nil {
				return 0, err
			}
			got, ok, err := local.Get()
			if err != nil {
				return 0, err
			}
			if !ok || got != value {
				return 0, errors.New("fiber local value was not readable")
			}
			values = append(values, got)
			return StepDone, nil
		})
		if err != nil {
			t.Fatalf("Spawn(%s) error = %v", expected, err)
		}
		if _, err := scheduler.Run(context.Background(), 0); err != nil {
			t.Fatalf("Run(%s) error = %v", expected, err)
		}
		if scheduler.Pending() != 0 {
			t.Fatalf("Pending() after %s = %d, want 0", expected, scheduler.Pending())
		}
		if err := scheduler.Reap(identifier); err != nil {
			t.Fatalf("Reap(%s) error = %v", expected, err)
		}
	}
	if !reflect.DeepEqual(values, []string{"first", "second"}) {
		t.Fatalf("values = %v, want [first second]", values)
	}
}

func TestTU32FiberLocalConcurrentIsolationAndDelete(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	local, err := NewLocal[int](scheduler)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	observed := make([]int, 0, 2)
	for _, expected := range []int{11, 22} {
		expected := expected
		started := false
		if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
			if !started {
				if err := local.Set(expected); err != nil {
					return 0, err
				}
				started = true
				return StepYield, nil
			}
			value, ok, err := local.Get()
			if err != nil {
				return 0, err
			}
			if !ok {
				return 0, errors.New("fiber local value disappeared before retry")
			}
			observed = append(observed, value)
			if err := local.Delete(); err != nil {
				return 0, err
			}
			if _, ok, err := local.Get(); err != nil {
				return 0, err
			} else if ok {
				return 0, errors.New("deleted fiber local value remained visible")
			}
			return StepDone, nil
		}); err != nil {
			t.Fatalf("Spawn(%d) error = %v", expected, err)
		}
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		return StepDone, nil
	}); !errors.Is(err, ErrFiberCapacity) {
		t.Fatalf("Spawn(over capacity) error = %v, want ErrFiberCapacity", err)
	}
	if _, err := scheduler.Run(context.Background(), 2); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	// The two fibers were spawned with a single StepFunc closure each. Reusing
	// the slots after the first yield verifies that the local value follows the
	// current FiberID rather than a shared scheduler field.
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if !reflect.DeepEqual(observed, []int{11, 22}) {
		t.Fatalf("observed = %v, want [11 22]", observed)
	}
}

func TestTU32FiberLocalTerminalCleanup(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	local, err := NewLocal[*int](scheduler)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	payload := 42
	identifier, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if err := local.Set(&payload); err != nil {
			return 0, err
		}
		return StepDone, nil
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	cell := local.cells[fiberIndex(identifier)]
	if cell.identifier != 0 || cell.value != nil {
		t.Fatalf("terminal local cell = identifier %v, value %v; want cleared", cell.identifier, cell.value)
	}
	if err := scheduler.Reap(identifier); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
}

func TestTU32FiberLocalCancelCleanupAndZeroValue(t *testing.T) {
	if _, err := NewLocal[int](nil); !errors.Is(err, ErrSchedulerRequired) {
		t.Fatalf("NewLocal(nil) error = %v, want ErrSchedulerRequired", err)
	}
	var zero Local[int]
	if _, _, err := zero.Get(); !errors.Is(err, ErrSchedulerRequired) {
		t.Fatalf("zero Local.Get() error = %v, want ErrSchedulerRequired", err)
	}
	if err := zero.Set(1); !errors.Is(err, ErrSchedulerRequired) {
		t.Fatalf("zero Local.Set() error = %v, want ErrSchedulerRequired", err)
	}

	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	local, err := NewLocal[*int](scheduler)
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	payload := 7
	identifier, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if err := local.Set(&payload); err != nil {
			return 0, err
		}
		return StepYield, nil
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 1); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := scheduler.Cancel(identifier); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	cell := local.cells[fiberIndex(identifier)]
	if cell.identifier != 0 || cell.value != nil {
		t.Fatalf("canceled local cell = identifier %v, value %v; want cleared", cell.identifier, cell.value)
	}
	if err := scheduler.Reap(identifier); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
}
