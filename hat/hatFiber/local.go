package hatFiber

import "errors"

const maxRetainedLocalEntries = 32

var (
	// ErrLocalInvalid reports a nil Local, an invalid Context, or a Context
	// whose fiber has already completed or been canceled.
	ErrLocalInvalid = errors.New("hatFiber: invalid local context")
)

// Local is typed storage owned by one fiber. The zero value is ready to use;
// a pointer to the Local identifies its slot, so copying a Local creates a
// separate slot. Values are not inherited by a newly spawned fiber and are
// cleared before the scheduler reuses a fiber slot.
//
// Local operations are safe when called from a Step or with a Context retained
// for inspection after completion. A retained Context observes no value after
// its fiber is released. Contexts should not be used to mutate a live fiber
// concurrently with its Step.
type Local[T any] struct {
	marker byte
}

// NewLocal returns an initialized typed local. The zero value of Local is also
// valid and is useful when declaring package-level locals.
func NewLocal[T any]() *Local[T] {
	return &Local[T]{}
}

// Get returns the value stored for this fiber and whether it is present.
func (local *Local[T]) Get(ctx Context) (value T, ok bool) {
	if local == nil || ctx.scheduler == nil || ctx.fiberState == nil || ctx.id == 0 {
		return value, false
	}
	fiber := ctx.fiberState
	if fiber.localID.Load() != ctx.id {
		return value, false
	}
	fiber.localMu.RLock()
	defer fiber.localMu.RUnlock()
	if fiber.localID.Load() != ctx.id {
		return value, false
	}
	raw, ok := fiber.locals[local]
	if !ok {
		return value, false
	}
	value, ok = raw.(T)
	return value, ok
}

// Set stores value for this fiber. It returns ErrLocalInvalid when ctx no
// longer identifies a live fiber.
func (local *Local[T]) Set(ctx Context, value T) error {
	if local == nil || ctx.scheduler == nil || ctx.fiberState == nil || ctx.id == 0 {
		return ErrLocalInvalid
	}
	fiber := ctx.fiberState
	if fiber.localID.Load() != ctx.id {
		return ErrLocalInvalid
	}
	fiber.localMu.Lock()
	defer fiber.localMu.Unlock()
	if fiber.localID.Load() != ctx.id {
		return ErrLocalInvalid
	}
	if fiber.locals == nil {
		fiber.locals = make(map[any]any)
	}
	fiber.locals[local] = value
	return nil
}

// Delete removes this fiber's value and reports whether one was present.
func (local *Local[T]) Delete(ctx Context) bool {
	if local == nil || ctx.scheduler == nil || ctx.fiberState == nil || ctx.id == 0 {
		return false
	}
	fiber := ctx.fiberState
	if fiber.localID.Load() != ctx.id {
		return false
	}
	fiber.localMu.Lock()
	defer fiber.localMu.Unlock()
	if fiber.localID.Load() != ctx.id {
		return false
	}
	if _, ok := fiber.locals[local]; !ok {
		return false
	}
	delete(fiber.locals, local)
	return true
}
