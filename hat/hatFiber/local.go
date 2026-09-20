package hatFiber

// fiberLocalStore is the scheduler-side cleanup hook used by typed Local
// instances. It is intentionally unexported; Local is the public API.
type fiberLocalStore interface {
	clear(index uint32)
}

type localCell[T any] struct {
	identifier FiberID
	value      T
}

// Local is typed storage scoped to the currently running fiber. A Local does
// not use a map or allocate on Set/Get; its cells are bounded by the
// scheduler's MaxFibers capacity.
type Local[T any] struct {
	scheduler *Scheduler
	cells     []localCell[T]
}

// NewLocal creates a typed fiber-local value store attached to scheduler.
// Values are cleared when their fiber reaches a terminal state.
func NewLocal[T any](scheduler *Scheduler) (*Local[T], error) {
	if scheduler == nil {
		return nil, ErrSchedulerRequired
	}
	local := &Local[T]{
		scheduler: scheduler,
		cells:     make([]localCell[T], scheduler.Capacity()),
	}
	scheduler.registerLocal(local)
	return local, nil
}

// Get returns the value for the current fiber. The boolean reports whether a
// value has been set; ErrFiberNotRunning is returned outside a callback.
func (local *Local[T]) Get() (value T, ok bool, err error) {
	identifier, index, err := local.current()
	if err != nil {
		return value, false, err
	}
	cell := &local.cells[index]
	if cell.identifier != identifier {
		local.clear(index)
		return value, false, nil
	}
	return cell.value, true, nil
}

// Set stores value for the current fiber. The value remains available across
// explicit yields until Delete or a terminal fiber transition clears it.
func (local *Local[T]) Set(value T) error {
	identifier, index, err := local.current()
	if err != nil {
		return err
	}
	cell := &local.cells[index]
	cell.identifier = identifier
	cell.value = value
	return nil
}

// Delete removes the current fiber's value and releases any referenced data.
func (local *Local[T]) Delete() error {
	_, index, err := local.current()
	if err != nil {
		return err
	}
	local.clear(index)
	return nil
}

func (local *Local[T]) current() (FiberID, uint32, error) {
	if local == nil || local.scheduler == nil {
		return 0, 0, ErrSchedulerRequired
	}
	identifier := local.scheduler.Current()
	if identifier == 0 {
		return 0, 0, ErrFiberNotRunning
	}
	slot, err := local.scheduler.lookup(identifier)
	if err != nil {
		return 0, 0, err
	}
	if slot.status != StatusRunning {
		return 0, 0, ErrFiberNotRunning
	}
	return identifier, fiberIndex(identifier), nil
}

func (local *Local[T]) clear(index uint32) {
	if local == nil || uint64(index) >= uint64(len(local.cells)) {
		return
	}
	cell := &local.cells[index]
	cell.identifier = 0
	var zero T
	cell.value = zero
}
