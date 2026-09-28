package hatWorkload

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

var (
	ErrMemoryAdmissionControllerNil   = errors.New("hatWorkload: memory admission controller is nil")
	ErrMemoryAdmissionRequestTooLarge = errors.New("hatWorkload: memory admission request exceeds class budget")
	ErrMemoryAdmissionOverflow        = errors.New("hatWorkload: memory admission reservation overflow")
)

// MemoryClassOptions adds an optional memory reservation budget to a workload
// class. A zero MaxMemoryBytes disables the memory limit for that class.
type MemoryClassOptions struct {
	ClassOptions
	MaxMemoryBytes uint64
}

// MemoryClassSnapshot reports admission and memory state for one class.
type MemoryClassSnapshot struct {
	ClassSnapshot
	MaxMemoryBytes      uint64 `json:"max_memory_bytes"`
	ReservedMemoryBytes uint64 `json:"reserved_memory_bytes"`
	ActiveMemoryBytes   uint64 `json:"active_memory_bytes"`
}

// MemoryAdmissionSnapshot is an immutable point-in-time report. Reserved
// memory includes requests waiting for an underlying concurrency slot; active
// memory includes requests that have received both reservations.
type MemoryAdmissionSnapshot struct {
	Admission AdmissionSnapshot `json:"admission"`
	AdmissionSnapshot
	ReservedMemoryBytes uint64                `json:"reserved_memory_bytes"`
	ActiveMemoryBytes   uint64                `json:"active_memory_bytes"`
	Classes             []MemoryClassSnapshot `json:"classes"`
}

type memoryAdmissionClass struct {
	MemoryClassOptions
	reservedMemoryBytes uint64
	activeMemoryBytes   uint64
}

type memoryAdmissionWaiter struct {
	class   string
	bytes   uint64
	done    chan struct{}
	granted bool
}

// MemoryAdmissionController combines the existing class-aware concurrency
// scheduler with per-class memory reservations. It is opt-in; callers that do
// not need memory budgets should continue using AdmissionController.
type MemoryAdmissionController struct {
	mu sync.Mutex

	admission *AdmissionController
	maxQueued int
	reserved  uint64
	active    uint64
	classes   map[string]*memoryAdmissionClass
	waiters   []*memoryAdmissionWaiter
}

// MemoryAdmissionLease represents one admitted request and its memory
// reservation. Release is safe to call more than once.
type MemoryAdmissionLease struct {
	controller *MemoryAdmissionController
	admission  AdmissionLease
	class      string
	bytes      uint64
	released   *uint32
}

// NewMemoryAdmissionController creates a memory-aware workload controller.
// The AdmissionOptions limits are shared with AdmissionController, including
// its bounded queue and priority scheduling.
func NewMemoryAdmissionController(options AdmissionOptions) (*MemoryAdmissionController, error) {
	admission, err := NewAdmissionController(options)
	if err != nil {
		return nil, err
	}
	maxQueued := options.MaxQueued
	if maxQueued == 0 {
		maxQueued = DefaultAdmissionMaxQueued
	}
	return &MemoryAdmissionController{
		admission: admission,
		maxQueued: maxQueued,
		classes:   make(map[string]*memoryAdmissionClass),
	}, nil
}

// RegisterClass adds one memory-aware workload class. ClassOptions retains the
// same normalization, duplicate detection, and concurrency rules as the
// underlying AdmissionController.
func (controller *MemoryAdmissionController) RegisterClass(options MemoryClassOptions) error {
	if controller == nil || controller.admission == nil {
		return ErrMemoryAdmissionControllerNil
	}
	name, err := normalizeClassName(options.Name)
	if err != nil {
		return err
	}
	options.Name = name

	controller.mu.Lock()
	defer controller.mu.Unlock()
	if _, exists := controller.classes[name]; exists {
		return ErrAdmissionClassDuplicate
	}
	if err := controller.admission.RegisterClass(options.ClassOptions); err != nil {
		return err
	}
	controller.classes[name] = &memoryAdmissionClass{MemoryClassOptions: options}
	controller.dispatchLocked()
	return nil
}

// Acquire waits for both a class memory reservation and an underlying
// concurrency slot. A zero bytes request consumes no memory budget.
func (controller *MemoryAdmissionController) Acquire(ctx context.Context, class string, bytes uint64) (MemoryAdmissionLease, error) {
	if controller == nil || controller.admission == nil {
		return MemoryAdmissionLease{}, ErrMemoryAdmissionControllerNil
	}
	if ctx == nil {
		return MemoryAdmissionLease{}, ErrAdmissionContextNil
	}
	if err := ctx.Err(); err != nil {
		return MemoryAdmissionLease{}, err
	}
	class, err := normalizeClassName(class)
	if err != nil {
		return MemoryAdmissionLease{}, err
	}

	controller.mu.Lock()
	state, exists := controller.classes[class]
	if !exists {
		controller.mu.Unlock()
		return MemoryAdmissionLease{}, ErrAdmissionClassNotFound
	}
	if state.MaxMemoryBytes != 0 && bytes > state.MaxMemoryBytes {
		controller.mu.Unlock()
		return MemoryAdmissionLease{}, ErrMemoryAdmissionRequestTooLarge
	}
	canReserve, reserveErr := controller.canReserveLocked(state, bytes)
	if reserveErr != nil {
		controller.mu.Unlock()
		return MemoryAdmissionLease{}, reserveErr
	}
	if len(controller.waiters) == 0 && canReserve {
		controller.reserveLocked(state, bytes)
		controller.mu.Unlock()
	} else {
		waiter := &memoryAdmissionWaiter{class: class, bytes: bytes, done: make(chan struct{})}
		if len(controller.waiters) >= controller.maxQueued {
			controller.mu.Unlock()
			return MemoryAdmissionLease{}, ErrAdmissionQueueFull
		}
		controller.waiters = append(controller.waiters, waiter)
		controller.dispatchLocked()
		granted := waiter.granted
		controller.mu.Unlock()
		if !granted {
			if err := controller.waitForMemory(ctx, waiter); err != nil {
				return MemoryAdmissionLease{}, err
			}
		}
	}

	admissionLease, err := controller.admission.Acquire(ctx, class)
	if err != nil {
		controller.releaseReservation(class, bytes)
		return MemoryAdmissionLease{}, err
	}
	controller.activate(class, bytes)
	released := uint32(0)
	return MemoryAdmissionLease{
		controller: controller,
		admission:  admissionLease,
		class:      class,
		bytes:      bytes,
		released:   &released,
	}, nil
}

// Release returns the memory reservation and concurrency slot. It is safe to
// call more than once.
func (lease *MemoryAdmissionLease) Release() {
	if lease == nil || lease.controller == nil || lease.released == nil {
		return
	}
	if !atomic.CompareAndSwapUint32(lease.released, 0, 1) {
		return
	}
	lease.controller.releaseReservation(lease.class, lease.bytes)
	lease.admission.Release()
}

// Class returns the normalized class name associated with the lease.
func (lease *MemoryAdmissionLease) Class() string {
	if lease == nil {
		return ""
	}
	return lease.class
}

// MemoryBytes returns the reserved bytes associated with the lease.
func (lease *MemoryAdmissionLease) MemoryBytes() uint64 {
	if lease == nil {
		return 0
	}
	return lease.bytes
}

// Snapshot returns a copy of controller state. Classes are sorted by name in
// the embedded admission snapshot and the same order is used here.
func (controller *MemoryAdmissionController) Snapshot() MemoryAdmissionSnapshot {
	if controller == nil || controller.admission == nil {
		return MemoryAdmissionSnapshot{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	admissionSnapshot := controller.admission.Snapshot()
	snapshot := MemoryAdmissionSnapshot{
		Admission:           admissionSnapshot,
		AdmissionSnapshot:   admissionSnapshot,
		ReservedMemoryBytes: controller.reserved,
		ActiveMemoryBytes:   controller.active,
		Classes:             make([]MemoryClassSnapshot, 0, len(admissionSnapshot.Classes)),
	}
	for _, classSnapshot := range admissionSnapshot.Classes {
		state := controller.classes[classSnapshot.Name]
		if state == nil {
			continue
		}
		snapshot.Classes = append(snapshot.Classes, MemoryClassSnapshot{
			ClassSnapshot:       classSnapshot,
			MaxMemoryBytes:      state.MaxMemoryBytes,
			ReservedMemoryBytes: state.reservedMemoryBytes,
			ActiveMemoryBytes:   state.activeMemoryBytes,
		})
	}
	return snapshot
}

func (controller *MemoryAdmissionController) waitForMemory(ctx context.Context, waiter *memoryAdmissionWaiter) error {
	select {
	case <-waiter.done:
		controller.mu.Lock()
		granted := waiter.granted
		controller.mu.Unlock()
		if granted {
			return nil
		}
		return ctx.Err()
	case <-ctx.Done():
		controller.mu.Lock()
		if waiter.granted {
			controller.mu.Unlock()
			return nil
		}
		if controller.removeWaiterLocked(waiter) {
			controller.dispatchLocked()
			controller.mu.Unlock()
			return ctx.Err()
		}
		granted := waiter.granted
		controller.mu.Unlock()
		if granted {
			return nil
		}
		return ctx.Err()
	}
}

func (controller *MemoryAdmissionController) canReserveLocked(state *memoryAdmissionClass, bytes uint64) (bool, error) {
	if bytes > 0 && controller.reserved > math.MaxUint64-bytes {
		return false, ErrMemoryAdmissionOverflow
	}
	if state.MaxMemoryBytes != 0 && state.reservedMemoryBytes > state.MaxMemoryBytes-bytes {
		return false, nil
	}
	return true, nil
}

func (controller *MemoryAdmissionController) reserveLocked(state *memoryAdmissionClass, bytes uint64) {
	state.reservedMemoryBytes += bytes
	controller.reserved += bytes
}

func (controller *MemoryAdmissionController) activate(class string, bytes uint64) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	state := controller.classes[class]
	if state == nil {
		return
	}
	state.activeMemoryBytes += bytes
	controller.active += bytes
}

func (controller *MemoryAdmissionController) releaseReservation(class string, bytes uint64) {
	controller.mu.Lock()
	state := controller.classes[class]
	if state != nil {
		if state.activeMemoryBytes >= bytes {
			state.activeMemoryBytes -= bytes
		}
		if state.reservedMemoryBytes >= bytes {
			state.reservedMemoryBytes -= bytes
		}
	}
	if controller.active >= bytes {
		controller.active -= bytes
	}
	if controller.reserved >= bytes {
		controller.reserved -= bytes
	}
	controller.dispatchLocked()
	controller.mu.Unlock()
}

func (controller *MemoryAdmissionController) dispatchLocked() {
	for index := 0; index < len(controller.waiters); {
		waiter := controller.waiters[index]
		state := controller.classes[waiter.class]
		if state == nil {
			controller.waiters = append(controller.waiters[:index], controller.waiters[index+1:]...)
			continue
		}
		canReserve, err := controller.canReserveLocked(state, waiter.bytes)
		if err != nil || !canReserve {
			index++
			continue
		}
		controller.reserveLocked(state, waiter.bytes)
		waiter.granted = true
		controller.waiters = append(controller.waiters[:index], controller.waiters[index+1:]...)
		close(waiter.done)
	}
}

func (controller *MemoryAdmissionController) removeWaiterLocked(target *memoryAdmissionWaiter) bool {
	for index, waiter := range controller.waiters {
		if waiter != target {
			continue
		}
		controller.waiters = append(controller.waiters[:index], controller.waiters[index+1:]...)
		return true
	}
	return false
}
