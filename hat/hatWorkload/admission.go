// Package hatWorkload provides opt-in admission control for named workload
// classes. It is transport- and storage-independent so callers can use the
// same priority policy for queries, background maintenance, or sinks.
package hatWorkload

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

var (
	ErrAdmissionControllerNil  = errors.New("hatWorkload: admission controller is nil")
	ErrAdmissionContextNil     = errors.New("hatWorkload: admission context is nil")
	ErrAdmissionOptionsInvalid = errors.New("hatWorkload: admission options are invalid")
	ErrAdmissionClassInvalid   = errors.New("hatWorkload: workload class is invalid")
	ErrAdmissionClassDuplicate = errors.New("hatWorkload: workload class already exists")
	ErrAdmissionClassNotFound  = errors.New("hatWorkload: workload class was not found")
	ErrAdmissionQueueFull      = errors.New("hatWorkload: admission queue is full")
)

const (
	DefaultAdmissionMaxInFlight   = 64
	DefaultAdmissionMaxQueued     = 1024
	DefaultAdmissionMaxClasses    = 64
	DefaultAdmissionPriorityBurst = 8
	MaxAdmissionMaxInFlight       = 1 << 20
	MaxAdmissionMaxQueued         = 1 << 20
	MaxAdmissionMaxClasses        = 1 << 16
	MaxAdmissionPriorityBurst     = 1 << 16
	maxAdmissionClassNameBytes    = 128
)

// AdmissionOptions bounds controller-wide state. Zero values select bounded
// defaults. PriorityBurst limits consecutive grants to higher-priority work
// while a lower-priority waiter is present, preventing starvation.
type AdmissionOptions struct {
	MaxInFlight   int
	MaxQueued     int
	MaxClasses    int
	PriorityBurst int
}

// ClassOptions describes one named workload class. A zero MaxInFlight uses the
// controller-wide limit. Larger numeric Priority values run first.
type ClassOptions struct {
	Name        string
	Priority    int
	MaxInFlight int
}

// ClassSnapshot is an immutable point-in-time class report.
type ClassSnapshot struct {
	Name        string `json:"name"`
	Priority    int    `json:"priority"`
	MaxInFlight int    `json:"max_in_flight"`
	Active      int    `json:"active"`
	Queued      int    `json:"queued"`
}

// AdmissionSnapshot is an immutable point-in-time controller report.
type AdmissionSnapshot struct {
	Active  int             `json:"active"`
	Queued  int             `json:"queued"`
	Classes []ClassSnapshot `json:"classes"`
}

type workloadClass struct {
	ClassOptions
	active int
}

type admissionWaiter struct {
	class    string
	priority int
	sequence uint64
	done     chan struct{}
	granted  bool
}

// AdmissionController coordinates bounded work across named classes.
// RegisterClass and Snapshot are safe to call concurrently with Acquire and
// Release. A zero controller is not usable; construct one with the factory.
type AdmissionController struct {
	mu sync.Mutex

	maxInFlight   int
	maxQueued     int
	maxClasses    int
	priorityBurst int
	active        int
	burstCount    int
	sequence      uint64
	classes       map[string]*workloadClass
	waiters       []*admissionWaiter
}

// AdmissionLease represents one admitted unit of work. Release is safe and
// idempotent; callers should release it as soon as the work completes.
type AdmissionLease struct {
	controller *AdmissionController
	class      string
	released   *uint32
}

// NewAdmissionController creates a bounded, opt-in workload controller.
func NewAdmissionController(options AdmissionOptions) (*AdmissionController, error) {
	maxInFlight := options.MaxInFlight
	if maxInFlight == 0 {
		maxInFlight = DefaultAdmissionMaxInFlight
	}
	maxQueued := options.MaxQueued
	if maxQueued == 0 {
		maxQueued = DefaultAdmissionMaxQueued
	}
	maxClasses := options.MaxClasses
	if maxClasses == 0 {
		maxClasses = DefaultAdmissionMaxClasses
	}
	priorityBurst := options.PriorityBurst
	if priorityBurst == 0 {
		priorityBurst = DefaultAdmissionPriorityBurst
	}
	if maxInFlight < 1 || maxInFlight > MaxAdmissionMaxInFlight ||
		maxQueued < 1 || maxQueued > MaxAdmissionMaxQueued ||
		maxClasses < 1 || maxClasses > MaxAdmissionMaxClasses ||
		priorityBurst < 1 || priorityBurst > MaxAdmissionPriorityBurst {
		return nil, ErrAdmissionOptionsInvalid
	}
	return &AdmissionController{
		maxInFlight:   maxInFlight,
		maxQueued:     maxQueued,
		maxClasses:    maxClasses,
		priorityBurst: priorityBurst,
		classes:       make(map[string]*workloadClass),
	}, nil
}

// RegisterClass adds one workload class. Names are trimmed and retained as
// their normalized form; duplicate registrations are rejected.
func (controller *AdmissionController) RegisterClass(options ClassOptions) error {
	if controller == nil {
		return ErrAdmissionControllerNil
	}
	name, err := normalizeClassName(options.Name)
	if err != nil {
		return err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if len(controller.classes) >= controller.maxClasses {
		return ErrAdmissionOptionsInvalid
	}
	if _, exists := controller.classes[name]; exists {
		return ErrAdmissionClassDuplicate
	}
	if options.MaxInFlight < 0 || options.MaxInFlight > MaxAdmissionMaxInFlight {
		return ErrAdmissionOptionsInvalid
	}
	if options.MaxInFlight == 0 {
		options.MaxInFlight = controller.maxInFlight
	}
	options.Name = name
	controller.classes[name] = &workloadClass{ClassOptions: options}
	controller.dispatchLocked()
	return nil
}

// Acquire waits until class has capacity and the priority scheduler admits
// the request. Context cancellation removes a queued request without
// consuming capacity. If cancellation races with admission, a granted lease
// wins and must be released by the caller.
func (controller *AdmissionController) Acquire(ctx context.Context, class string) (AdmissionLease, error) {
	if controller == nil {
		return AdmissionLease{}, ErrAdmissionControllerNil
	}
	if ctx == nil {
		return AdmissionLease{}, ErrAdmissionContextNil
	}
	if err := ctx.Err(); err != nil {
		return AdmissionLease{}, err
	}
	class, err := normalizeClassName(class)
	if err != nil {
		return AdmissionLease{}, err
	}
	controller.mu.Lock()
	classState, exists := controller.classes[class]
	if !exists {
		controller.mu.Unlock()
		return AdmissionLease{}, ErrAdmissionClassNotFound
	}
	if len(controller.waiters) == 0 && controller.active < controller.maxInFlight && classState.active < classState.MaxInFlight {
		classState.active++
		controller.active++
		controller.mu.Unlock()
		return newAdmissionLease(controller, class), nil
	}
	if len(controller.waiters) >= controller.maxQueued {
		controller.mu.Unlock()
		return AdmissionLease{}, ErrAdmissionQueueFull
	}
	controller.sequence++
	work := &admissionWaiter{
		class:    class,
		priority: controller.classes[class].Priority,
		sequence: controller.sequence,
		done:     make(chan struct{}),
	}
	controller.waiters = append(controller.waiters, work)
	controller.dispatchLocked()
	if work.granted {
		controller.mu.Unlock()
		return newAdmissionLease(controller, class), nil
	}
	controller.mu.Unlock()

	select {
	case <-work.done:
		controller.mu.Lock()
		granted := work.granted
		controller.mu.Unlock()
		if granted {
			return newAdmissionLease(controller, class), nil
		}
		return AdmissionLease{}, ctx.Err()
	case <-ctx.Done():
		controller.mu.Lock()
		if work.granted {
			controller.mu.Unlock()
			return newAdmissionLease(controller, class), nil
		}
		if controller.removeWaiterLocked(work) {
			controller.dispatchLocked()
			controller.mu.Unlock()
			return AdmissionLease{}, ctx.Err()
		}
		granted := work.granted
		controller.mu.Unlock()
		if granted {
			return newAdmissionLease(controller, class), nil
		}
		return AdmissionLease{}, ctx.Err()
	}
}

// Release returns an admitted slot. It is safe to call more than once.
func (lease *AdmissionLease) Release() {
	if lease == nil || lease.controller == nil || lease.released == nil {
		return
	}
	if !atomic.CompareAndSwapUint32(lease.released, 0, 1) {
		return
	}
	controller := lease.controller
	controller.mu.Lock()
	if class := controller.classes[lease.class]; class != nil && class.active > 0 {
		class.active--
		if controller.active > 0 {
			controller.active--
		}
	}
	controller.dispatchLocked()
	controller.mu.Unlock()
}

// Class returns the normalized class name associated with the lease.
func (lease *AdmissionLease) Class() string {
	if lease == nil {
		return ""
	}
	return lease.class
}

func newAdmissionLease(controller *AdmissionController, class string) AdmissionLease {
	released := uint32(0)
	return AdmissionLease{controller: controller, class: class, released: &released}
}

// Snapshot returns classes in normalized name order and copies all state.
func (controller *AdmissionController) Snapshot() AdmissionSnapshot {
	if controller == nil {
		return AdmissionSnapshot{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	snapshot := AdmissionSnapshot{Active: controller.active, Queued: len(controller.waiters), Classes: make([]ClassSnapshot, 0, len(controller.classes))}
	for _, class := range controller.classes {
		snapshot.Classes = append(snapshot.Classes, ClassSnapshot{
			Name:        class.Name,
			Priority:    class.Priority,
			MaxInFlight: class.MaxInFlight,
			Active:      class.active,
		})
	}
	queuedByClass := make(map[string]int, len(snapshot.Classes))
	for _, waiter := range controller.waiters {
		queuedByClass[waiter.class]++
	}
	for index := range snapshot.Classes {
		snapshot.Classes[index].Queued = queuedByClass[snapshot.Classes[index].Name]
	}
	sort.Slice(snapshot.Classes, func(left, right int) bool { return snapshot.Classes[left].Name < snapshot.Classes[right].Name })
	return snapshot
}

func (controller *AdmissionController) dispatchLocked() {
	for controller.active < controller.maxInFlight {
		index := controller.nextWaiterLocked()
		if index < 0 {
			return
		}
		highestPriority := controller.highestEligiblePriorityLocked()
		waiter := controller.waiters[index]
		controller.waiters = append(controller.waiters[:index], controller.waiters[index+1:]...)
		class := controller.classes[waiter.class]
		class.active++
		controller.active++
		waiter.granted = true
		controller.updateBurstLocked(waiter.priority, highestPriority)
		close(waiter.done)
	}
}

func (controller *AdmissionController) nextWaiterLocked() int {
	best := -1
	highestPriority := controller.highestEligiblePriorityLocked()
	for index, waiter := range controller.waiters {
		class := controller.classes[waiter.class]
		if class == nil || class.active >= class.MaxInFlight {
			continue
		}
		if best < 0 || waiter.priority > highestPriority ||
			(waiter.priority == highestPriority && waiter.sequence < controller.waiters[best].sequence) {
			best = index
			highestPriority = waiter.priority
		}
	}
	if best < 0 || controller.burstCount < controller.priorityBurst {
		return best
	}
	for index, waiter := range controller.waiters {
		class := controller.classes[waiter.class]
		if class == nil || class.active >= class.MaxInFlight || waiter.priority >= highestPriority {
			continue
		}
		if best < 0 || waiter.sequence < controller.waiters[best].sequence || controller.waiters[best].priority >= highestPriority {
			best = index
		}
	}
	return best
}

func (controller *AdmissionController) highestEligiblePriorityLocked() int {
	highest := 0
	found := false
	for _, waiter := range controller.waiters {
		class := controller.classes[waiter.class]
		if class == nil || class.active >= class.MaxInFlight {
			continue
		}
		if !found || waiter.priority > highest {
			highest = waiter.priority
			found = true
		}
	}
	return highest
}

func (controller *AdmissionController) updateBurstLocked(priority, highestPriority int) {
	if priority < highestPriority {
		controller.burstCount = 0
		return
	}
	for _, waiter := range controller.waiters {
		if waiter.priority < priority {
			if controller.burstCount < controller.priorityBurst {
				controller.burstCount++
			}
			return
		}
	}
	controller.burstCount = 0
}

func (controller *AdmissionController) removeWaiterLocked(target *admissionWaiter) bool {
	for index, waiter := range controller.waiters {
		if waiter != target {
			continue
		}
		controller.waiters = append(controller.waiters[:index], controller.waiters[index+1:]...)
		return true
	}
	return false
}

func normalizeClassName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxAdmissionClassNameBytes || !utf8.ValidString(name) {
		return "", ErrAdmissionClassInvalid
	}
	return name, nil
}
