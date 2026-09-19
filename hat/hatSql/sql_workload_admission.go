package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrSQLWorkloadAdmissionNil reports a method call on a nil controller.
	ErrSQLWorkloadAdmissionNil = errors.New("hatSql: workload admission controller is nil")
	// ErrSQLWorkloadAdmissionOptionsInvalid reports invalid controller limits or classes.
	ErrSQLWorkloadAdmissionOptionsInvalid = errors.New("hatSql: workload admission options are invalid")
	// ErrSQLWorkloadAdmissionClassInvalid reports an unknown or empty workload class.
	ErrSQLWorkloadAdmissionClassInvalid = errors.New("hatSql: workload admission class is invalid")
	// ErrSQLWorkloadAdmissionRunInvalid reports a missing admitted callback.
	ErrSQLWorkloadAdmissionRunInvalid = errors.New("hatSql: workload admission callback is invalid")
	// ErrSQLWorkloadAdmissionClosed reports an admission controller that has stopped accepting work.
	ErrSQLWorkloadAdmissionClosed = errors.New("hatSql: workload admission controller is closed")
)

const (
	// DefaultSQLWorkloadAdmissionMaxConcurrent keeps opt-in admission serialized
	// unless the caller explicitly allows concurrent work.
	DefaultSQLWorkloadAdmissionMaxConcurrent = 1
	// DefaultSQLWorkloadAdmissionMaxPending bounds queued work and retained
	// request metadata.
	DefaultSQLWorkloadAdmissionMaxPending = 1024
	// DefaultSQLWorkloadAdmissionMaxPriorityBurst gives lower-priority work a
	// turn even while a higher-priority class remains busy.
	DefaultSQLWorkloadAdmissionMaxPriorityBurst = 8
)

// SQLWorkloadClass describes one caller-defined admission class. Higher
// Priority values are selected first. Weight controls service share among
// queued classes with the same priority; zero selects weight one.
type SQLWorkloadClass struct {
	Name     string
	Priority int
	Weight   uint32
}

// SQLWorkloadAdmissionOptions configures an opt-in bounded admission
// controller. MaxPending is the total queued-request limit, not a per-class
// limit. A caller waiting while the queue is full can still cancel its context.
type SQLWorkloadAdmissionOptions struct {
	MaxConcurrent    int
	MaxPending       int
	MaxPriorityBurst int
	Classes          []SQLWorkloadClass
}

// SQLWorkloadAdmissionStats is a point-in-time controller snapshot.
type SQLWorkloadAdmissionStats struct {
	Active        int
	Pending       int
	MaxConcurrent int
	MaxPending    int
	Admitted      uint64
	Canceled      uint64
	Closed        bool
}

type sqlWorkloadAdmissionClass struct {
	SQLWorkloadClass
	weight  uint64
	deficit uint64
	pending []*sqlWorkloadAdmissionRequest
	order   int
}

type sqlWorkloadAdmissionRequest struct {
	class     *sqlWorkloadAdmissionClass
	completed chan struct{}
	admitted  bool
	err       error
}

// SQLWorkloadAdmission provides bounded, caller-driven admission for queries
// or other SQL work. It has no background goroutine; releasing a permit
// dispatches queued work synchronously, which keeps shutdown and cancellation
// deterministic.
type SQLWorkloadAdmission struct {
	mu sync.Mutex

	maxConcurrent    int
	maxPending       int
	maxPriorityBurst int
	classes          map[string]*sqlWorkloadAdmissionClass
	classOrder       []*sqlWorkloadAdmissionClass

	active                 int
	pending                int
	admitted               uint64
	canceled               uint64
	closed                 bool
	notify                 chan struct{}
	classCursor            int
	consecutivePriority    int
	consecutivePriorityRun int
}

// NewSQLWorkloadAdmission validates options and returns an opt-in admission
// controller. The implicit "default" class is available unless the caller
// provides its own definition.
func NewSQLWorkloadAdmission(options SQLWorkloadAdmissionOptions) (*SQLWorkloadAdmission, error) {
	if options.MaxConcurrent < 0 || options.MaxPending < 0 || options.MaxPriorityBurst < 0 {
		return nil, ErrSQLWorkloadAdmissionOptionsInvalid
	}
	if options.MaxConcurrent == 0 {
		options.MaxConcurrent = DefaultSQLWorkloadAdmissionMaxConcurrent
	}
	if options.MaxPending == 0 {
		options.MaxPending = DefaultSQLWorkloadAdmissionMaxPending
	}
	if options.MaxPriorityBurst == 0 {
		options.MaxPriorityBurst = DefaultSQLWorkloadAdmissionMaxPriorityBurst
	}
	admission := &SQLWorkloadAdmission{
		maxConcurrent:    options.MaxConcurrent,
		maxPending:       options.MaxPending,
		maxPriorityBurst: options.MaxPriorityBurst,
		classes:          make(map[string]*sqlWorkloadAdmissionClass, len(options.Classes)+1),
		notify:           make(chan struct{}),
	}
	for order, definition := range options.Classes {
		name := normalizeSQLWorkloadClassName(definition.Name)
		if name == "" || admission.classes[name] != nil {
			return nil, fmt.Errorf("%w: %q", ErrSQLWorkloadAdmissionOptionsInvalid, definition.Name)
		}
		weight := uint64(definition.Weight)
		if weight == 0 {
			weight = 1
		}
		class := &sqlWorkloadAdmissionClass{
			SQLWorkloadClass: SQLWorkloadClass{Name: name, Priority: definition.Priority, Weight: uint32(weight)},
			weight:           weight,
			order:            order,
		}
		admission.classes[name] = class
		admission.classOrder = append(admission.classOrder, class)
	}
	if admission.classes["default"] == nil {
		class := &sqlWorkloadAdmissionClass{
			SQLWorkloadClass: SQLWorkloadClass{Name: "default", Weight: 1},
			weight:           1,
			order:            len(admission.classOrder),
		}
		admission.classes[class.Name] = class
		admission.classOrder = append(admission.classOrder, class)
	}
	return admission, nil
}

// Acquire waits until class can start work and returns an idempotent release
// function. Context cancellation removes a queued request without consuming a
// permit. A request admitted concurrently with cancellation is considered a
// successful acquire and must still be released.
func (admission *SQLWorkloadAdmission) Acquire(ctx context.Context, className string) (func(), error) {
	if err := admission.acquirePermit(ctx, className); err != nil {
		return nil, err
	}
	return admission.releaseFunc(), nil
}

func (admission *SQLWorkloadAdmission) acquirePermit(ctx context.Context, className string) error {
	if admission == nil {
		return ErrSQLWorkloadAdmissionNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	className = normalizeSQLWorkloadClassName(className)
	if className == "" {
		return ErrSQLWorkloadAdmissionClassInvalid
	}

	for {
		admission.mu.Lock()
		if admission.closed {
			admission.mu.Unlock()
			return ErrSQLWorkloadAdmissionClosed
		}
		class := admission.classes[className]
		if class == nil {
			admission.mu.Unlock()
			return fmt.Errorf("%w: %s", ErrSQLWorkloadAdmissionClassInvalid, className)
		}
		if admission.active < admission.maxConcurrent && admission.pending == 0 {
			admission.active++
			admission.admitted++
			admission.mu.Unlock()
			return nil
		}
		if admission.pending >= admission.maxPending {
			wait := admission.notify
			admission.mu.Unlock()
			select {
			case <-ctx.Done():
				admission.mu.Lock()
				admission.canceled++
				admission.mu.Unlock()
				return ctx.Err()
			case <-wait:
				continue
			}
		}
		request := &sqlWorkloadAdmissionRequest{
			class:     class,
			completed: make(chan struct{}),
		}
		class.pending = append(class.pending, request)
		admission.pending++
		admission.mu.Unlock()

		select {
		case <-request.completed:
			if request.err != nil {
				return request.err
			}
			return nil
		case <-ctx.Done():
			admission.mu.Lock()
			if request.admitted {
				admission.mu.Unlock()
				return nil
			}
			if request.err != nil {
				err := request.err
				admission.mu.Unlock()
				return err
			}
			if admission.removeRequestLocked(request) {
				request.err = ctx.Err()
				admission.canceled++
				admission.signalLocked()
			}
			err := request.err
			admission.mu.Unlock()
			if err == nil {
				err = ctx.Err()
			}
			return err
		}
	}
}

// Run acquires a class permit, invokes run, and releases the permit even when
// run returns an error. It is the simplest integration point for HTTP, gRPC,
// or application-owned SQL execution handlers.
func (admission *SQLWorkloadAdmission) Run(ctx context.Context, className string, run func(context.Context) error) error {
	if admission == nil {
		return ErrSQLWorkloadAdmissionNil
	}
	if run == nil {
		return ErrSQLWorkloadAdmissionRunInvalid
	}
	if err := admission.acquirePermit(ctx, className); err != nil {
		return err
	}
	defer admission.releaseOne()
	return run(ctx)
}

// Stats returns a bounded point-in-time snapshot. Nil receivers return the
// zero value so monitoring code can report an absent optional controller.
func (admission *SQLWorkloadAdmission) Stats() SQLWorkloadAdmissionStats {
	if admission == nil {
		return SQLWorkloadAdmissionStats{}
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return SQLWorkloadAdmissionStats{
		Active:        admission.active,
		Pending:       admission.pending,
		MaxConcurrent: admission.maxConcurrent,
		MaxPending:    admission.maxPending,
		Admitted:      admission.admitted,
		Canceled:      admission.canceled,
		Closed:        admission.closed,
	}
}

// Close rejects new work and cancels all queued requests. Active permits are
// still valid and their release functions remain safe after close.
func (admission *SQLWorkloadAdmission) Close() {
	if admission == nil {
		return
	}
	admission.mu.Lock()
	if admission.closed {
		admission.mu.Unlock()
		return
	}
	admission.closed = true
	for _, class := range admission.classOrder {
		for _, request := range class.pending {
			request.err = ErrSQLWorkloadAdmissionClosed
			admission.canceled++
			close(request.completed)
		}
		class.pending = nil
	}
	admission.pending = 0
	admission.signalLocked()
	admission.mu.Unlock()
}

func (admission *SQLWorkloadAdmission) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			admission.releaseOne()
		})
	}
}

func (admission *SQLWorkloadAdmission) releaseOne() {
	admission.mu.Lock()
	hadPending := admission.pending > 0
	if admission.active > 0 {
		admission.active--
	}
	admission.dispatchLocked()
	if hadPending {
		admission.signalLocked()
	}
	admission.mu.Unlock()
}

func (admission *SQLWorkloadAdmission) dispatchLocked() {
	for !admission.closed && admission.active < admission.maxConcurrent && admission.pending > 0 {
		class := admission.nextClassLocked()
		if class == nil || len(class.pending) == 0 {
			return
		}
		request := class.pending[0]
		class.pending = class.pending[1:]
		admission.pending--
		request.admitted = true
		admission.active++
		admission.admitted++
		admission.recordPriorityAdmissionLocked(class.Priority)
		close(request.completed)
	}
}

func (admission *SQLWorkloadAdmission) nextClassLocked() *sqlWorkloadAdmissionClass {
	highestPriority := 0
	haveClass := false
	for _, class := range admission.classOrder {
		if len(class.pending) == 0 {
			continue
		}
		if !haveClass || class.Priority > highestPriority {
			highestPriority = class.Priority
			haveClass = true
		}
	}
	if !haveClass {
		return nil
	}
	targetPriority := highestPriority
	if admission.consecutivePriorityRun >= admission.maxPriorityBurst {
		for _, class := range admission.classOrder {
			if len(class.pending) == 0 || class.Priority >= highestPriority {
				continue
			}
			if targetPriority == highestPriority || class.Priority > targetPriority {
				targetPriority = class.Priority
			}
		}
	}

	var selected *sqlWorkloadAdmissionClass
	var totalWeight uint64
	for _, class := range admission.classOrder {
		if len(class.pending) == 0 || class.Priority != targetPriority {
			continue
		}
		class.deficit += class.weight
		totalWeight += class.weight
		if selected == nil || class.deficit > selected.deficit {
			selected = class
		}
	}
	if selected != nil {
		if selected.deficit >= totalWeight {
			selected.deficit -= totalWeight
		} else {
			selected.deficit = 0
		}
	}
	return selected
}

func (admission *SQLWorkloadAdmission) recordPriorityAdmissionLocked(priority int) {
	if admission.consecutivePriority == priority {
		admission.consecutivePriorityRun++
		return
	}
	admission.consecutivePriority = priority
	admission.consecutivePriorityRun = 1
}

func (admission *SQLWorkloadAdmission) removeRequestLocked(target *sqlWorkloadAdmissionRequest) bool {
	for _, class := range admission.classOrder {
		for index, request := range class.pending {
			if request != target {
				continue
			}
			copy(class.pending[index:], class.pending[index+1:])
			class.pending[len(class.pending)-1] = nil
			class.pending = class.pending[:len(class.pending)-1]
			admission.pending--
			return true
		}
	}
	return false
}

func (admission *SQLWorkloadAdmission) signalLocked() {
	close(admission.notify)
	admission.notify = make(chan struct{})
}

func normalizeSQLWorkloadClassName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
