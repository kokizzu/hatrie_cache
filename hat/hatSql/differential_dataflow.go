package hatSql

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrDifferentialDataflowNil indicates an operation on a nil flow.
	ErrDifferentialDataflowNil = errors.New("hatSql: differential dataflow is nil")
	// ErrDifferentialDataflowPolicyInvalid indicates an unknown correction or
	// frontier mode.
	ErrDifferentialDataflowPolicyInvalid = errors.New("hatSql: differential dataflow policy is invalid")
	// ErrDifferentialDataflowSinkRequired indicates that no maintained sink was
	// supplied.
	ErrDifferentialDataflowSinkRequired = errors.New("hatSql: differential dataflow sink is required")
	// ErrDifferentialDataflowTooLate indicates a rejected too-late update.
	ErrDifferentialDataflowTooLate = errors.New("hatSql: differential dataflow update is too late")
	// ErrDifferentialDataflowFrontierRegression indicates a backwards frontier.
	ErrDifferentialDataflowFrontierRegression = errors.New("hatSql: differential dataflow frontier moved backwards")
	// ErrDifferentialDataflowStatsOverflow indicates that an operator counter
	// could not represent another successful update.
	ErrDifferentialDataflowStatsOverflow = errors.New("hatSql: differential dataflow stats overflow")
)

// DifferentialDataflowCorrection controls updates beyond AllowedLateness.
// Updates inside the allowed lateness are always accepted as corrections.
type DifferentialDataflowCorrection uint8

const (
	// DifferentialDataflowAccept retains too-late updates for correction.
	DifferentialDataflowAccept DifferentialDataflowCorrection = iota
	// DifferentialDataflowDrop discards too-late updates and advances the
	// frontier normally.
	DifferentialDataflowDrop
	// DifferentialDataflowReject rejects the whole batch containing a too-late
	// update without calling the sink.
	DifferentialDataflowReject
)

// DifferentialDataflowFrontier controls automatic frontier advancement.
type DifferentialDataflowFrontier uint8

const (
	// DifferentialDataflowManual requires callers to use Advance.
	DifferentialDataflowManual DifferentialDataflowFrontier = iota
	// DifferentialDataflowBatchMax advances to the greatest input time after a
	// successful batch. Manual Advance calls remain valid.
	DifferentialDataflowBatchMax
)

// DifferentialDataflowPolicy declares lateness, correction, and frontier
// behavior as one operator policy. Time values use DifferentialRow.Time units.
type DifferentialDataflowPolicy struct {
	AllowedLateness uint64
	Correction      DifferentialDataflowCorrection
	Frontier        DifferentialDataflowFrontier
}

// DifferentialDataflowSink receives the accepted, detached batch. It must be
// batch-atomic: returning an error must leave the maintained object unchanged.
type DifferentialDataflowSink func([]DifferentialRow) error

// DifferentialDataflowOptions configures one policy gate around a maintained
// differential operator.
type DifferentialDataflowOptions struct {
	Policy          DifferentialDataflowPolicy
	InitialFrontier uint64
	Sink            DifferentialDataflowSink
}

// DifferentialDataflowStats reports policy decisions and committed batches.
// TooLateRows is a subset of LateRows. AcceptedLateRows includes too-late
// rows when the correction mode is DifferentialDataflowAccept.
type DifferentialDataflowStats struct {
	Frontier         uint64
	AppliedBatches   uint64
	AcceptedRows     uint64
	LateRows         uint64
	AcceptedLateRows uint64
	TooLateRows      uint64
	DroppedRows      uint64
	RejectedBatches  uint64
	RejectedRows     uint64
	SinkErrors       uint64
}

// DifferentialDataflow couples a declared late-data policy to one maintained
// sink. It is safe for concurrent Apply, Advance, Frontier, and Stats calls.
type DifferentialDataflow struct {
	mu       sync.Mutex
	policy   DifferentialDataflowPolicy
	frontier uint64
	sink     DifferentialDataflowSink
	stats    DifferentialDataflowStats
}

// NewDifferentialDataflow creates a policy gate. The sink is required so an
// accepted batch cannot be silently discarded.
func NewDifferentialDataflow(options DifferentialDataflowOptions) (*DifferentialDataflow, error) {
	if options.Policy.Correction > DifferentialDataflowReject || options.Policy.Frontier > DifferentialDataflowBatchMax {
		return nil, ErrDifferentialDataflowPolicyInvalid
	}
	if options.Sink == nil {
		return nil, ErrDifferentialDataflowSinkRequired
	}
	return &DifferentialDataflow{
		policy:   options.Policy,
		frontier: options.InitialFrontier,
		sink:     options.Sink,
		stats: DifferentialDataflowStats{
			Frontier: options.InitialFrontier,
		},
	}, nil
}

// Apply classifies one batch and sends accepted updates to the sink. Policy
// rejection is atomic: the sink is not called and the frontier does not move.
// A sink error likewise leaves the batch and frontier uncommitted.
func (flow *DifferentialDataflow) Apply(rows []DifferentialRow) error {
	if flow == nil {
		return ErrDifferentialDataflowNil
	}
	if len(rows) == 0 {
		return nil
	}
	flow.mu.Lock()
	defer flow.mu.Unlock()

	accepted := make([]DifferentialRow, 0, len(rows))
	var lateRows, acceptedLateRows, tooLateRows, droppedRows, rejectedRows uint64
	var firstTooLate DifferentialRow
	hasTooLate := false
	maxTime := flow.frontier
	for index, row := range rows {
		if index == 0 || row.Time > maxTime {
			maxTime = row.Time
		}
		if row.Time >= flow.frontier {
			accepted = append(accepted, cloneDifferentialRowUpdate(row))
			continue
		}
		lateRows++
		if flow.frontier-row.Time <= flow.policy.AllowedLateness {
			acceptedLateRows++
			accepted = append(accepted, cloneDifferentialRowUpdate(row))
			continue
		}
		tooLateRows++
		switch flow.policy.Correction {
		case DifferentialDataflowAccept:
			acceptedLateRows++
			accepted = append(accepted, cloneDifferentialRowUpdate(row))
		case DifferentialDataflowDrop:
			droppedRows++
		case DifferentialDataflowReject:
			rejectedRows++
			if !hasTooLate {
				firstTooLate = row
				hasTooLate = true
			}
		}
	}

	nextStats := flow.stats
	if err := addDifferentialDataflowCounter(&nextStats.LateRows, lateRows); err != nil {
		return err
	}
	if err := addDifferentialDataflowCounter(&nextStats.TooLateRows, tooLateRows); err != nil {
		return err
	}
	if hasTooLate && flow.policy.Correction == DifferentialDataflowReject {
		if err := addDifferentialDataflowCounter(&nextStats.RejectedBatches, 1); err != nil {
			return err
		}
		if err := addDifferentialDataflowCounter(&nextStats.RejectedRows, rejectedRows); err != nil {
			return err
		}
		nextStats.Frontier = flow.frontier
		flow.stats = nextStats
		return fmt.Errorf("key %q at time %d: %w", firstTooLate.Key, firstTooLate.Time, ErrDifferentialDataflowTooLate)
	}
	if err := addDifferentialDataflowCounter(&nextStats.AcceptedLateRows, acceptedLateRows); err != nil {
		return err
	}
	if err := addDifferentialDataflowCounter(&nextStats.DroppedRows, droppedRows); err != nil {
		return err
	}
	if err := addDifferentialDataflowCounter(&nextStats.AcceptedRows, uint64(len(accepted))); err != nil {
		return err
	}
	if err := addDifferentialDataflowCounter(&nextStats.AppliedBatches, 1); err != nil {
		return err
	}

	if len(accepted) > 0 {
		if err := flow.sink(accepted); err != nil {
			if addErr := addDifferentialDataflowCounter(&flow.stats.SinkErrors, 1); addErr != nil {
				return addErr
			}
			return fmt.Errorf("differential dataflow sink: %w", err)
		}
	}
	nextFrontier := flow.frontier
	if flow.policy.Frontier == DifferentialDataflowBatchMax && maxTime > nextFrontier {
		nextFrontier = maxTime
	}
	nextStats.Frontier = nextFrontier
	flow.frontier = nextFrontier
	flow.stats = nextStats
	return nil
}

// Advance moves the frontier monotonically. In BatchMax mode, successful
// Apply calls may move it further to the batch maximum.
func (flow *DifferentialDataflow) Advance(frontier uint64) error {
	if flow == nil {
		return ErrDifferentialDataflowNil
	}
	flow.mu.Lock()
	defer flow.mu.Unlock()
	if frontier < flow.frontier {
		return fmt.Errorf("frontier %d follows %d: %w", frontier, flow.frontier, ErrDifferentialDataflowFrontierRegression)
	}
	flow.frontier = frontier
	flow.stats.Frontier = frontier
	return nil
}

// Frontier returns the current logical frontier. A nil flow reports zero.
func (flow *DifferentialDataflow) Frontier() uint64 {
	if flow == nil {
		return 0
	}
	flow.mu.Lock()
	defer flow.mu.Unlock()
	return flow.frontier
}

// Stats returns an independent snapshot of policy decisions.
func (flow *DifferentialDataflow) Stats() DifferentialDataflowStats {
	if flow == nil {
		return DifferentialDataflowStats{}
	}
	flow.mu.Lock()
	defer flow.mu.Unlock()
	return flow.stats
}

func cloneDifferentialRowUpdate(row DifferentialRow) DifferentialRow {
	row.Row = cloneDifferentialRow(row.Row)
	return row
}

func addDifferentialDataflowCounter(counter *uint64, delta uint64) error {
	if delta > ^uint64(0)-*counter {
		return ErrDifferentialDataflowStatsOverflow
	}
	*counter += delta
	return nil
}
