package hatPipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

const (
	// MaxReplicatedComputeRouterOperators bounds one immutable route table.
	MaxReplicatedComputeRouterOperators = 1 << 16
	// MaxReplicatedComputeRouterReplicasPerOperator keeps route selection
	// bounded while leaving enough room for large failure-domain layouts.
	MaxReplicatedComputeRouterReplicasPerOperator = 64
)

var (
	// ErrReplicatedComputeRouterInvalid reports a malformed placement plan or
	// a method call on a nil router.
	ErrReplicatedComputeRouterInvalid = errors.New("hatPipeline: replicated compute router is invalid")
	// ErrReplicatedComputeRouterOperatorNotFound reports an unknown operator.
	ErrReplicatedComputeRouterOperatorNotFound = errors.New("hatPipeline: replicated compute operator is not configured")
	// ErrReplicatedComputeRouterWorkerNotFound reports an unknown worker.
	ErrReplicatedComputeRouterWorkerNotFound = errors.New("hatPipeline: replicated compute worker is not configured")
	// ErrReplicatedComputeRouterUnavailable reports that every replica is
	// currently unhealthy.
	ErrReplicatedComputeRouterUnavailable = errors.New("hatPipeline: replicated compute operator has no healthy worker")
)

// ReplicatedComputeWorkerState is a detached health view for one worker.
type ReplicatedComputeWorkerState struct {
	WorkerID string
	Healthy  bool
}

type replicatedComputeRoute struct {
	assignments []DataflowPlacementAssignment
	health      []*atomic.Bool
}

// ReplicatedComputeRouter selects a healthy replica from an immutable
// DataflowPlacementPlan. Replica zero is preferred, followed by increasing
// replica number. Health changes are atomic and shared by every operator route
// that references the worker, so a failed worker is removed from all of its
// maintained indexes without taking a routing lock.
//
// The router deliberately does not retry application callbacks. Callers must
// report worker health explicitly after an idempotent or otherwise safe
// failure decision; this avoids duplicating side effects for non-idempotent
// maintained-index updates.
type ReplicatedComputeRouter struct {
	routes  map[string]*replicatedComputeRoute
	workers map[string]*atomic.Bool
}

// NewReplicatedComputeRouter creates a deterministic, allocation-free-after-
// construction failover router from a placement plan. Every operator's
// replicas must be numbered contiguously from zero and must use distinct
// workers.
func NewReplicatedComputeRouter(plan DataflowPlacementPlan) (*ReplicatedComputeRouter, error) {
	if len(plan.Assignments) == 0 || len(plan.Assignments) > MaxReplicatedComputeRouterOperators*MaxReplicatedComputeRouterReplicasPerOperator {
		return nil, fmt.Errorf("%w: assignment count", ErrReplicatedComputeRouterInvalid)
	}
	grouped := make(map[string][]DataflowPlacementAssignment)
	for _, input := range plan.Assignments {
		operatorID := strings.TrimSpace(input.OperatorID)
		workerID := strings.TrimSpace(input.WorkerID)
		if operatorID == "" || workerID == "" || len(operatorID) > maxDataflowPlacementIDSize || len(workerID) > maxDataflowPlacementIDSize || input.Replica < 0 {
			return nil, fmt.Errorf("%w: operator, worker, or replica metadata", ErrReplicatedComputeRouterInvalid)
		}
		input.OperatorID = operatorID
		input.WorkerID = workerID
		input.FailureDomain = strings.TrimSpace(input.FailureDomain)
		grouped[operatorID] = append(grouped[operatorID], input)
	}
	if len(grouped) == 0 || len(grouped) > MaxReplicatedComputeRouterOperators {
		return nil, fmt.Errorf("%w: operator count", ErrReplicatedComputeRouterInvalid)
	}

	router := &ReplicatedComputeRouter{
		routes:  make(map[string]*replicatedComputeRoute, len(grouped)),
		workers: make(map[string]*atomic.Bool),
	}
	for operatorID, assignments := range grouped {
		if len(assignments) > MaxReplicatedComputeRouterReplicasPerOperator {
			return nil, fmt.Errorf("%w: operator %q has too many replicas", ErrReplicatedComputeRouterInvalid, operatorID)
		}
		sort.Slice(assignments, func(left, right int) bool {
			return assignments[left].Replica < assignments[right].Replica
		})
		health := make([]*atomic.Bool, len(assignments))
		seenWorkers := make(map[string]struct{}, len(assignments))
		for replica, assignment := range assignments {
			if assignment.Replica != replica {
				return nil, fmt.Errorf("%w: operator %q replica numbering", ErrReplicatedComputeRouterInvalid, operatorID)
			}
			if _, exists := seenWorkers[assignment.WorkerID]; exists {
				return nil, fmt.Errorf("%w: operator %q repeats worker %q", ErrReplicatedComputeRouterInvalid, operatorID, assignment.WorkerID)
			}
			seenWorkers[assignment.WorkerID] = struct{}{}
			workerHealth := router.workers[assignment.WorkerID]
			if workerHealth == nil {
				workerHealth = &atomic.Bool{}
				workerHealth.Store(true)
				router.workers[assignment.WorkerID] = workerHealth
			}
			health[replica] = workerHealth
		}
		router.routes[operatorID] = &replicatedComputeRoute{
			assignments: append([]DataflowPlacementAssignment(nil), assignments...),
			health:      health,
		}
	}
	return router, nil
}

// Select returns the preferred healthy replica for operatorID. It performs no
// allocation and is safe to call concurrently with health changes.
func (router *ReplicatedComputeRouter) Select(operatorID string) (DataflowPlacementAssignment, error) {
	if router == nil {
		return DataflowPlacementAssignment{}, ErrReplicatedComputeRouterInvalid
	}
	route, ok := router.routes[operatorID]
	if !ok {
		normalizedOperatorID := strings.TrimSpace(operatorID)
		if normalizedOperatorID != operatorID {
			route, ok = router.routes[normalizedOperatorID]
		}
	}
	if !ok {
		return DataflowPlacementAssignment{}, ErrReplicatedComputeRouterOperatorNotFound
	}
	for index, assignment := range route.assignments {
		if route.health[index].Load() {
			return assignment, nil
		}
	}
	return DataflowPlacementAssignment{}, ErrReplicatedComputeRouterUnavailable
}

// MarkWorkerUnhealthy removes workerID from every operator route that uses it.
// It is idempotent and does not retry or cancel application work.
func (router *ReplicatedComputeRouter) MarkWorkerUnhealthy(workerID string) error {
	return router.setWorkerHealth(workerID, false)
}

// MarkWorkerHealthy makes workerID eligible for primary or standby routing
// again. It is idempotent.
func (router *ReplicatedComputeRouter) MarkWorkerHealthy(workerID string) error {
	return router.setWorkerHealth(workerID, true)
}

func (router *ReplicatedComputeRouter) setWorkerHealth(workerID string, healthy bool) error {
	if router == nil {
		return ErrReplicatedComputeRouterInvalid
	}
	workerID = strings.TrimSpace(workerID)
	worker, ok := router.workers[workerID]
	if !ok {
		return ErrReplicatedComputeRouterWorkerNotFound
	}
	worker.Store(healthy)
	return nil
}

// WorkerStates returns sorted detached health state for all workers referenced
// by the placement plan.
func (router *ReplicatedComputeRouter) WorkerStates() []ReplicatedComputeWorkerState {
	if router == nil || len(router.workers) == 0 {
		return []ReplicatedComputeWorkerState{}
	}
	workerIDs := make([]string, 0, len(router.workers))
	for workerID := range router.workers {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Strings(workerIDs)
	states := make([]ReplicatedComputeWorkerState, 0, len(workerIDs))
	for _, workerID := range workerIDs {
		states = append(states, ReplicatedComputeWorkerState{
			WorkerID: workerID,
			Healthy:  router.workers[workerID].Load(),
		})
	}
	return states
}
