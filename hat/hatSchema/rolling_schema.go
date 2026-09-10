package hatSchema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrRollingSchemaPlanInvalid reports invalid schemas or replica identities.
	ErrRollingSchemaPlanInvalid = errors.New("hatSchema: rolling schema plan is invalid")
	// ErrRollingSchemaNodeUnknown reports a node not registered in the plan.
	ErrRollingSchemaNodeUnknown = errors.New("hatSchema: rolling schema node is unknown")
	// ErrRollingSchemaTransition reports an attempted phase skip or regression.
	ErrRollingSchemaTransition = errors.New("hatSchema: rolling schema transition is invalid")
	// ErrRollingSchemaCoordinatorInvalid reports missing hooks or an empty plan.
	ErrRollingSchemaCoordinatorInvalid = errors.New("hatSchema: rolling schema coordinator is invalid")
)

// RollingSchemaPhase is the monotone deployment phase for one replica.
type RollingSchemaPhase uint8

const (
	RollingSchemaPhasePending RollingSchemaPhase = iota
	RollingSchemaPhasePrepared
	RollingSchemaPhaseActive
	rollingSchemaPhaseInstalling
	rollingSchemaPhaseActivating
)

func (phase RollingSchemaPhase) String() string {
	switch phase {
	case RollingSchemaPhasePending:
		return "pending"
	case RollingSchemaPhasePrepared:
		return "prepared"
	case RollingSchemaPhaseActive:
		return "active"
	default:
		return "unknown"
	}
}

// RollingSchemaNode is a stable snapshot of one replica's rollout phase.
type RollingSchemaNode struct {
	Node  string             `json:"node"`
	Phase RollingSchemaPhase `json:"phase"`
}

// RollingSchemaPlan contains immutable validated schemas and replica IDs for
// one conservative rolling deployment. It does not alter schemas or contact
// replicas; the deployment state machine supplies the coordination boundary.
type RollingSchemaPlan struct {
	previous Schema
	next     Schema
	nodes    []string
}

// NewRollingSchemaPlan validates a conservative schema transition and returns
// an independent, deterministic plan for the supplied replicas.
func NewRollingSchemaPlan(previous, next Schema, nodes []string) (RollingSchemaPlan, error) {
	if err := previous.Validate(); err != nil {
		return RollingSchemaPlan{}, fmt.Errorf("%w: previous schema: %v", ErrRollingSchemaPlanInvalid, err)
	}
	if err := next.Validate(); err != nil {
		return RollingSchemaPlan{}, fmt.Errorf("%w: next schema: %v", ErrRollingSchemaPlanInvalid, err)
	}
	report, err := CheckRollingCompatibility(previous, next)
	if err != nil {
		return RollingSchemaPlan{}, fmt.Errorf("%w: compatibility: %v", ErrRollingSchemaPlanInvalid, err)
	}
	if !report.Compatible {
		return RollingSchemaPlan{}, fmt.Errorf("%w: incompatible schema change", ErrRollingSchemaPlanInvalid)
	}
	if len(nodes) == 0 {
		return RollingSchemaPlan{}, fmt.Errorf("%w: at least one node is required", ErrRollingSchemaPlanInvalid)
	}
	normalizedNodes := make([]string, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return RollingSchemaPlan{}, fmt.Errorf("%w: node %d is empty", ErrRollingSchemaPlanInvalid, index)
		}
		if _, exists := seen[node]; exists {
			return RollingSchemaPlan{}, fmt.Errorf("%w: duplicate node %q", ErrRollingSchemaPlanInvalid, node)
		}
		seen[node] = struct{}{}
		normalizedNodes[index] = node
	}
	sort.Strings(normalizedNodes)
	return RollingSchemaPlan{
		previous: previous.Clone(),
		next:     next.Clone(),
		nodes:    normalizedNodes,
	}, nil
}

// PreviousSchema returns an independent copy of the schema served before the
// rollout.
func (plan RollingSchemaPlan) PreviousSchema() Schema {
	return plan.previous.Clone()
}

// NextSchema returns an independent copy of the schema activated by the
// rollout.
func (plan RollingSchemaPlan) NextSchema() Schema {
	return plan.next.Clone()
}

// Nodes returns replica IDs in deterministic order.
func (plan RollingSchemaPlan) Nodes() []string {
	return append([]string(nil), plan.nodes...)
}

// Begin creates an independent synchronized deployment state machine.
func (plan RollingSchemaPlan) Begin() *RollingSchemaDeployment {
	nodes := append([]string(nil), plan.nodes...)
	indices := make(map[string]int, len(nodes))
	for index, node := range nodes {
		indices[node] = index
	}
	return &RollingSchemaDeployment{
		nodes:  nodes,
		phases: make([]RollingSchemaPhase, len(nodes)),
		index:  indices,
	}
}

// RollingSchemaDeployment tracks per-replica progress. Methods are safe for
// concurrent control-plane callers and permit idempotent retries.
type RollingSchemaDeployment struct {
	mu     sync.RWMutex
	nodes  []string
	phases []RollingSchemaPhase
	index  map[string]int
}

// Prepare records that node has installed and validated the next schema while
// it can still serve the previous schema. Repeating the operation is safe.
func (deployment *RollingSchemaDeployment) Prepare(node string) error {
	return deployment.advance(node, RollingSchemaPhasePrepared)
}

// Activate records that node has switched to the next schema. Activation is
// only legal after Prepare and is idempotent for retrying coordinators.
func (deployment *RollingSchemaDeployment) Activate(node string) error {
	return deployment.advance(node, RollingSchemaPhaseActive)
}

// RollingSchemaInstallFunc installs and validates the next schema on one
// replica while that replica can still serve the previous schema.
type RollingSchemaInstallFunc func(context.Context, string, Schema) error

// RollingSchemaActivateFunc switches one prepared replica to the next schema.
type RollingSchemaActivateFunc func(context.Context, string, Schema) error

// Run coordinates a sequential rolling deployment through caller-supplied
// transport hooks. Successful phases are recorded before returning, so a
// failed or canceled run can be retried without repeating completed work.
// Hooks receive independent schema snapshots and are never called while the
// deployment state lock is held. Concurrent runs that reach the same node
// while its hook is active are rejected as an invalid phase transition.
func (plan RollingSchemaPlan) Run(ctx context.Context, deployment *RollingSchemaDeployment, install RollingSchemaInstallFunc, activate RollingSchemaActivateFunc) error {
	if deployment == nil {
		return ErrRollingSchemaCoordinatorInvalid
	}
	if install == nil || activate == nil {
		return fmt.Errorf("%w: install and activate hooks are required", ErrRollingSchemaCoordinatorInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	deployment.mu.RLock()
	nodes := append([]string(nil), deployment.nodes...)
	deployment.mu.RUnlock()
	if len(nodes) == 0 {
		return fmt.Errorf("%w: deployment has no nodes", ErrRollingSchemaCoordinatorInvalid)
	}
	if len(nodes) != len(plan.nodes) {
		return fmt.Errorf("%w: deployment does not match plan", ErrRollingSchemaCoordinatorInvalid)
	}
	for index, node := range nodes {
		if node != plan.nodes[index] {
			return fmt.Errorf("%w: deployment does not match plan", ErrRollingSchemaCoordinatorInvalid)
		}
	}
	next := plan.next

	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		phase, ok := deployment.Phase(node)
		if !ok {
			return fmt.Errorf("%w: node %q disappeared", ErrRollingSchemaCoordinatorInvalid, node)
		}
		if phase == RollingSchemaPhasePending {
			if err := deployment.claimPhase(node, RollingSchemaPhasePending, rollingSchemaPhaseInstalling); err != nil {
				return err
			}
			if err := install(ctx, node, next.Clone()); err != nil {
				deployment.restorePhase(node, rollingSchemaPhaseInstalling, RollingSchemaPhasePending)
				return fmt.Errorf("hatSchema: install %q: %w", node, err)
			}
			if err := deployment.finishPhase(node, rollingSchemaPhaseInstalling, RollingSchemaPhasePrepared); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		phase, ok = deployment.Phase(node)
		if !ok {
			return fmt.Errorf("%w: node %q disappeared", ErrRollingSchemaCoordinatorInvalid, node)
		}
		if phase == RollingSchemaPhasePrepared {
			if err := deployment.claimPhase(node, RollingSchemaPhasePrepared, rollingSchemaPhaseActivating); err != nil {
				return err
			}
			if err := activate(ctx, node, next.Clone()); err != nil {
				deployment.restorePhase(node, rollingSchemaPhaseActivating, RollingSchemaPhasePrepared)
				return fmt.Errorf("hatSchema: activate %q: %w", node, err)
			}
			if err := deployment.finishPhase(node, rollingSchemaPhaseActivating, RollingSchemaPhaseActive); err != nil {
				return err
			}
		}
	}
	return nil
}

func (deployment *RollingSchemaDeployment) claimPhase(node string, expected, inProgress RollingSchemaPhase) error {
	if deployment == nil {
		return ErrRollingSchemaCoordinatorInvalid
	}
	node = strings.TrimSpace(node)
	deployment.mu.Lock()
	defer deployment.mu.Unlock()
	index, ok := deployment.index[node]
	if !ok {
		return fmt.Errorf("%w: %q", ErrRollingSchemaNodeUnknown, node)
	}
	current := deployment.phases[index]
	if current != expected {
		if current == rollingSchemaPhaseInstalling || current == rollingSchemaPhaseActivating {
			return fmt.Errorf("%w: node %q already has a transition in progress", ErrRollingSchemaTransition, node)
		}
		return fmt.Errorf("%w: node %q cannot start from %s", ErrRollingSchemaTransition, node, current)
	}
	deployment.phases[index] = inProgress
	return nil
}

func (deployment *RollingSchemaDeployment) finishPhase(node string, inProgress, completed RollingSchemaPhase) error {
	return deployment.replacePhase(node, inProgress, completed)
}

func (deployment *RollingSchemaDeployment) restorePhase(node string, inProgress, restored RollingSchemaPhase) error {
	return deployment.replacePhase(node, inProgress, restored)
}

func (deployment *RollingSchemaDeployment) replacePhase(node string, expected, replacement RollingSchemaPhase) error {
	if deployment == nil {
		return ErrRollingSchemaCoordinatorInvalid
	}
	node = strings.TrimSpace(node)
	deployment.mu.Lock()
	defer deployment.mu.Unlock()
	index, ok := deployment.index[node]
	if !ok {
		return fmt.Errorf("%w: %q", ErrRollingSchemaNodeUnknown, node)
	}
	if deployment.phases[index] != expected {
		return fmt.Errorf("%w: node %q changed while transition was in progress", ErrRollingSchemaTransition, node)
	}
	deployment.phases[index] = replacement
	return nil
}

func stableRollingSchemaPhase(phase RollingSchemaPhase) RollingSchemaPhase {
	switch phase {
	case rollingSchemaPhaseInstalling:
		return RollingSchemaPhasePending
	case rollingSchemaPhaseActivating:
		return RollingSchemaPhasePrepared
	default:
		return phase
	}
}

func (deployment *RollingSchemaDeployment) advance(node string, target RollingSchemaPhase) error {
	if deployment == nil {
		return ErrRollingSchemaTransition
	}
	node = strings.TrimSpace(node)
	deployment.mu.Lock()
	defer deployment.mu.Unlock()
	index, ok := deployment.index[node]
	if !ok {
		return fmt.Errorf("%w: %q", ErrRollingSchemaNodeUnknown, node)
	}
	current := deployment.phases[index]
	if current == rollingSchemaPhaseInstalling || current == rollingSchemaPhaseActivating {
		return fmt.Errorf("%w: node %q already has a transition in progress", ErrRollingSchemaTransition, node)
	}
	if current == target || current > target {
		return nil
	}
	if target != current+1 {
		return fmt.Errorf("%w: node %q cannot move from %s to %s", ErrRollingSchemaTransition, node, current, target)
	}
	deployment.phases[index] = target
	return nil
}

// Phase returns the current phase for node.
func (deployment *RollingSchemaDeployment) Phase(node string) (RollingSchemaPhase, bool) {
	if deployment == nil {
		return RollingSchemaPhasePending, false
	}
	node = strings.TrimSpace(node)
	deployment.mu.RLock()
	defer deployment.mu.RUnlock()
	index, ok := deployment.index[node]
	if !ok {
		return RollingSchemaPhasePending, false
	}
	return stableRollingSchemaPhase(deployment.phases[index]), true
}

// Complete reports whether every planned replica has activated the next
// schema. An empty or nil deployment is never complete.
func (deployment *RollingSchemaDeployment) Complete() bool {
	if deployment == nil {
		return false
	}
	deployment.mu.RLock()
	defer deployment.mu.RUnlock()
	if len(deployment.phases) == 0 {
		return false
	}
	for _, phase := range deployment.phases {
		if phase != RollingSchemaPhaseActive {
			return false
		}
	}
	return true
}

// Snapshot returns all node phases in deterministic node order.
func (deployment *RollingSchemaDeployment) Snapshot() []RollingSchemaNode {
	if deployment == nil {
		return nil
	}
	deployment.mu.RLock()
	defer deployment.mu.RUnlock()
	if len(deployment.nodes) == 0 {
		return nil
	}
	snapshot := make([]RollingSchemaNode, len(deployment.nodes))
	for index, node := range deployment.nodes {
		snapshot[index] = RollingSchemaNode{Node: node, Phase: stableRollingSchemaPhase(deployment.phases[index])}
	}
	return snapshot
}
