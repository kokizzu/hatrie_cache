package hatSchema

import (
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
)

// RollingSchemaPhase is the monotone deployment phase for one replica.
type RollingSchemaPhase uint8

const (
	RollingSchemaPhasePending RollingSchemaPhase = iota
	RollingSchemaPhasePrepared
	RollingSchemaPhaseActive
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
	return deployment.phases[index], true
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
		snapshot[index] = RollingSchemaNode{Node: node, Phase: deployment.phases[index]}
	}
	return snapshot
}
