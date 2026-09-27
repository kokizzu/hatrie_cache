package hatCache

import (
	"context"
	"errors"
	"fmt"

	"hatrie_cache/hat/hatSchema"
)

var (
	// ErrReplicationSchemaRolloutNil reports a method call on a nil rollout.
	ErrReplicationSchemaRolloutNil = errors.New("replication schema rollout is nil")
	// ErrReplicationSchemaRolloutInvalid reports a malformed rollout operation.
	ErrReplicationSchemaRolloutInvalid = errors.New("replication schema rollout is invalid")
)

// ReplicationSchemaRollout composes a validated rolling schema plan with the
// replication contract transition that surrounds it. It is opt-in control
// plane state; ordinary replication does not allocate or consult it.
//
// During a rollout, both the previous and next contracts are accepted. Each
// replica reports the previous contract until activation, then reports the
// next contract. Once every replica is active, the previous contract is
// retired from the compatibility policy.
type ReplicationSchemaRollout struct {
	plan             hatSchema.RollingSchemaPlan
	deployment       *hatSchema.RollingSchemaDeployment
	previous         ReplicationSchemaContract
	next             ReplicationSchemaContract
	transitionPolicy *ReplicationSchemaCompatibilityPolicy
}

// NewReplicationSchemaRollout validates a conservative schema change and
// creates a deterministic replica rollout. The input schemas and node list
// are copied by the underlying plan.
func NewReplicationSchemaRollout(previous, next hatSchema.Schema, nodes []string) (*ReplicationSchemaRollout, error) {
	plan, err := hatSchema.NewRollingSchemaPlan(previous, next, nodes)
	if err != nil {
		return nil, fmt.Errorf("%w: plan: %v", ErrReplicationSchemaRolloutInvalid, err)
	}
	previousSnapshot := plan.PreviousSchema()
	nextSnapshot := plan.NextSchema()
	policy, err := NewReplicationSchemaCompatibilityPolicy(nextSnapshot, previousSnapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: contracts: %v", ErrReplicationSchemaRolloutInvalid, err)
	}
	return &ReplicationSchemaRollout{
		plan:             plan,
		deployment:       plan.Begin(),
		previous:         NewReplicationSchemaContract(previousSnapshot),
		next:             NewReplicationSchemaContract(nextSnapshot),
		transitionPolicy: policy,
	}, nil
}

// Nodes returns the deterministic replica order captured by the rollout.
func (rollout *ReplicationSchemaRollout) Nodes() []string {
	if rollout == nil {
		return nil
	}
	return rollout.plan.Nodes()
}

// Begin returns a fresh rollout over the same immutable plan and contracts.
// It is useful for retrying a complete control-plane cycle or for callers that
// retain one validated plan for multiple deployments.
func (rollout *ReplicationSchemaRollout) Begin() *ReplicationSchemaRollout {
	if rollout == nil {
		return nil
	}
	return &ReplicationSchemaRollout{
		plan:             rollout.plan,
		deployment:       rollout.plan.Begin(),
		previous:         rollout.previous,
		next:             rollout.next,
		transitionPolicy: rollout.transitionPolicy,
	}
}

// PreviousContract returns the contract served before the rollout.
func (rollout *ReplicationSchemaRollout) PreviousContract() ReplicationSchemaContract {
	if rollout == nil {
		return ReplicationSchemaContract{}
	}
	return rollout.previous
}

// NextContract returns the contract served after the rollout completes.
func (rollout *ReplicationSchemaRollout) NextContract() ReplicationSchemaContract {
	if rollout == nil {
		return ReplicationSchemaContract{}
	}
	return rollout.next
}

// Prepare records that a replica has installed and validated the next schema.
func (rollout *ReplicationSchemaRollout) Prepare(node string) error {
	if rollout == nil {
		return ErrReplicationSchemaRolloutNil
	}
	return rollout.deployment.Prepare(node)
}

// Activate switches a prepared replica to the next schema.
func (rollout *ReplicationSchemaRollout) Activate(node string) error {
	if rollout == nil {
		return ErrReplicationSchemaRolloutNil
	}
	return rollout.deployment.Activate(node)
}

// Run executes the existing sequential rolling coordinator with the rollout's
// contract-aware deployment state.
func (rollout *ReplicationSchemaRollout) Run(ctx context.Context, install hatSchema.RollingSchemaInstallFunc, activate hatSchema.RollingSchemaActivateFunc) error {
	if rollout == nil {
		return ErrReplicationSchemaRolloutNil
	}
	return rollout.plan.Run(ctx, rollout.deployment, install, activate)
}

// Phase returns one replica's synchronized rollout phase.
func (rollout *ReplicationSchemaRollout) Phase(node string) (hatSchema.RollingSchemaPhase, bool) {
	if rollout == nil {
		return hatSchema.RollingSchemaPhasePending, false
	}
	return rollout.deployment.Phase(node)
}

// Contract returns the contract a replica should serve at its current stable
// phase. In-progress transitions conservatively report the previous contract.
func (rollout *ReplicationSchemaRollout) Contract(node string) (ReplicationSchemaContract, bool) {
	if rollout == nil {
		return ReplicationSchemaContract{}, false
	}
	phase, ok := rollout.deployment.Phase(node)
	if !ok {
		return ReplicationSchemaContract{}, false
	}
	if phase == hatSchema.RollingSchemaPhaseActive {
		return rollout.next, true
	}
	return rollout.previous, true
}

// Complete reports whether every replica is active on the next contract.
func (rollout *ReplicationSchemaRollout) Complete() bool {
	return rollout != nil && rollout.deployment.Complete()
}

// Accepts reports whether a replication contract is valid for the current
// rollout phase. The previous contract is rejected immediately after the last
// replica activates.
func (rollout *ReplicationSchemaRollout) Accepts(contract ReplicationSchemaContract) bool {
	if rollout == nil {
		return false
	}
	if rollout.Complete() {
		return contract == rollout.next
	}
	return rollout.transitionPolicy.Accepts(contract)
}

// CompatibilityPolicy returns a policy snapshot for use by command or stream
// options. Callers should refresh it after Complete becomes true to retire the
// previous contract. Accepts is allocation-free and always reflects current
// rollout state.
func (rollout *ReplicationSchemaRollout) CompatibilityPolicy() (*ReplicationSchemaCompatibilityPolicy, error) {
	if rollout == nil {
		return nil, ErrReplicationSchemaRolloutNil
	}
	if !rollout.Complete() {
		return rollout.transitionPolicy, nil
	}
	return NewReplicationSchemaCompatibilityPolicy(rollout.plan.NextSchema())
}
