package hatReplication

import (
	"errors"
	"fmt"
)

var (
	// ErrQuorumPolicyInvalid reports an invalid node or quorum configuration.
	ErrQuorumPolicyInvalid = errors.New("hatriecache: quorum policy is invalid")
	// ErrReadQuorumInvalid reports invalid read acknowledgement inputs.
	ErrReadQuorumInvalid = errors.New("hatriecache: read quorum is invalid")
	// ErrReadQuorumUnsatisfied reports that a read did not reach its quorum.
	ErrReadQuorumUnsatisfied = errors.New("hatriecache: read quorum is unsatisfied")
)

// QuorumPolicy contains explicit read and write acknowledgement thresholds.
// It only evaluates supplied acknowledgements; it does not send network
// requests or change the existing asynchronous replication path.
type QuorumPolicy struct {
	Total         int
	ReadRequired  int
	WriteRequired int
}

// ReadQuorumDecision reports one read quorum evaluation. It shares the
// decision fields with WriteQuorumDecision for easy aggregation by callers.
type ReadQuorumDecision = WriteQuorumDecision

// NewQuorumPolicy validates explicit read and write thresholds for a fixed
// replica set.
func NewQuorumPolicy(total, readRequired, writeRequired int) (QuorumPolicy, error) {
	policy := QuorumPolicy{Total: total, ReadRequired: readRequired, WriteRequired: writeRequired}
	if err := validateQuorumPolicy(policy); err != nil {
		return QuorumPolicy{}, err
	}
	return policy, nil
}

// NewDefaultQuorumPolicy uses a majority for both reads and writes.
func NewDefaultQuorumPolicy(total int) (QuorumPolicy, error) {
	majority := total/2 + 1
	return NewQuorumPolicy(total, majority, majority)
}

// EvaluateReadQuorum evaluates a read acknowledgement count against an
// explicit threshold.
func EvaluateReadQuorum(total, acknowledged, required int) (ReadQuorumDecision, error) {
	if total <= 0 || acknowledged < 0 || acknowledged > total || required < 1 || required > total {
		return ReadQuorumDecision{}, fmt.Errorf("%w: total=%d acknowledged=%d required=%d", ErrReadQuorumInvalid, total, acknowledged, required)
	}
	decision := ReadQuorumDecision{Total: total, Acknowledged: acknowledged, Required: required, Satisfied: acknowledged >= required}
	if !decision.Satisfied {
		return decision, fmt.Errorf("%w: acknowledged=%d required=%d", ErrReadQuorumUnsatisfied, acknowledged, required)
	}
	return decision, nil
}

// EvaluateRead evaluates the policy's read threshold.
func (policy QuorumPolicy) EvaluateRead(acknowledged int) (ReadQuorumDecision, error) {
	if err := validateQuorumPolicy(policy); err != nil {
		return ReadQuorumDecision{}, err
	}
	return EvaluateReadQuorum(policy.Total, acknowledged, policy.ReadRequired)
}

// EvaluateWrite evaluates the policy's write threshold.
func (policy QuorumPolicy) EvaluateWrite(acknowledged int) (WriteQuorumDecision, error) {
	if err := validateQuorumPolicy(policy); err != nil {
		return WriteQuorumDecision{}, err
	}
	return EvaluateWriteQuorum(policy.Total, acknowledged, policy.WriteRequired)
}

func validateQuorumPolicy(policy QuorumPolicy) error {
	if policy.Total <= 0 || policy.ReadRequired < 1 || policy.ReadRequired > policy.Total || policy.WriteRequired < 1 || policy.WriteRequired > policy.Total {
		return fmt.Errorf("%w: total=%d read_required=%d write_required=%d", ErrQuorumPolicyInvalid, policy.Total, policy.ReadRequired, policy.WriteRequired)
	}
	return nil
}
