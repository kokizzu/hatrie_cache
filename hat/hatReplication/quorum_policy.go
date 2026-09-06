package hatReplication

import (
	"errors"
	"fmt"
)

var (
	ErrWriteQuorumInvalid     = errors.New("hatriecache: write quorum configuration is invalid")
	ErrWriteQuorumUnsatisfied = errors.New("hatriecache: write quorum is unsatisfied")
)

// WriteQuorumDecision reports one explicit quorum evaluation.
type WriteQuorumDecision struct {
	Total        int  `json:"total"`
	Acknowledged int  `json:"acknowledged"`
	Required     int  `json:"required"`
	Satisfied    bool `json:"satisfied"`
}

// EvaluateWriteQuorum validates quorum inputs and reports whether the required
// number of replicas acknowledged a write. Required includes whichever local
// or remote participants the caller chooses to count.
func EvaluateWriteQuorum(total, acknowledged, required int) (WriteQuorumDecision, error) {
	if total < 1 || acknowledged < 0 || acknowledged > total || required < 1 || required > total {
		return WriteQuorumDecision{}, fmt.Errorf("%w: total=%d acknowledged=%d required=%d", ErrWriteQuorumInvalid, total, acknowledged, required)
	}
	decision := WriteQuorumDecision{Total: total, Acknowledged: acknowledged, Required: required, Satisfied: acknowledged >= required}
	if !decision.Satisfied {
		return decision, fmt.Errorf("%w: acknowledged=%d required=%d", ErrWriteQuorumUnsatisfied, acknowledged, required)
	}
	return decision, nil
}
