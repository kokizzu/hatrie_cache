package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const maxJournalWriteQuorumTargets = 1024

var (
	// ErrJournalWriteQuorumInvalid reports invalid coordinator, request, target,
	// or callback input.
	ErrJournalWriteQuorumInvalid = errors.New("hatriecache: journal write quorum input is invalid")
	// ErrJournalWriteQuorumUnsatisfied reports that exact acknowledgements did
	// not reach the configured threshold.
	ErrJournalWriteQuorumUnsatisfied = errors.New("hatriecache: journal write quorum is unsatisfied")
	// ErrJournalWriteQuorumContextCanceled reports cancellation before the
	// required exact acknowledgements were available.
	ErrJournalWriteQuorumContextCanceled = errors.New("hatriecache: journal write quorum was canceled")
	// ErrJournalWriteQuorumStaleAcknowledgement marks an acknowledgement for a
	// different sequence or journal entry digest.
	ErrJournalWriteQuorumStaleAcknowledgement = errors.New("hatriecache: journal write quorum acknowledgement is stale")
	// ErrJournalWriteQuorumNotApplied marks a peer response that did not apply
	// the requested journal entry.
	ErrJournalWriteQuorumNotApplied = errors.New("hatriecache: journal write quorum entry was not applied")
)

// JournalWriteQuorumOptions configures one opt-in synchronous journal quorum.
// Required zero selects a majority of Nodes. Enabled false is a no-op and is
// the default, preserving asynchronous replication behavior.
type JournalWriteQuorumOptions struct {
	Enabled  bool
	Nodes    []string
	Required int
}

// JournalWriteQuorumRequest identifies one immutable journal entry. Sequence
// is the journal-wide monotonic coordinate; Digest prevents an acknowledgement
// for a different entry from satisfying the same sequence accidentally.
type JournalWriteQuorumRequest struct {
	Sequence uint64
	Digest   [32]byte
}

// JournalWriteQuorumAck is returned by a peer after it has applied the exact
// requested journal entry.
type JournalWriteQuorumAck struct {
	Sequence uint64
	Digest   [32]byte
	Applied  bool
}

// JournalWriteQuorumAckFunc applies or waits for one exact journal entry on
// one target. The callback is invoked concurrently for every configured node.
type JournalWriteQuorumAckFunc func(context.Context, string, JournalWriteQuorumRequest) (JournalWriteQuorumAck, error)

// JournalWriteQuorumAttempt records one target outcome in configured order.
type JournalWriteQuorumAttempt struct {
	Node         string `json:"node"`
	Sequence     uint64 `json:"sequence"`
	Applied      bool   `json:"applied"`
	Exact        bool   `json:"exact"`
	Acknowledged bool   `json:"acknowledged"`
	Error        string `json:"error,omitempty"`
}

// JournalWriteQuorumResult contains the exact sequence decision and every
// target outcome. A satisfied quorum does not roll back failed targets; the
// caller can use Attempts for repair or later reconciliation.
type JournalWriteQuorumResult struct {
	Enabled  bool                        `json:"enabled"`
	Sequence uint64                      `json:"sequence"`
	Decision WriteQuorumDecision         `json:"decision"`
	Attempts []JournalWriteQuorumAttempt `json:"attempts,omitempty"`
}

// JournalWriteQuorumCoordinator provides an opt-in per-entry synchronous
// acknowledgement boundary. It does not alter the existing replication path
// until a caller constructs it with Enabled true and wraps its journal write.
type JournalWriteQuorumCoordinator struct {
	enabled  bool
	nodes    []string
	required int
}

// NewJournalWriteQuorumCoordinator validates and copies a quorum policy.
// Disabled coordinators intentionally accept zero options and perform no
// callback work.
func NewJournalWriteQuorumCoordinator(options JournalWriteQuorumOptions) (*JournalWriteQuorumCoordinator, error) {
	if !options.Enabled {
		return &JournalWriteQuorumCoordinator{}, nil
	}
	if len(options.Nodes) == 0 || len(options.Nodes) > maxJournalWriteQuorumTargets {
		return nil, fmt.Errorf("%w: target count=%d", ErrJournalWriteQuorumInvalid, len(options.Nodes))
	}
	nodes := make([]string, len(options.Nodes))
	seen := make(map[string]struct{}, len(options.Nodes))
	for index, node := range options.Nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return nil, fmt.Errorf("%w: empty target", ErrJournalWriteQuorumInvalid)
		}
		if _, found := seen[node]; found {
			return nil, fmt.Errorf("%w: duplicate target=%q", ErrJournalWriteQuorumInvalid, node)
		}
		seen[node] = struct{}{}
		nodes[index] = node
	}
	required := options.Required
	if required == 0 {
		required = len(nodes)/2 + 1
	}
	if required < 1 || required > len(nodes) {
		return nil, fmt.Errorf("%w: required=%d targets=%d", ErrJournalWriteQuorumInvalid, required, len(nodes))
	}
	return &JournalWriteQuorumCoordinator{enabled: true, nodes: nodes, required: required}, nil
}

// Enabled reports whether this coordinator can issue synchronous callbacks.
func (coordinator *JournalWriteQuorumCoordinator) Enabled() bool {
	return coordinator != nil && coordinator.enabled
}

// Required returns the exact acknowledgement threshold, or zero when the
// coordinator is nil or disabled.
func (coordinator *JournalWriteQuorumCoordinator) Required() int {
	if coordinator == nil || !coordinator.enabled {
		return 0
	}
	return coordinator.required
}

// Wait waits for all configured peers and succeeds only when Required peers
// report Applied=true with the exact request sequence and digest. All peers
// are attempted so the caller receives repair information even after a quorum
// is satisfied.
func (coordinator *JournalWriteQuorumCoordinator) Wait(ctx context.Context, request JournalWriteQuorumRequest, acknowledge JournalWriteQuorumAckFunc) (JournalWriteQuorumResult, error) {
	if coordinator == nil {
		return JournalWriteQuorumResult{}, ErrJournalWriteQuorumInvalid
	}
	if ctx == nil {
		return JournalWriteQuorumResult{}, fmt.Errorf("%w: context is nil", ErrJournalWriteQuorumInvalid)
	}
	if !coordinator.enabled {
		return JournalWriteQuorumResult{Sequence: request.Sequence}, nil
	}
	if request.Sequence == 0 || acknowledge == nil {
		return JournalWriteQuorumResult{}, ErrJournalWriteQuorumInvalid
	}
	result := JournalWriteQuorumResult{
		Enabled:  true,
		Sequence: request.Sequence,
		Decision: WriteQuorumDecision{Total: len(coordinator.nodes), Required: coordinator.required},
		Attempts: make([]JournalWriteQuorumAttempt, len(coordinator.nodes)),
	}
	for index, node := range coordinator.nodes {
		result.Attempts[index].Node = node
	}
	if err := ctx.Err(); err != nil {
		for index := range result.Attempts {
			result.Attempts[index].Error = err.Error()
		}
		return result, fmt.Errorf("%w: %v", ErrJournalWriteQuorumContextCanceled, err)
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(coordinator.nodes))
	for index, node := range coordinator.nodes {
		go func(index int, node string) {
			defer waitGroup.Done()
			if err := ctx.Err(); err != nil {
				result.Attempts[index].Error = err.Error()
				return
			}
			ack, err := acknowledge(ctx, node, request)
			if err != nil {
				result.Attempts[index].Error = err.Error()
				return
			}
			attempt := &result.Attempts[index]
			attempt.Sequence = ack.Sequence
			attempt.Applied = ack.Applied
			if ack.Sequence != request.Sequence || ack.Digest != request.Digest {
				attempt.Error = ErrJournalWriteQuorumStaleAcknowledgement.Error()
				return
			}
			attempt.Exact = true
			if !ack.Applied {
				attempt.Error = ErrJournalWriteQuorumNotApplied.Error()
				return
			}
			attempt.Acknowledged = true
		}(index, node)
	}
	waitGroup.Wait()

	for _, attempt := range result.Attempts {
		if attempt.Acknowledged {
			result.Decision.Acknowledged++
		}
	}
	if err := ctx.Err(); err != nil && result.Decision.Acknowledged < coordinator.required {
		return result, fmt.Errorf("%w: %v", ErrJournalWriteQuorumContextCanceled, err)
	}
	decision, err := EvaluateWriteQuorum(result.Decision.Total, result.Decision.Acknowledged, result.Decision.Required)
	result.Decision = decision
	if err != nil {
		return result, fmt.Errorf("%w: %v", ErrJournalWriteQuorumUnsatisfied, err)
	}
	return result, nil
}
