package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrJournalWriteQuorumInvalidOptions         = errors.New("hatriecache: journal write quorum options are invalid")
	ErrJournalWriteQuorumInvalidProposal        = errors.New("hatriecache: journal write quorum proposal is invalid")
	ErrJournalWriteQuorumInvalidAcknowledgement = errors.New("hatriecache: journal write quorum acknowledgement is invalid")
	ErrJournalWriteQuorumDisabled               = errors.New("hatriecache: journal write quorum is disabled")
	ErrJournalWriteQuorumContextCanceled        = errors.New("hatriecache: journal write quorum execution was canceled")
	ErrJournalWriteQuorumUnsatisfied            = errors.New("hatriecache: journal write quorum is unsatisfied")
)

// JournalWriteQuorumOptions configures the opt-in journal-wide write contract.
// Voters should include the local writer and every replica that can acknowledge
// the journal sequence. Required zero means a strict majority.
type JournalWriteQuorumOptions struct {
	Enabled  bool
	Voters   []string
	Required int
}

// JournalWriteQuorum is a reusable, transport-neutral quorum coordinator.
// Construction normalizes configuration once; callers supply the journal
// sequence and fence token for each write.
type JournalWriteQuorum struct {
	enabled  bool
	voters   []string
	required int
}

// JournalWriteQuorumProposal identifies the exact journal commit that replicas
// must durably acknowledge.
type JournalWriteQuorumProposal struct {
	Sequence   uint64 `json:"sequence"`
	FenceToken uint64 `json:"fence_token,omitempty"`
}

// JournalWriteQuorumAcknowledgement is one replica's response for a proposal.
// A false Accepted response is a valid rejection and never contributes to the
// quorum. Sequence and FenceToken must match the proposal exactly.
type JournalWriteQuorumAcknowledgement struct {
	Node       string `json:"node"`
	Sequence   uint64 `json:"sequence"`
	FenceToken uint64 `json:"fence_token,omitempty"`
	Accepted   bool   `json:"accepted"`
}

// JournalWriteQuorumDecision reports the result without retaining replica
// names. Counts keep the valid decision path allocation-free.
type JournalWriteQuorumDecision struct {
	Sequence     uint64 `json:"sequence"`
	FenceToken   uint64 `json:"fence_token,omitempty"`
	Total        int    `json:"total"`
	Acknowledged int    `json:"acknowledged"`
	Rejected     int    `json:"rejected"`
	Stale        int    `json:"stale"`
	Required     int    `json:"required"`
	Satisfied    bool   `json:"satisfied"`
}

// JournalWriteQuorumAcknowledgeFunc obtains one replica's durable response.
// The callback must bind its response to the supplied node and proposal.
type JournalWriteQuorumAcknowledgeFunc func(context.Context, string, JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error)

// NewJournalWriteQuorum creates an opt-in journal write quorum. Disabled
// options return a reusable no-op coordinator and intentionally skip all
// voter validation, preserving the default-off configuration path.
func NewJournalWriteQuorum(options JournalWriteQuorumOptions) (*JournalWriteQuorum, error) {
	if !options.Enabled {
		return &JournalWriteQuorum{}, nil
	}
	voters, required, err := normalizeJournalWriteQuorumOptions(options)
	if err != nil {
		return nil, err
	}
	return &JournalWriteQuorum{enabled: true, voters: voters, required: required}, nil
}

// Enabled reports whether this coordinator will admit synchronous quorum
// execution.
func (quorum *JournalWriteQuorum) Enabled() bool {
	return quorum != nil && quorum.enabled
}

// Evaluate validates acknowledgements collected by a caller and evaluates the
// exact journal sequence/fence proposal. The successful path does not allocate.
func (quorum *JournalWriteQuorum) Evaluate(proposal JournalWriteQuorumProposal, acknowledgements []JournalWriteQuorumAcknowledgement) (JournalWriteQuorumDecision, error) {
	decision, err := quorum.beginDecision(proposal)
	if err != nil {
		return decision, err
	}
	for index, acknowledgement := range acknowledgements {
		node := strings.TrimSpace(acknowledgement.Node)
		if journalWriteQuorumVoterIndex(quorum.voters, node) < 0 {
			return decision, fmt.Errorf("%w: unknown voter %q", ErrJournalWriteQuorumInvalidAcknowledgement, node)
		}
		for previous := 0; previous < index; previous++ {
			if strings.TrimSpace(acknowledgements[previous].Node) == node {
				return decision, fmt.Errorf("%w: duplicate voter %q", ErrJournalWriteQuorumInvalidAcknowledgement, node)
			}
		}
		if acknowledgement.Sequence != proposal.Sequence || acknowledgement.FenceToken != proposal.FenceToken {
			decision.Stale++
			continue
		}
		if acknowledgement.Accepted {
			decision.Acknowledged++
		} else {
			decision.Rejected++
		}
	}
	if decision.Acknowledged < decision.Required {
		return decision, fmt.Errorf("%w: acknowledged=%d required=%d", ErrJournalWriteQuorumUnsatisfied, decision.Acknowledged, decision.Required)
	}
	decision.Satisfied = true
	return decision, nil
}

// Execute collects acknowledgements concurrently and evaluates them against
// one proposal. Failed callbacks simply do not count; a stale or malformed
// successful response fails closed instead of being counted as durable.
func (quorum *JournalWriteQuorum) Execute(ctx context.Context, proposal JournalWriteQuorumProposal, acknowledge JournalWriteQuorumAcknowledgeFunc) (JournalWriteQuorumDecision, error) {
	decision, err := quorum.beginDecision(proposal)
	if err != nil {
		return decision, err
	}
	if ctx == nil || acknowledge == nil {
		return decision, ErrJournalWriteQuorumInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return decision, fmt.Errorf("%w: %v", ErrJournalWriteQuorumContextCanceled, err)
	}

	acknowledgements := make([]JournalWriteQuorumAcknowledgement, 0, len(quorum.voters))
	valid := make([]bool, len(quorum.voters))
	responses := make([]JournalWriteQuorumAcknowledgement, len(quorum.voters))
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(quorum.voters))
	for index, node := range quorum.voters {
		go func(index int, node string) {
			defer waitGroup.Done()
			if ctx.Err() != nil {
				return
			}
			response, callbackErr := acknowledge(ctx, node, proposal)
			if callbackErr != nil {
				return
			}
			if strings.TrimSpace(response.Node) != node {
				return
			}
			responses[index] = response
			valid[index] = true
		}(index, node)
	}
	waitGroup.Wait()
	if err := ctx.Err(); err != nil {
		return decision, fmt.Errorf("%w: %v", ErrJournalWriteQuorumContextCanceled, err)
	}
	for index, response := range responses {
		if valid[index] {
			acknowledgements = append(acknowledgements, response)
		}
	}
	return quorum.Evaluate(proposal, acknowledgements)
}

func (quorum *JournalWriteQuorum) beginDecision(proposal JournalWriteQuorumProposal) (JournalWriteQuorumDecision, error) {
	if quorum == nil || !quorum.enabled {
		return JournalWriteQuorumDecision{Sequence: proposal.Sequence, FenceToken: proposal.FenceToken}, ErrJournalWriteQuorumDisabled
	}
	decision := JournalWriteQuorumDecision{
		Sequence:   proposal.Sequence,
		FenceToken: proposal.FenceToken,
		Total:      len(quorum.voters),
		Required:   quorum.required,
	}
	if proposal.Sequence == 0 {
		return decision, ErrJournalWriteQuorumInvalidProposal
	}
	return decision, nil
}

func normalizeJournalWriteQuorumOptions(options JournalWriteQuorumOptions) ([]string, int, error) {
	if len(options.Voters) == 0 {
		return nil, 0, ErrJournalWriteQuorumInvalidOptions
	}
	voters := make([]string, len(options.Voters))
	for index, voter := range options.Voters {
		voter = strings.TrimSpace(voter)
		if voter == "" || journalWriteQuorumVoterIndex(voters[:index], voter) >= 0 {
			return nil, 0, ErrJournalWriteQuorumInvalidOptions
		}
		voters[index] = voter
	}
	required := options.Required
	if required == 0 {
		required = len(voters)/2 + 1
	}
	if required < 1 || required > len(voters) {
		return nil, 0, ErrJournalWriteQuorumInvalidOptions
	}
	return voters, required, nil
}

func journalWriteQuorumVoterIndex(voters []string, node string) int {
	for index, voter := range voters {
		if voter == node {
			return index
		}
	}
	return -1
}
