package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	ErrPerSpaceWriteQuorumInvalidOptions = errors.New("hatReplication: per-space write quorum options are invalid")
	ErrPerSpaceWriteQuorumInvalidSpace   = errors.New("hatReplication: per-space write quorum space is invalid")
)

// PerSpaceWriteQuorumPolicy marks one space as critical and supplies the
// voters that must durably acknowledge its writes. Spaces without a policy
// stay on the caller's normal asynchronous path.
type PerSpaceWriteQuorumPolicy struct {
	Space    string
	Voters   []string
	Required int
}

// PerSpaceWriteQuorumOptions configures only the critical spaces that require
// synchronous replication. An empty policy set preserves asynchronous writes
// everywhere.
type PerSpaceWriteQuorumOptions struct {
	Policies []PerSpaceWriteQuorumPolicy
}

// PerSpaceWriteQuorumResult identifies whether a space policy was applied.
// Enforced=false is a successful no-op for an unconfigured, non-critical
// space; callers can then continue their existing asynchronous write path.
type PerSpaceWriteQuorumResult struct {
	Space    string                     `json:"space"`
	Enforced bool                       `json:"enforced"`
	Decision JournalWriteQuorumDecision `json:"decision"`
}

// PerSpaceWriteQuorum routes critical-space writes to reusable journal quorum
// coordinators. The registry is immutable after construction and safe for
// concurrent Execute calls.
type PerSpaceWriteQuorum struct {
	policies map[string]*JournalWriteQuorum
}

// PerSpaceWriteQuorumAcknowledgeFunc obtains one durable acknowledgement. The
// normalized space is provided so one callback can serve every configured
// space without relying on mutable ambient state.
type PerSpaceWriteQuorumAcknowledgeFunc func(context.Context, string, string, JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error)

// NewPerSpaceWriteQuorum validates and freezes per-space quorum policies. A
// zero-value options object is valid and leaves all spaces asynchronous.
func NewPerSpaceWriteQuorum(options PerSpaceWriteQuorumOptions) (*PerSpaceWriteQuorum, error) {
	quorum := &PerSpaceWriteQuorum{policies: make(map[string]*JournalWriteQuorum, len(options.Policies))}
	for _, policy := range options.Policies {
		space := strings.TrimSpace(policy.Space)
		if space == "" {
			return nil, fmt.Errorf("%w: space is required", ErrPerSpaceWriteQuorumInvalidSpace)
		}
		if _, exists := quorum.policies[space]; exists {
			return nil, fmt.Errorf("%w: duplicate space %q", ErrPerSpaceWriteQuorumInvalidOptions, space)
		}
		coordinator, err := NewJournalWriteQuorum(JournalWriteQuorumOptions{
			Enabled:  true,
			Voters:   policy.Voters,
			Required: policy.Required,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: space %q: %v", ErrPerSpaceWriteQuorumInvalidOptions, space, err)
		}
		quorum.policies[space] = coordinator
	}
	return quorum, nil
}

// EnabledFor reports whether space is configured as a critical synchronous
// space. It is allocation-free and safe on a nil registry.
func (quorum *PerSpaceWriteQuorum) EnabledFor(space string) bool {
	if quorum == nil {
		return false
	}
	_, enabled := quorum.policies[strings.TrimSpace(space)]
	return enabled
}

// ConfiguredSpaces returns normalized policy names in deterministic order.
func (quorum *PerSpaceWriteQuorum) ConfiguredSpaces() []string {
	if quorum == nil || len(quorum.policies) == 0 {
		return nil
	}
	spaces := make([]string, 0, len(quorum.policies))
	for space := range quorum.policies {
		spaces = append(spaces, space)
	}
	sort.Strings(spaces)
	return spaces
}

// Evaluate applies the configured policy to acknowledgements collected by the
// caller. Unconfigured spaces are successful no-ops and report Enforced=false.
func (quorum *PerSpaceWriteQuorum) Evaluate(space string, proposal JournalWriteQuorumProposal, acknowledgements []JournalWriteQuorumAcknowledgement) (PerSpaceWriteQuorumResult, error) {
	space = strings.TrimSpace(space)
	result := PerSpaceWriteQuorumResult{Space: space}
	if space == "" {
		return result, ErrPerSpaceWriteQuorumInvalidSpace
	}
	if quorum == nil {
		return result, nil
	}
	coordinator, configured := quorum.policies[space]
	if !configured {
		return result, nil
	}
	result.Enforced = true
	var err error
	result.Decision, err = coordinator.Evaluate(proposal, acknowledgements)
	return result, err
}

// Execute applies synchronous replication only to a configured critical
// space. Unconfigured spaces return Enforced=false without validating or
// calling acknowledge, preserving the existing asynchronous caller path.
func (quorum *PerSpaceWriteQuorum) Execute(ctx context.Context, space string, proposal JournalWriteQuorumProposal, acknowledge PerSpaceWriteQuorumAcknowledgeFunc) (PerSpaceWriteQuorumResult, error) {
	space = strings.TrimSpace(space)
	result := PerSpaceWriteQuorumResult{Space: space}
	if space == "" {
		return result, ErrPerSpaceWriteQuorumInvalidSpace
	}
	if quorum == nil {
		return result, nil
	}
	coordinator, configured := quorum.policies[space]
	if !configured {
		return result, nil
	}
	result.Enforced = true
	decision, err := coordinator.Execute(ctx, proposal, func(callbackContext context.Context, node string, callbackProposal JournalWriteQuorumProposal) (JournalWriteQuorumAcknowledgement, error) {
		if acknowledge == nil {
			return JournalWriteQuorumAcknowledgement{}, ErrJournalWriteQuorumInvalidOptions
		}
		return acknowledge(callbackContext, space, node, callbackProposal)
	})
	result.Decision = decision
	return result, err
}
