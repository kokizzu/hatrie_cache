package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSpaceWriteQuorumInvalidOptions reports an invalid per-space policy.
	ErrSpaceWriteQuorumInvalidOptions = errors.New("hatriecache: space write quorum options are invalid")
	// ErrSpaceWriteQuorumDisabled reports that the default-off policy is not enabled.
	ErrSpaceWriteQuorumDisabled = errors.New("hatriecache: space write quorum is disabled")
	// ErrSpaceWriteQuorumNotConfigured reports a non-critical space without a policy.
	ErrSpaceWriteQuorumNotConfigured = errors.New("hatriecache: space write quorum is not configured")
)

const (
	MaxSpaceWriteQuorumSpaces    = 1024
	MaxSpaceWriteQuorumNameBytes = 128
)

// SpaceWriteQuorumOptions configures synchronous quorum admission only for
// explicitly listed critical spaces. The zero value preserves asynchronous
// replication and is the default.
type SpaceWriteQuorumOptions struct {
	Enabled bool
	Spaces  map[string]JournalWriteQuorumOptions
}

// SpaceWriteQuorum selects a reusable journal quorum by logical space name.
// It is transport-neutral; the embedding service decides which writes to
// submit and continues its normal asynchronous path for unconfigured spaces.
type SpaceWriteQuorum struct {
	enabled bool
	spaces  map[string]*JournalWriteQuorum
}

// NewSpaceWriteQuorum creates a bounded per-space quorum selector. Configuration
// is copied during construction, so later caller mutation cannot change the
// admission policy. Disabled options return a no-op selector.
func NewSpaceWriteQuorum(options SpaceWriteQuorumOptions) (*SpaceWriteQuorum, error) {
	if !options.Enabled {
		return &SpaceWriteQuorum{}, nil
	}
	if len(options.Spaces) == 0 || len(options.Spaces) > MaxSpaceWriteQuorumSpaces {
		return nil, ErrSpaceWriteQuorumInvalidOptions
	}
	spaces := make(map[string]*JournalWriteQuorum, len(options.Spaces))
	for rawSpace, quorumOptions := range options.Spaces {
		space, err := normalizeSpaceWriteQuorumName(rawSpace)
		if err != nil {
			return nil, fmt.Errorf("%w: space=%q", ErrSpaceWriteQuorumInvalidOptions, rawSpace)
		}
		if _, exists := spaces[space]; exists {
			return nil, fmt.Errorf("%w: duplicate space=%q", ErrSpaceWriteQuorumInvalidOptions, space)
		}
		if !quorumOptions.Enabled {
			return nil, fmt.Errorf("%w: space=%q is disabled", ErrSpaceWriteQuorumInvalidOptions, space)
		}
		quorum, err := NewJournalWriteQuorum(quorumOptions)
		if err != nil {
			return nil, fmt.Errorf("%w: space=%q: %v", ErrSpaceWriteQuorumInvalidOptions, space, err)
		}
		spaces[space] = quorum
	}
	return &SpaceWriteQuorum{enabled: true, spaces: spaces}, nil
}

// Enabled reports whether at least one critical-space policy is active.
func (quorum *SpaceWriteQuorum) Enabled() bool {
	return quorum != nil && quorum.enabled
}

// EnabledFor reports whether the named space has a synchronous quorum policy.
func (quorum *SpaceWriteQuorum) EnabledFor(space string) bool {
	selected, err := quorum.lookup(space)
	return err == nil && selected != nil
}

// ForSpace resolves one configured policy for reuse by a space's write path.
// Resolve it during space setup and retain the returned quorum for hot writes
// to avoid repeating the policy map lookup on every record.
func (quorum *SpaceWriteQuorum) ForSpace(space string) (*JournalWriteQuorum, error) {
	return quorum.lookup(space)
}

// Evaluate validates acknowledgements for one configured space. Unconfigured
// spaces return an explicit error so callers can retain their async path
// instead of accidentally treating a missing quorum as satisfied.
func (quorum *SpaceWriteQuorum) Evaluate(space string, proposal JournalWriteQuorumProposal, acknowledgements []JournalWriteQuorumAcknowledgement) (JournalWriteQuorumDecision, error) {
	selected, err := quorum.lookup(space)
	if err != nil {
		return spaceWriteQuorumDecision(proposal), err
	}
	return selected.Evaluate(proposal, acknowledgements)
}

// Execute collects acknowledgements for one configured space and evaluates
// them against the exact sequence and fence supplied by the caller.
func (quorum *SpaceWriteQuorum) Execute(ctx context.Context, space string, proposal JournalWriteQuorumProposal, acknowledge JournalWriteQuorumAcknowledgeFunc) (JournalWriteQuorumDecision, error) {
	selected, err := quorum.lookup(space)
	if err != nil {
		return spaceWriteQuorumDecision(proposal), err
	}
	return selected.Execute(ctx, proposal, acknowledge)
}

func (quorum *SpaceWriteQuorum) lookup(rawSpace string) (*JournalWriteQuorum, error) {
	if quorum == nil || !quorum.enabled {
		return nil, ErrSpaceWriteQuorumDisabled
	}
	if selected, ok := quorum.spaces[rawSpace]; ok {
		return selected, nil
	}
	space, err := normalizeSpaceWriteQuorumName(rawSpace)
	if err != nil {
		return nil, fmt.Errorf("%w: space=%q", ErrSpaceWriteQuorumInvalidOptions, rawSpace)
	}
	selected, ok := quorum.spaces[space]
	if !ok {
		return nil, fmt.Errorf("%w: space=%q", ErrSpaceWriteQuorumNotConfigured, space)
	}
	return selected, nil
}

func normalizeSpaceWriteQuorumName(space string) (string, error) {
	space = strings.TrimSpace(space)
	if space == "" || len(space) > MaxSpaceWriteQuorumNameBytes || strings.IndexByte(space, 0) >= 0 {
		return "", ErrSpaceWriteQuorumInvalidOptions
	}
	return space, nil
}

func spaceWriteQuorumDecision(proposal JournalWriteQuorumProposal) JournalWriteQuorumDecision {
	return JournalWriteQuorumDecision{Sequence: proposal.Sequence, FenceToken: proposal.FenceToken}
}
