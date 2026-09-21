package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultRaftConfigurationStateMaxMembers bounds one configuration when
	// the caller does not provide a smaller limit.
	DefaultRaftConfigurationStateMaxMembers = 64
	maxRaftConfigurationStateMembers        = 1 << 20
	maxRaftConfigurationStateNodeBytes      = 256
)

var (
	// ErrRaftConfigurationStateNil indicates a method call on a nil state.
	ErrRaftConfigurationStateNil = errors.New("hatReplication: raft configuration state is nil")
	// ErrRaftConfigurationStateInvalid indicates malformed membership or a
	// malformed configuration transition.
	ErrRaftConfigurationStateInvalid = errors.New("hatReplication: raft configuration state is invalid")
	// ErrRaftConfigurationStatePending indicates that another transition must
	// commit or be discarded before a new one can be proposed.
	ErrRaftConfigurationStatePending = errors.New("hatReplication: raft configuration transition is pending")
	// ErrRaftConfigurationStateNoChange indicates a transition with no effect.
	ErrRaftConfigurationStateNoChange = errors.New("hatReplication: raft configuration transition has no change")
	// ErrRaftConfigurationStateQuorum indicates that both joint quorums were
	// not satisfied by the acknowledgements.
	ErrRaftConfigurationStateQuorum = errors.New("hatReplication: raft configuration joint quorum is unsatisfied")
	// ErrRaftConfigurationStateAcknowledgement indicates an invalid or stale
	// acknowledgement.
	ErrRaftConfigurationStateAcknowledgement = errors.New("hatReplication: raft configuration acknowledgement is invalid")
	// ErrRaftConfigurationStateStale indicates a stale term, generation, or
	// configuration entry.
	ErrRaftConfigurationStateStale = errors.New("hatReplication: raft configuration state is stale")
	// ErrRaftConfigurationStateNoPending indicates that there is no transition
	// to commit.
	ErrRaftConfigurationStateNoPending = errors.New("hatReplication: raft configuration has no pending transition")
)

// RaftConfigurationStateOptions configures the initial committed membership.
// Membership state is opt-in and transport-neutral; this type does not start
// an election or replicate a log.
type RaftConfigurationStateOptions struct {
	InitialVoters   []string
	InitialLearners []string
	MaxMembers      int
}

// RaftConfiguration is a canonical membership view. Voters participate in
// quorum; learners receive state but do not participate in quorum.
type RaftConfiguration struct {
	Voters   []string `json:"voters"`
	Learners []string `json:"learners,omitempty"`
}

// RaftConfigurationChange describes one atomic membership transition. Remove
// operations are applied before add operations, so learner promotion and
// demotion can be expressed in one joint transition.
type RaftConfigurationChange struct {
	AddVoters      []string
	RemoveVoters   []string
	AddLearners    []string
	RemoveLearners []string
}

// RaftConfigurationEntry is the immutable joint-consensus proposal returned by
// Propose. Both Previous and Next must acknowledge the entry before commit.
type RaftConfigurationEntry struct {
	Index      uint64
	Term       uint64
	Generation uint64
	Previous   RaftConfiguration
	Next       RaftConfiguration
}

// RaftConfigurationAcknowledgement is one transport-reported acceptance of a
// configuration entry. Learners and unknown nodes cannot acknowledge quorum.
type RaftConfigurationAcknowledgement struct {
	Node     string
	Index    uint64
	Term     uint64
	Accepted bool
}

// RaftConfigurationSnapshot is a detached state view. Pending is non-nil only
// while a joint transition awaits both quorum certificates.
type RaftConfigurationSnapshot struct {
	Current    RaftConfiguration
	Pending    *RaftConfigurationEntry
	Index      uint64
	Term       uint64
	Generation uint64
}

// RaftConfigurationState tracks one committed membership and at most one
// pending joint transition. It does not persist entries, elect leaders, send
// messages, or publish serving topology; callers own those side effects.
type RaftConfigurationState struct {
	mu         sync.RWMutex
	maxMembers int
	current    RaftConfiguration
	pending    *RaftConfigurationEntry
	index      uint64
	term       uint64
	generation uint64
}

// NewRaftConfigurationState creates a validated committed configuration.
func NewRaftConfigurationState(options RaftConfigurationStateOptions) (*RaftConfigurationState, error) {
	maxMembers := options.MaxMembers
	if maxMembers == 0 {
		maxMembers = DefaultRaftConfigurationStateMaxMembers
	}
	if maxMembers < 1 || maxMembers > maxRaftConfigurationStateMembers {
		return nil, ErrRaftConfigurationStateInvalid
	}
	current, err := normalizeRaftConfiguration(options.InitialVoters, options.InitialLearners, maxMembers)
	if err != nil {
		return nil, err
	}
	return &RaftConfigurationState{
		maxMembers: maxMembers,
		current:    current,
		generation: 1,
	}, nil
}

// Propose validates and records one joint-consensus transition. Term must be
// non-zero and must not move backward. A second proposal is rejected until the
// first entry commits.
func (state *RaftConfigurationState) Propose(change RaftConfigurationChange, term uint64) (RaftConfigurationEntry, error) {
	if state == nil {
		return RaftConfigurationEntry{}, ErrRaftConfigurationStateNil
	}
	if term == 0 {
		return RaftConfigurationEntry{}, fmt.Errorf("%w: term is zero", ErrRaftConfigurationStateInvalid)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pending != nil {
		return RaftConfigurationEntry{}, ErrRaftConfigurationStatePending
	}
	if term < state.term {
		return RaftConfigurationEntry{}, ErrRaftConfigurationStateStale
	}
	next, err := applyRaftConfigurationChange(state.current, change, state.maxMembers)
	if err != nil {
		return RaftConfigurationEntry{}, err
	}
	if equalRaftConfiguration(state.current, next) {
		return RaftConfigurationEntry{}, ErrRaftConfigurationStateNoChange
	}
	entry := RaftConfigurationEntry{
		Index:      state.index + 1,
		Term:       term,
		Generation: state.generation,
		Previous:   cloneRaftConfiguration(state.current),
		Next:       cloneRaftConfiguration(next),
	}
	state.pending = &entry
	return cloneRaftConfigurationEntry(entry), nil
}

// Commit validates acknowledgements for a pending entry and publishes its
// next configuration only when the old and new voter sets both have a
// majority. Failed commits leave the pending transition unchanged.
func (state *RaftConfigurationState) Commit(entry RaftConfigurationEntry, acknowledgements []RaftConfigurationAcknowledgement) (RaftConfigurationSnapshot, error) {
	if state == nil {
		return RaftConfigurationSnapshot{}, ErrRaftConfigurationStateNil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pending == nil {
		return RaftConfigurationSnapshot{}, ErrRaftConfigurationStateNoPending
	}
	if !equalRaftConfigurationEntry(*state.pending, entry) || entry.Generation != state.generation {
		return RaftConfigurationSnapshot{}, ErrRaftConfigurationStateStale
	}
	oldAcks, newAcks, err := validateRaftConfigurationAcknowledgements(entry, acknowledgements)
	if err != nil {
		return RaftConfigurationSnapshot{}, err
	}
	if oldAcks < raftConfigurationMajority(len(entry.Previous.Voters)) || newAcks < raftConfigurationMajority(len(entry.Next.Voters)) {
		return RaftConfigurationSnapshot{}, ErrRaftConfigurationStateQuorum
	}
	state.current = cloneRaftConfiguration(entry.Next)
	state.pending = nil
	state.index = entry.Index
	state.term = entry.Term
	state.generation++
	return state.snapshotLocked(), nil
}

// Snapshot returns a detached state view suitable for status, persistence, or
// retry coordination.
func (state *RaftConfigurationState) Snapshot() RaftConfigurationSnapshot {
	if state == nil {
		return RaftConfigurationSnapshot{}
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.snapshotLocked()
}

func (state *RaftConfigurationState) snapshotLocked() RaftConfigurationSnapshot {
	snapshot := RaftConfigurationSnapshot{
		Current:    cloneRaftConfiguration(state.current),
		Index:      state.index,
		Term:       state.term,
		Generation: state.generation,
	}
	if state.pending != nil {
		pending := cloneRaftConfigurationEntry(*state.pending)
		snapshot.Pending = &pending
	}
	return snapshot
}

func applyRaftConfigurationChange(current RaftConfiguration, change RaftConfigurationChange, maxMembers int) (RaftConfiguration, error) {
	voters := append([]string(nil), current.Voters...)
	learners := append([]string(nil), current.Learners...)
	var err error
	for _, node := range change.RemoveVoters {
		voters, err = removeRaftConfigurationMember(voters, node)
		if err != nil {
			return RaftConfiguration{}, err
		}
	}
	for _, node := range change.RemoveLearners {
		learners, err = removeRaftConfigurationMember(learners, node)
		if err != nil {
			return RaftConfiguration{}, err
		}
	}
	for _, node := range change.AddVoters {
		if containsRaftConfigurationMember(voters, node) || containsRaftConfigurationMember(learners, node) {
			return RaftConfiguration{}, fmt.Errorf("%w: voter %q already exists", ErrRaftConfigurationStateInvalid, node)
		}
		voters = append(voters, node)
	}
	for _, node := range change.AddLearners {
		if containsRaftConfigurationMember(voters, node) || containsRaftConfigurationMember(learners, node) {
			return RaftConfiguration{}, fmt.Errorf("%w: learner %q already exists", ErrRaftConfigurationStateInvalid, node)
		}
		learners = append(learners, node)
	}
	return normalizeRaftConfiguration(voters, learners, maxMembers)
}

func normalizeRaftConfiguration(voters, learners []string, maxMembers int) (RaftConfiguration, error) {
	normalizedVoters, err := normalizeRaftConfigurationMembers(voters)
	if err != nil {
		return RaftConfiguration{}, err
	}
	if len(normalizedVoters) == 0 {
		return RaftConfiguration{}, fmt.Errorf("%w: at least one voter is required", ErrRaftConfigurationStateInvalid)
	}
	normalizedLearners, err := normalizeRaftConfigurationMembers(learners)
	if err != nil {
		return RaftConfiguration{}, err
	}
	for _, voter := range normalizedVoters {
		if containsRaftConfigurationMember(normalizedLearners, voter) {
			return RaftConfiguration{}, fmt.Errorf("%w: node %q is both voter and learner", ErrRaftConfigurationStateInvalid, voter)
		}
	}
	if len(normalizedVoters)+len(normalizedLearners) > maxMembers {
		return RaftConfiguration{}, fmt.Errorf("%w: member limit exceeded", ErrRaftConfigurationStateInvalid)
	}
	return RaftConfiguration{Voters: normalizedVoters, Learners: normalizedLearners}, nil
}

func normalizeRaftConfigurationMembers(members []string) ([]string, error) {
	if len(members) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(members))
	result := make([]string, 0, len(members))
	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" || len(member) > maxRaftConfigurationStateNodeBytes {
			return nil, fmt.Errorf("%w: member ID is invalid", ErrRaftConfigurationStateInvalid)
		}
		if _, exists := seen[member]; exists {
			return nil, fmt.Errorf("%w: duplicate member %q", ErrRaftConfigurationStateInvalid, member)
		}
		seen[member] = struct{}{}
		result = append(result, member)
	}
	sort.Strings(result)
	return result, nil
}

func removeRaftConfigurationMember(members []string, node string) ([]string, error) {
	node = strings.TrimSpace(node)
	if node == "" || len(node) > maxRaftConfigurationStateNodeBytes {
		return nil, fmt.Errorf("%w: member ID is invalid", ErrRaftConfigurationStateInvalid)
	}
	for index, member := range members {
		if member == node {
			return append(append([]string(nil), members[:index]...), members[index+1:]...), nil
		}
	}
	return nil, fmt.Errorf("%w: member %q is not present", ErrRaftConfigurationStateInvalid, node)
}

func containsRaftConfigurationMember(members []string, node string) bool {
	for _, member := range members {
		if member == strings.TrimSpace(node) {
			return true
		}
	}
	return false
}

func validateRaftConfigurationAcknowledgements(entry RaftConfigurationEntry, acknowledgements []RaftConfigurationAcknowledgement) (int, int, error) {
	oldVoters := make(map[string]struct{}, len(entry.Previous.Voters))
	for _, node := range entry.Previous.Voters {
		oldVoters[node] = struct{}{}
	}
	newVoters := make(map[string]struct{}, len(entry.Next.Voters))
	for _, node := range entry.Next.Voters {
		newVoters[node] = struct{}{}
	}
	seen := make(map[string]struct{}, len(acknowledgements))
	oldCount, newCount := 0, 0
	for _, acknowledgement := range acknowledgements {
		node := strings.TrimSpace(acknowledgement.Node)
		if node == "" || node != acknowledgement.Node || acknowledgement.Index != entry.Index || acknowledgement.Term != entry.Term {
			return 0, 0, ErrRaftConfigurationStateAcknowledgement
		}
		if _, exists := seen[node]; exists {
			return 0, 0, ErrRaftConfigurationStateAcknowledgement
		}
		seen[node] = struct{}{}
		if _, old := oldVoters[node]; !old {
			if _, next := newVoters[node]; !next {
				return 0, 0, ErrRaftConfigurationStateAcknowledgement
			}
		}
		if !acknowledgement.Accepted {
			continue
		}
		if _, old := oldVoters[node]; old {
			oldCount++
		}
		if _, next := newVoters[node]; next {
			newCount++
		}
	}
	return oldCount, newCount, nil
}

func raftConfigurationMajority(voters int) int {
	return voters/2 + 1
}

func cloneRaftConfiguration(configuration RaftConfiguration) RaftConfiguration {
	return RaftConfiguration{
		Voters:   append([]string(nil), configuration.Voters...),
		Learners: append([]string(nil), configuration.Learners...),
	}
}

func cloneRaftConfigurationEntry(entry RaftConfigurationEntry) RaftConfigurationEntry {
	entry.Previous = cloneRaftConfiguration(entry.Previous)
	entry.Next = cloneRaftConfiguration(entry.Next)
	return entry
}

func equalRaftConfiguration(left, right RaftConfiguration) bool {
	if len(left.Voters) != len(right.Voters) || len(left.Learners) != len(right.Learners) {
		return false
	}
	for index := range left.Voters {
		if left.Voters[index] != right.Voters[index] {
			return false
		}
	}
	for index := range left.Learners {
		if left.Learners[index] != right.Learners[index] {
			return false
		}
	}
	return true
}

func equalRaftConfigurationEntry(left, right RaftConfigurationEntry) bool {
	return left.Index == right.Index && left.Term == right.Term && left.Generation == right.Generation &&
		equalRaftConfiguration(left.Previous, right.Previous) && equalRaftConfiguration(left.Next, right.Next)
}
