package hatTopology

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// MaxRaftConfigurationMembers bounds one configuration and prevents a
	// malformed membership entry from allocating without limit.
	MaxRaftConfigurationMembers = 1024
	// MaxRaftConfigurationNodeIDBytes bounds identifiers carried in entries.
	MaxRaftConfigurationNodeIDBytes = 256
)

var (
	ErrRaftConfigurationInvalid      = errors.New("hatTopology: invalid raft configuration")
	ErrRaftConfigurationProposal     = errors.New("hatTopology: invalid raft configuration proposal")
	ErrRaftConfigurationJointPending = errors.New("hatTopology: raft joint configuration is pending")
	ErrRaftConfigurationIndex        = errors.New("hatTopology: raft configuration index is invalid")
	ErrRaftConfigurationTerm         = errors.New("hatTopology: raft configuration term is invalid")
	ErrRaftConfigurationMember       = errors.New("hatTopology: raft configuration member is invalid")
)

// RaftConfigurationChangeKind identifies one membership state-machine
// operation. Voter changes use joint consensus; learner-only changes publish
// directly because they do not change the voting quorum.
type RaftConfigurationChangeKind string

const (
	RaftConfigurationAddVoter       RaftConfigurationChangeKind = "add_voter"
	RaftConfigurationRemoveVoter    RaftConfigurationChangeKind = "remove_voter"
	RaftConfigurationAddLearner     RaftConfigurationChangeKind = "add_learner"
	RaftConfigurationRemoveLearner  RaftConfigurationChangeKind = "remove_learner"
	RaftConfigurationPromoteLearner RaftConfigurationChangeKind = "promote_learner"
)

// RaftConfigurationChange is one deterministic membership mutation.
type RaftConfigurationChange struct {
	Kind   RaftConfigurationChangeKind `json:"kind"`
	NodeID string                      `json:"node_id"`
}

// RaftConfiguration is an immutable-by-convention membership snapshot. During
// joint consensus Voters is the incoming set and JointOldVoters plus
// JointNewVoters define the two quorums that must both acknowledge a commit.
type RaftConfiguration struct {
	Index          uint64   `json:"index"`
	Term           uint64   `json:"term"`
	Voters         []string `json:"voters"`
	Learners       []string `json:"learners,omitempty"`
	JointOldVoters []string `json:"joint_old_voters,omitempty"`
	JointNewVoters []string `json:"joint_new_voters,omitempty"`
}

// IsJoint reports whether this snapshot requires both old and new majorities.
func (configuration RaftConfiguration) IsJoint() bool {
	return len(configuration.JointOldVoters) != 0 || len(configuration.JointNewVoters) != 0
}

// RaftConfigurationEntry is the replicated state-machine value applied by
// each node. The transport and durable Raft log remain caller-owned.
type RaftConfigurationEntry struct {
	Configuration RaftConfiguration `json:"configuration"`
}

// RaftConfigurationState applies ordered configuration entries and serializes
// one local proposal at a time. It contains no network or goroutine state.
type RaftConfigurationState struct {
	configuration RaftConfiguration
	pending       *RaftConfigurationEntry
}

// RaftQuorumDecision reports the acknowledgement counts for one snapshot.
type RaftQuorumDecision struct {
	Acknowledged    int  `json:"acknowledged"`
	Required        int  `json:"required"`
	OldAcknowledged int  `json:"old_acknowledged"`
	OldRequired     int  `json:"old_required"`
	NewAcknowledged int  `json:"new_acknowledged"`
	NewRequired     int  `json:"new_required"`
	Satisfied       bool `json:"satisfied"`
}

// NewRaftConfigurationState creates a stable configuration at index and term
// zero. At least one unique voter is required.
func NewRaftConfigurationState(voters []string) (*RaftConfigurationState, error) {
	normalized, err := normalizeRaftConfigurationMembers(voters, "voters")
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return nil, fmt.Errorf("%w: at least one voter is required", ErrRaftConfigurationInvalid)
	}
	return &RaftConfigurationState{configuration: RaftConfiguration{Voters: normalized}}, nil
}

// Snapshot returns an independently owned configuration snapshot.
func (state *RaftConfigurationState) Snapshot() RaftConfiguration {
	if state == nil {
		return RaftConfiguration{}
	}
	return cloneRaftConfiguration(state.configuration)
}

// ProposeMembershipChange validates and returns the next replicated entry. The
// state is not advanced until Apply receives the returned entry.
func (state *RaftConfigurationState) ProposeMembershipChange(term uint64, change RaftConfigurationChange) (RaftConfigurationEntry, error) {
	if state == nil {
		return RaftConfigurationEntry{}, ErrRaftConfigurationInvalid
	}
	if state.pending != nil {
		return RaftConfigurationEntry{}, ErrRaftConfigurationProposal
	}
	if err := validateRaftConfigurationTerm(state.configuration.Term, term); err != nil {
		return RaftConfigurationEntry{}, err
	}
	if state.configuration.IsJoint() {
		return RaftConfigurationEntry{}, ErrRaftConfigurationJointPending
	}
	nextVoters, nextLearners, err := applyRaftConfigurationChange(state.configuration.Voters, state.configuration.Learners, change)
	if err != nil {
		return RaftConfigurationEntry{}, err
	}
	configuration := RaftConfiguration{
		Index:    state.configuration.Index + 1,
		Term:     term,
		Voters:   nextVoters,
		Learners: nextLearners,
	}
	if raftConfigurationChangeUsesJointConsensus(change.Kind) {
		configuration.JointOldVoters = append([]string(nil), state.configuration.Voters...)
		configuration.JointNewVoters = append([]string(nil), nextVoters...)
	}
	return state.rememberProposal(RaftConfigurationEntry{Configuration: configuration})
}

// ProposeMembershipFinalize returns the stable entry that completes the
// currently applied joint configuration.
func (state *RaftConfigurationState) ProposeMembershipFinalize(term uint64) (RaftConfigurationEntry, error) {
	if state == nil {
		return RaftConfigurationEntry{}, ErrRaftConfigurationInvalid
	}
	if state.pending != nil {
		return RaftConfigurationEntry{}, ErrRaftConfigurationProposal
	}
	if err := validateRaftConfigurationTerm(state.configuration.Term, term); err != nil {
		return RaftConfigurationEntry{}, err
	}
	if !state.configuration.IsJoint() {
		return RaftConfigurationEntry{}, fmt.Errorf("%w: no joint configuration is pending", ErrRaftConfigurationProposal)
	}
	configuration := RaftConfiguration{
		Index:    state.configuration.Index + 1,
		Term:     term,
		Voters:   append([]string(nil), state.configuration.JointNewVoters...),
		Learners: append([]string(nil), state.configuration.Learners...),
	}
	return state.rememberProposal(RaftConfigurationEntry{Configuration: configuration})
}

// Apply validates and publishes one ordered configuration entry. A failed
// apply leaves the current snapshot and pending proposal unchanged.
func (state *RaftConfigurationState) Apply(entry RaftConfigurationEntry) error {
	if state == nil {
		return ErrRaftConfigurationInvalid
	}
	configuration, err := normalizeRaftConfiguration(entry.Configuration)
	if err != nil {
		return err
	}
	if configuration.Index != state.configuration.Index+1 {
		return fmt.Errorf("%w: got %d, want %d", ErrRaftConfigurationIndex, configuration.Index, state.configuration.Index+1)
	}
	if err := validateRaftConfigurationTerm(state.configuration.Term, configuration.Term); err != nil {
		return err
	}
	if err := validateRaftConfigurationTransition(state.configuration, configuration); err != nil {
		return err
	}
	if state.pending != nil && !equalRaftConfiguration(state.pending.Configuration, configuration) {
		return ErrRaftConfigurationProposal
	}
	state.configuration = configuration
	state.pending = nil
	return nil
}

// EvaluateQuorum counts unique voter acknowledgements. In joint mode both old
// and new majorities are required; learners and unknown IDs are rejected.
func (configuration RaftConfiguration) EvaluateQuorum(acknowledgements []string) (RaftQuorumDecision, error) {
	normalized, err := normalizeRaftConfiguration(configuration)
	if err != nil {
		return RaftQuorumDecision{}, err
	}
	acknowledged := make(map[string]struct{}, len(acknowledgements))
	for _, member := range acknowledgements {
		member = strings.TrimSpace(member)
		if member == "" {
			return RaftQuorumDecision{}, ErrRaftConfigurationMember
		}
		if _, exists := acknowledged[member]; exists {
			return RaftQuorumDecision{}, fmt.Errorf("%w: duplicate acknowledgement %q", ErrRaftConfigurationMember, member)
		}
		if !raftConfigurationVoter(normalized, member) {
			return RaftQuorumDecision{}, fmt.Errorf("%w: %q is not a voter", ErrRaftConfigurationMember, member)
		}
		acknowledged[member] = struct{}{}
	}
	if !normalized.IsJoint() {
		required := raftMajority(len(normalized.Voters))
		count := countRaftConfigurationMembers(acknowledged, normalized.Voters)
		return RaftQuorumDecision{Acknowledged: count, Required: required, OldAcknowledged: count, OldRequired: required, Satisfied: count >= required}, nil
	}
	oldRequired := raftMajority(len(normalized.JointOldVoters))
	newRequired := raftMajority(len(normalized.JointNewVoters))
	oldCount := countRaftConfigurationMembers(acknowledged, normalized.JointOldVoters)
	newCount := countRaftConfigurationMembers(acknowledged, normalized.JointNewVoters)
	return RaftQuorumDecision{
		Acknowledged:    len(acknowledged),
		OldAcknowledged: oldCount,
		OldRequired:     oldRequired,
		NewAcknowledged: newCount,
		NewRequired:     newRequired,
		Satisfied:       oldCount >= oldRequired && newCount >= newRequired,
	}, nil
}

func (state *RaftConfigurationState) rememberProposal(entry RaftConfigurationEntry) (RaftConfigurationEntry, error) {
	configuration, err := normalizeRaftConfiguration(entry.Configuration)
	if err != nil {
		return RaftConfigurationEntry{}, err
	}
	entry.Configuration = configuration
	state.pending = &RaftConfigurationEntry{Configuration: cloneRaftConfiguration(configuration)}
	return RaftConfigurationEntry{Configuration: cloneRaftConfiguration(configuration)}, nil
}

func validateRaftConfigurationTerm(current, next uint64) error {
	if next == 0 || next < current {
		return fmt.Errorf("%w: current %d, next %d", ErrRaftConfigurationTerm, current, next)
	}
	return nil
}

func raftConfigurationChangeUsesJointConsensus(kind RaftConfigurationChangeKind) bool {
	switch kind {
	case RaftConfigurationAddVoter, RaftConfigurationRemoveVoter, RaftConfigurationPromoteLearner:
		return true
	default:
		return false
	}
}

func applyRaftConfigurationChange(voters, learners []string, change RaftConfigurationChange) ([]string, []string, error) {
	nodeID := strings.TrimSpace(change.NodeID)
	if nodeID == "" || len(nodeID) > MaxRaftConfigurationNodeIDBytes || nodeID != change.NodeID {
		return nil, nil, fmt.Errorf("%w: node ID is invalid", ErrRaftConfigurationMember)
	}
	nextVoters := append([]string(nil), voters...)
	nextLearners := append([]string(nil), learners...)
	if raftConfigurationMemberIn(nodeID, voters) || raftConfigurationMemberIn(nodeID, learners) {
		switch change.Kind {
		case RaftConfigurationRemoveVoter:
			if !raftConfigurationMemberIn(nodeID, voters) {
				return nil, nil, fmt.Errorf("%w: %q is not a voter", ErrRaftConfigurationMember, nodeID)
			}
			if len(voters) <= 1 {
				return nil, nil, fmt.Errorf("%w: a configuration needs one voter", ErrRaftConfigurationInvalid)
			}
			nextVoters = removeRaftConfigurationMember(voters, nodeID)
		case RaftConfigurationRemoveLearner:
			if !raftConfigurationMemberIn(nodeID, learners) {
				return nil, nil, fmt.Errorf("%w: %q is not a learner", ErrRaftConfigurationMember, nodeID)
			}
			nextLearners = removeRaftConfigurationMember(learners, nodeID)
		case RaftConfigurationPromoteLearner:
			if !raftConfigurationMemberIn(nodeID, learners) {
				return nil, nil, fmt.Errorf("%w: %q is not a learner", ErrRaftConfigurationMember, nodeID)
			}
			nextLearners = removeRaftConfigurationMember(learners, nodeID)
			nextVoters = append(nextVoters, nodeID)
		default:
			return nil, nil, fmt.Errorf("%w: member %q already exists", ErrRaftConfigurationProposal, nodeID)
		}
	} else {
		switch change.Kind {
		case RaftConfigurationAddVoter:
			nextVoters = append(nextVoters, nodeID)
		case RaftConfigurationAddLearner:
			nextLearners = append(nextLearners, nodeID)
		default:
			return nil, nil, fmt.Errorf("%w: member %q does not exist", ErrRaftConfigurationMember, nodeID)
		}
	}
	sort.Strings(nextVoters)
	sort.Strings(nextLearners)
	return nextVoters, nextLearners, nil
}

func validateRaftConfigurationTransition(previous, next RaftConfiguration) error {
	if previous.IsJoint() {
		if next.IsJoint() || !equalStringSlices(next.Voters, previous.JointNewVoters) {
			return fmt.Errorf("%w: joint configuration must be finalized to its new voters", ErrRaftConfigurationProposal)
		}
		return nil
	}
	if next.IsJoint() {
		if !equalStringSlices(next.JointOldVoters, previous.Voters) || !equalStringSlices(next.JointNewVoters, next.Voters) {
			return fmt.Errorf("%w: joint configuration does not name the current and next voters", ErrRaftConfigurationProposal)
		}
		return nil
	}
	if !equalStringSlices(previous.Voters, next.Voters) {
		return fmt.Errorf("%w: voter changes require joint consensus", ErrRaftConfigurationProposal)
	}
	return nil
}

func normalizeRaftConfiguration(configuration RaftConfiguration) (RaftConfiguration, error) {
	voters, err := normalizeRaftConfigurationMembers(configuration.Voters, "voters")
	if err != nil {
		return RaftConfiguration{}, err
	}
	if len(voters) == 0 {
		return RaftConfiguration{}, fmt.Errorf("%w: voters are empty", ErrRaftConfigurationInvalid)
	}
	learners, err := normalizeRaftConfigurationMembers(configuration.Learners, "learners")
	if err != nil {
		return RaftConfiguration{}, err
	}
	for _, learner := range learners {
		if raftConfigurationMemberIn(learner, voters) {
			return RaftConfiguration{}, fmt.Errorf("%w: %q is both voter and learner", ErrRaftConfigurationInvalid, learner)
		}
	}
	configuration.Voters = voters
	configuration.Learners = learners
	if !configuration.IsJoint() {
		return configuration, nil
	}
	oldVoters, err := normalizeRaftConfigurationMembers(configuration.JointOldVoters, "joint old voters")
	if err != nil {
		return RaftConfiguration{}, err
	}
	newVoters, err := normalizeRaftConfigurationMembers(configuration.JointNewVoters, "joint new voters")
	if err != nil {
		return RaftConfiguration{}, err
	}
	if !equalStringSlices(newVoters, voters) {
		return RaftConfiguration{}, fmt.Errorf("%w: joint new voters must equal voters", ErrRaftConfigurationInvalid)
	}
	configuration.JointOldVoters = oldVoters
	configuration.JointNewVoters = newVoters
	return configuration, nil
}

func normalizeRaftConfigurationMembers(members []string, kind string) ([]string, error) {
	if len(members) > MaxRaftConfigurationMembers {
		return nil, fmt.Errorf("%w: %s exceed member limit", ErrRaftConfigurationInvalid, kind)
	}
	result := append([]string(nil), members...)
	sort.Strings(result)
	for index, member := range result {
		if member == "" || len(member) > MaxRaftConfigurationNodeIDBytes || member != strings.TrimSpace(member) {
			return nil, fmt.Errorf("%w: invalid %s member", ErrRaftConfigurationMember, kind)
		}
		if index > 0 && result[index-1] == member {
			return nil, fmt.Errorf("%w: duplicate %s member %q", ErrRaftConfigurationInvalid, kind, member)
		}
	}
	return result, nil
}

func cloneRaftConfiguration(configuration RaftConfiguration) RaftConfiguration {
	configuration.Voters = append([]string(nil), configuration.Voters...)
	configuration.Learners = append([]string(nil), configuration.Learners...)
	configuration.JointOldVoters = append([]string(nil), configuration.JointOldVoters...)
	configuration.JointNewVoters = append([]string(nil), configuration.JointNewVoters...)
	return configuration
}

func equalRaftConfiguration(left, right RaftConfiguration) bool {
	return left.Index == right.Index && left.Term == right.Term && equalStringSlices(left.Voters, right.Voters) && equalStringSlices(left.Learners, right.Learners) && equalStringSlices(left.JointOldVoters, right.JointOldVoters) && equalStringSlices(left.JointNewVoters, right.JointNewVoters)
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func raftConfigurationMemberIn(member string, members []string) bool {
	index := sort.SearchStrings(members, member)
	return index < len(members) && members[index] == member
}

func removeRaftConfigurationMember(members []string, member string) []string {
	result := make([]string, 0, len(members)-1)
	for _, candidate := range members {
		if candidate != member {
			result = append(result, candidate)
		}
	}
	return result
}

func raftConfigurationVoter(configuration RaftConfiguration, member string) bool {
	return raftConfigurationMemberIn(member, configuration.Voters) || (configuration.IsJoint() && (raftConfigurationMemberIn(member, configuration.JointOldVoters) || raftConfigurationMemberIn(member, configuration.JointNewVoters)))
}

func countRaftConfigurationMembers(acknowledged map[string]struct{}, members []string) int {
	count := 0
	for _, member := range members {
		if _, exists := acknowledged[member]; exists {
			count++
		}
	}
	return count
}

func raftMajority(count int) int {
	return count/2 + 1
}
