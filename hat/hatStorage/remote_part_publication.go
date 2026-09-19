package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	ErrRemotePartPublicationInvalid           = errors.New("hatriecache: invalid remote-part publication")
	ErrRemotePartPublicationContextRequired   = errors.New("hatriecache: remote-part publication context is required")
	ErrRemotePartPublicationStoreRequired     = errors.New("hatriecache: remote-part publication store is required")
	ErrRemotePartPublicationQuorumUnsatisfied = errors.New("hatriecache: remote-part publication quorum is unsatisfied")
	ErrRemotePartPublicationStale             = errors.New("hatriecache: remote-part publication fencing token is stale")
)

// RemotePartPublicationOptions describes the control-plane identity and
// quorum policy for one remote-part publication.
//
// Required zero means a strict majority of Voters. Publication is opt-in:
// creating a proposal does not make a remote part visible.
type RemotePartPublicationOptions struct {
	PublicationID               string   `json:"publication_id"`
	ExpectedManifestFingerprint string   `json:"expected_manifest_fingerprint"`
	FencingToken                uint64   `json:"fencing_token"`
	Voters                      []string `json:"voters"`
	Required                    int      `json:"required,omitempty"`
}

// RemotePartPublicationProposal binds a remote-part reference to one
// manifest snapshot and fencing token. The candidate fingerprint is derived
// from all publication identity fields and the complete reference.
type RemotePartPublicationProposal struct {
	PublicationID               string              `json:"publication_id"`
	Reference                   RemotePartReference `json:"reference"`
	ExpectedManifestFingerprint string              `json:"expected_manifest_fingerprint"`
	FencingToken                uint64              `json:"fencing_token"`
	Voters                      []string            `json:"voters"`
	Required                    int                 `json:"required"`
	candidateFingerprint        string
}

// CandidateFingerprint returns the fingerprint that voters must acknowledge.
func (proposal RemotePartPublicationProposal) CandidateFingerprint() string {
	return proposal.candidateFingerprint
}

// RemotePartPublicationVote is a response from one publication voter. A vote
// is counted only if every identity field matches the proposal.
type RemotePartPublicationVote struct {
	NodeID                      string `json:"node_id"`
	PublicationID               string `json:"publication_id"`
	ExpectedManifestFingerprint string `json:"expected_manifest_fingerprint"`
	CandidateFingerprint        string `json:"candidate_fingerprint"`
	FencingToken                uint64 `json:"fencing_token"`
	Accepted                    bool   `json:"accepted"`
}

// RemotePartPublicationDecision is the deterministic result of collecting
// votes. It is safe to persist or send over another control-plane protocol.
type RemotePartPublicationDecision struct {
	PublicationID               string   `json:"publication_id"`
	ExpectedManifestFingerprint string   `json:"expected_manifest_fingerprint"`
	CandidateFingerprint        string   `json:"candidate_fingerprint"`
	FencingToken                uint64   `json:"fencing_token"`
	Voters                      []string `json:"voters"`
	Required                    int      `json:"required"`
	Acknowledged                []string `json:"acknowledged,omitempty"`
	Rejected                    []string `json:"rejected,omitempty"`
	Satisfied                   bool     `json:"satisfied"`
}

// RemotePartPublicationStore makes a remote part visible after the caller
// has collected a valid quorum. Implementations own durability and must not
// expose the part before PublishRemotePart returns successfully.
type RemotePartPublicationStore interface {
	PublishRemotePart(context.Context, RemotePartReference, uint64) error
}

// RemotePartPublicationResult reports the decision and whether the store
// completed the visibility transition.
type RemotePartPublicationResult struct {
	Decision  RemotePartPublicationDecision `json:"decision"`
	Published bool                          `json:"published"`
}

// NewRemotePartPublicationProposal validates and snapshots a publication
// request. It normalizes voter order so fingerprints and decisions are
// deterministic regardless of transport order.
func NewRemotePartPublicationProposal(reference RemotePartReference, options RemotePartPublicationOptions) (RemotePartPublicationProposal, error) {
	normalizedReference, err := normalizeRemotePartReference(reference)
	if err != nil {
		return RemotePartPublicationProposal{}, err
	}
	publicationID := strings.TrimSpace(options.PublicationID)
	manifestFingerprint := strings.TrimSpace(options.ExpectedManifestFingerprint)
	if publicationID == "" {
		return RemotePartPublicationProposal{}, fmt.Errorf("%w: publication id is empty", ErrRemotePartPublicationInvalid)
	}
	if manifestFingerprint == "" {
		return RemotePartPublicationProposal{}, fmt.Errorf("%w: expected manifest fingerprint is empty", ErrRemotePartPublicationInvalid)
	}
	if options.FencingToken == 0 {
		return RemotePartPublicationProposal{}, fmt.Errorf("%w: fencing token must be positive", ErrRemotePartPublicationInvalid)
	}
	voters, required, err := normalizeRemotePartPublicationVoters(options.Voters, options.Required)
	if err != nil {
		return RemotePartPublicationProposal{}, err
	}
	proposal := RemotePartPublicationProposal{
		PublicationID:               publicationID,
		Reference:                   normalizedReference,
		ExpectedManifestFingerprint: manifestFingerprint,
		FencingToken:                options.FencingToken,
		Voters:                      voters,
		Required:                    required,
	}
	proposal.candidateFingerprint = remotePartPublicationFingerprint(proposal)
	return proposal, nil
}

// EvaluateRemotePartPublication validates votes and returns their deterministic
// quorum decision. An unsatisfied quorum is returned with its decision so the
// caller can record rejected and missing acknowledgements.
func EvaluateRemotePartPublication(proposal RemotePartPublicationProposal, votes []RemotePartPublicationVote) (RemotePartPublicationDecision, error) {
	if err := validateRemotePartPublicationProposal(proposal); err != nil {
		return RemotePartPublicationDecision{}, err
	}
	return evaluateValidatedRemotePartPublication(proposal, votes)
}

func evaluateValidatedRemotePartPublication(proposal RemotePartPublicationProposal, votes []RemotePartPublicationVote) (RemotePartPublicationDecision, error) {
	voterSet := make(map[string]struct{}, len(proposal.Voters))
	for _, voter := range proposal.Voters {
		voterSet[voter] = struct{}{}
	}
	voteByNode := make(map[string]RemotePartPublicationVote, len(votes))
	for _, vote := range votes {
		nodeID := strings.TrimSpace(vote.NodeID)
		if nodeID == "" {
			return RemotePartPublicationDecision{}, fmt.Errorf("%w: vote node id is empty", ErrRemotePartPublicationInvalid)
		}
		if _, ok := voterSet[nodeID]; !ok {
			return RemotePartPublicationDecision{}, fmt.Errorf("%w: %q is not a voter", ErrRemotePartPublicationInvalid, nodeID)
		}
		if _, ok := voteByNode[nodeID]; ok {
			return RemotePartPublicationDecision{}, fmt.Errorf("%w: duplicate vote from %q", ErrRemotePartPublicationInvalid, nodeID)
		}
		if strings.TrimSpace(vote.PublicationID) != proposal.PublicationID ||
			strings.TrimSpace(vote.ExpectedManifestFingerprint) != proposal.ExpectedManifestFingerprint ||
			strings.TrimSpace(vote.CandidateFingerprint) != proposal.candidateFingerprint ||
			vote.FencingToken != proposal.FencingToken {
			return RemotePartPublicationDecision{}, fmt.Errorf("%w: vote from %q is bound to another proposal", ErrRemotePartPublicationInvalid, nodeID)
		}
		voteByNode[nodeID] = vote
	}

	acknowledged := make([]string, 0, len(voteByNode))
	rejected := make([]string, 0, len(voteByNode))
	for _, voter := range proposal.Voters {
		vote, ok := voteByNode[voter]
		if !ok {
			continue
		}
		if vote.Accepted {
			acknowledged = append(acknowledged, voter)
		} else {
			rejected = append(rejected, voter)
		}
	}
	decision := RemotePartPublicationDecision{
		PublicationID:               proposal.PublicationID,
		ExpectedManifestFingerprint: proposal.ExpectedManifestFingerprint,
		CandidateFingerprint:        proposal.candidateFingerprint,
		FencingToken:                proposal.FencingToken,
		Voters:                      append([]string(nil), proposal.Voters...),
		Required:                    proposal.Required,
		Acknowledged:                acknowledged,
		Rejected:                    rejected,
		Satisfied:                   len(acknowledged) >= proposal.Required,
	}
	if !decision.Satisfied {
		return decision, ErrRemotePartPublicationQuorumUnsatisfied
	}
	return decision, nil
}

// PublishRemotePartAfterQuorum makes one remote part visible only after a
// proposal has a valid quorum and a fencing token newer than lastFencingToken.
// The store call is deliberately injected: this function does not discover
// peers, retry network operations, or attempt rollback.
func PublishRemotePartAfterQuorum(ctx context.Context, store RemotePartPublicationStore, proposal RemotePartPublicationProposal, votes []RemotePartPublicationVote, lastFencingToken uint64) (RemotePartPublicationResult, error) {
	if ctx == nil {
		return RemotePartPublicationResult{}, ErrRemotePartPublicationContextRequired
	}
	if store == nil {
		return RemotePartPublicationResult{}, ErrRemotePartPublicationStoreRequired
	}
	if err := validateRemotePartPublicationProposal(proposal); err != nil {
		return RemotePartPublicationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return RemotePartPublicationResult{}, err
	}
	if proposal.FencingToken <= lastFencingToken {
		return RemotePartPublicationResult{}, fmt.Errorf("%w: token %d is not newer than %d", ErrRemotePartPublicationStale, proposal.FencingToken, lastFencingToken)
	}
	decision, err := evaluateValidatedRemotePartPublication(proposal, votes)
	result := RemotePartPublicationResult{Decision: decision}
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := store.PublishRemotePart(ctx, proposal.Reference, proposal.FencingToken); err != nil {
		return result, fmt.Errorf("publish remote part: %w", err)
	}
	result.Published = true
	return result, nil
}

func normalizeRemotePartReference(reference RemotePartReference) (RemotePartReference, error) {
	normalized, err := NewRemotePartReference(
		reference.ObjectURI(),
		reference.LocalMetadataPath(),
		reference.Checksum(),
		reference.SizeBytes(),
	)
	if err != nil {
		return RemotePartReference{}, fmt.Errorf("%w: reference: %v", ErrRemotePartPublicationInvalid, err)
	}
	return normalized, nil
}

func normalizeRemotePartPublicationVoters(voters []string, required int) ([]string, int, error) {
	if len(voters) == 0 {
		return nil, 0, fmt.Errorf("%w: voters are empty", ErrRemotePartPublicationInvalid)
	}
	normalized := make([]string, 0, len(voters))
	seen := make(map[string]struct{}, len(voters))
	for _, voter := range voters {
		voter = strings.TrimSpace(voter)
		if voter == "" {
			return nil, 0, fmt.Errorf("%w: voter id is empty", ErrRemotePartPublicationInvalid)
		}
		if _, ok := seen[voter]; ok {
			return nil, 0, fmt.Errorf("%w: duplicate voter %q", ErrRemotePartPublicationInvalid, voter)
		}
		seen[voter] = struct{}{}
		normalized = append(normalized, voter)
	}
	sort.Strings(normalized)
	if required == 0 {
		required = len(normalized)/2 + 1
	}
	if required < 1 || required > len(normalized) {
		return nil, 0, fmt.Errorf("%w: required acknowledgements %d outside 1..%d", ErrRemotePartPublicationInvalid, required, len(normalized))
	}
	return normalized, required, nil
}

func validateRemotePartPublicationProposal(proposal RemotePartPublicationProposal) error {
	if strings.TrimSpace(proposal.PublicationID) == "" {
		return fmt.Errorf("%w: publication id is empty", ErrRemotePartPublicationInvalid)
	}
	if strings.TrimSpace(proposal.ExpectedManifestFingerprint) == "" {
		return fmt.Errorf("%w: expected manifest fingerprint is empty", ErrRemotePartPublicationInvalid)
	}
	if proposal.FencingToken == 0 {
		return fmt.Errorf("%w: fencing token must be positive", ErrRemotePartPublicationInvalid)
	}
	voters, required, err := normalizeRemotePartPublicationVoters(proposal.Voters, proposal.Required)
	if err != nil {
		return err
	}
	if !sameStringSlice(voters, proposal.Voters) || required != proposal.Required {
		return fmt.Errorf("%w: proposal voters or quorum is not normalized", ErrRemotePartPublicationInvalid)
	}
	if _, err := normalizeRemotePartReference(proposal.Reference); err != nil {
		return err
	}
	expectedFingerprint := remotePartPublicationFingerprint(proposal)
	if proposal.candidateFingerprint == "" || proposal.candidateFingerprint != expectedFingerprint {
		return fmt.Errorf("%w: candidate fingerprint does not match proposal", ErrRemotePartPublicationInvalid)
	}
	return nil
}

func remotePartPublicationFingerprint(proposal RemotePartPublicationProposal) string {
	hash := sha256.New()
	writeFingerprintString(hash, proposal.PublicationID)
	writeFingerprintString(hash, proposal.Reference.ObjectURI())
	writeFingerprintString(hash, proposal.Reference.LocalMetadataPath())
	writeFingerprintString(hash, proposal.Reference.Checksum())
	writeFingerprintString(hash, fmt.Sprintf("%d", proposal.Reference.SizeBytes()))
	writeFingerprintString(hash, proposal.ExpectedManifestFingerprint)
	var token [8]byte
	binary.BigEndian.PutUint64(token[:], proposal.FencingToken)
	_, _ = hash.Write(token[:])
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func writeFingerprintString(hash interface{ Write([]byte) (int, error) }, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(value))
}

func sameStringSlice(left, right []string) bool {
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
