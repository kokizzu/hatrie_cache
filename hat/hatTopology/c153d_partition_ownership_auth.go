package hatTopology

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const partitionOwnershipConsensusAuthenticationDomain = "hatrie-cache/partition-ownership-consensus-v1"

var ErrPartitionOwnershipConsensusAuthentication = errors.New("hatriecache: partition ownership consensus authentication failed")

// PartitionOwnershipConsensusAuthenticator signs and verifies ownership votes
// for one caller-owned control-plane key. It is opt-in and does not alter the
// legacy unsigned evaluator.
type PartitionOwnershipConsensusAuthenticator struct {
	keyID string
	key   []byte
}

// NewPartitionOwnershipConsensusAuthenticator creates an HMAC-SHA256 vote
// authenticator. Keys must be at least 32 bytes so control-plane deployments
// do not accidentally configure a weak short secret.
func NewPartitionOwnershipConsensusAuthenticator(keyID string, key []byte) (*PartitionOwnershipConsensusAuthenticator, error) {
	trimmedKeyID := strings.TrimSpace(keyID)
	if trimmedKeyID == "" || keyID != trimmedKeyID || len(key) < sha256.Size {
		return nil, ErrPartitionOwnershipConsensusAuthentication
	}
	return &PartitionOwnershipConsensusAuthenticator{keyID: trimmedKeyID, key: append([]byte(nil), key...)}, nil
}

// Sign adds the authenticator key ID and deterministic HMAC signature to a
// canonical ownership vote. The returned vote owns an independent signature
// slice and can be transferred by a caller-owned transport.
func (authenticator *PartitionOwnershipConsensusAuthenticator) Sign(vote PartitionOwnershipConsensusVote) (PartitionOwnershipConsensusVote, error) {
	if authenticator == nil || len(authenticator.key) < sha256.Size {
		return PartitionOwnershipConsensusVote{}, ErrPartitionOwnershipConsensusAuthentication
	}
	if err := validateAuthenticatedPartitionOwnershipConsensusVote(vote); err != nil {
		return PartitionOwnershipConsensusVote{}, err
	}
	vote.KeyID = authenticator.keyID
	vote.Signature = authenticator.signature(vote)
	return vote, nil
}

// Verify checks the key ID, canonical vote fields, and HMAC signature.
func (authenticator *PartitionOwnershipConsensusAuthenticator) Verify(vote PartitionOwnershipConsensusVote) error {
	if authenticator == nil || len(authenticator.key) < sha256.Size {
		return ErrPartitionOwnershipConsensusAuthentication
	}
	if err := validateAuthenticatedPartitionOwnershipConsensusVote(vote); err != nil {
		return err
	}
	if vote.KeyID != authenticator.keyID || len(vote.Signature) != sha256.Size {
		return ErrPartitionOwnershipConsensusAuthentication
	}
	if !hmac.Equal(vote.Signature, authenticator.signature(vote)) {
		return ErrPartitionOwnershipConsensusAuthentication
	}
	return nil
}

// EvaluateAuthenticatedPartitionOwnershipConsensus verifies every supplied
// vote before applying the existing deterministic quorum evaluator. An invalid
// or unsigned vote is an input error rather than a rejected quorum vote.
func EvaluateAuthenticatedPartitionOwnershipConsensus(policy TopologyConsensusPolicy, expected PartitionOwnership, votes []PartitionOwnershipConsensusVote, authenticator *PartitionOwnershipConsensusAuthenticator) (PartitionOwnershipConsensusDecision, error) {
	if authenticator == nil {
		return PartitionOwnershipConsensusDecision{}, ErrPartitionOwnershipConsensusAuthentication
	}
	for _, vote := range votes {
		if err := authenticator.Verify(vote); err != nil {
			return PartitionOwnershipConsensusDecision{}, fmt.Errorf("%w: vote from %q: %v", ErrPartitionOwnershipConsensusAuthentication, vote.NodeID, err)
		}
	}
	return EvaluatePartitionOwnershipConsensus(policy, expected, votes)
}

func (authenticator *PartitionOwnershipConsensusAuthenticator) signature(vote PartitionOwnershipConsensusVote) []byte {
	payload := make([]byte, 0, 128+len(vote.NodeID)+len(vote.KeyID)+len(vote.Ownership.Primary)+len(vote.Ownership.TopologyFingerprint))
	payload = append(payload, partitionOwnershipConsensusAuthenticationDomain...)
	payload = appendPartitionOwnershipConsensusAuthString(payload, vote.KeyID)
	payload = appendPartitionOwnershipConsensusAuthString(payload, vote.NodeID)
	payload = appendPartitionOwnershipConsensusAuthUint32(payload, vote.Ownership.ShardID)
	payload = appendPartitionOwnershipConsensusAuthUint64(payload, vote.Ownership.FencingToken)
	payload = appendPartitionOwnershipConsensusAuthString(payload, vote.Ownership.Primary)
	payload = appendPartitionOwnershipConsensusAuthString(payload, vote.Ownership.TopologyFingerprint)
	payload = appendPartitionOwnershipConsensusAuthUint64(payload, uint64(len(vote.Ownership.Replicas)))
	for _, replica := range vote.Ownership.Replicas {
		payload = appendPartitionOwnershipConsensusAuthString(payload, replica)
	}
	if vote.Accepted {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	mac := hmac.New(sha256.New, authenticator.key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func validateAuthenticatedPartitionOwnershipConsensusVote(vote PartitionOwnershipConsensusVote) error {
	if vote.NodeID == "" || vote.NodeID != strings.TrimSpace(vote.NodeID) {
		return ErrPartitionOwnershipConsensusAuthentication
	}
	if vote.KeyID != "" && vote.KeyID != strings.TrimSpace(vote.KeyID) {
		return ErrPartitionOwnershipConsensusAuthentication
	}
	if err := validatePartitionOwnershipConsensusMetadata(vote.Ownership); err != nil {
		return fmt.Errorf("%w: %v", ErrPartitionOwnershipConsensusAuthentication, err)
	}
	return nil
}

func appendPartitionOwnershipConsensusAuthString(payload []byte, value string) []byte {
	payload = appendPartitionOwnershipConsensusAuthUint64(payload, uint64(len(value)))
	return append(payload, value...)
}

func appendPartitionOwnershipConsensusAuthUint32(payload []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(payload, encoded[:]...)
}

func appendPartitionOwnershipConsensusAuthUint64(payload []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(payload, encoded[:]...)
}
