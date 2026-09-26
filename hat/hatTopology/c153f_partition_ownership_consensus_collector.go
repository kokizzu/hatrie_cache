package hatTopology

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

const (
	// DefaultPartitionOwnershipConsensusMaxConcurrent bounds concurrent
	// caller-owned control-plane requests when no explicit limit is supplied.
	DefaultPartitionOwnershipConsensusMaxConcurrent = 8
	// MaxPartitionOwnershipConsensusCollectorVoters prevents an accidental
	// unbounded worker request from turning a malformed policy into a resource
	// exhaustion vector.
	MaxPartitionOwnershipConsensusCollectorVoters  = 4096
	maxPartitionOwnershipConsensusCollectorWorkers = 64
)

var (
	ErrPartitionOwnershipConsensusCollectorInvalid = errors.New("hatriecache: invalid partition ownership consensus collector")
)

// PartitionOwnershipConsensusVoteFetcher obtains one vote from one named
// voter. Implementations normally wrap an HTTP or gRPC request and must honor
// ctx so quorum completion and caller cancellation can stop outstanding work.
type PartitionOwnershipConsensusVoteFetcher func(ctx context.Context, voter string, expected PartitionOwnership) (PartitionOwnershipConsensusVote, error)

// PartitionOwnershipConsensusCollectorOptions configures the opt-in
// transport-neutral vote collector. A nil Authenticator preserves the legacy
// unsigned vote path; a non-nil authenticator verifies every returned vote
// before it can contribute to quorum.
type PartitionOwnershipConsensusCollectorOptions struct {
	MaxConcurrent int
	Authenticator *PartitionOwnershipConsensusAuthenticator
}

// PartitionOwnershipConsensusVoteFailure records a transport or validation
// failure for one voter. Missing or failed votes do not count toward quorum,
// matching EvaluatePartitionOwnershipConsensus semantics.
type PartitionOwnershipConsensusVoteFailure struct {
	NodeID string
	Err    error
}

// PartitionOwnershipConsensusCollection contains the deterministic quorum
// decision, valid votes, and per-voter failures collected by
// CollectPartitionOwnershipConsensus.
type PartitionOwnershipConsensusCollection struct {
	Decision PartitionOwnershipConsensusDecision
	Votes    []PartitionOwnershipConsensusVote
	Failures []PartitionOwnershipConsensusVoteFailure
}

// CollectPartitionOwnershipConsensus concurrently obtains caller-owned
// transport votes and evaluates them with the existing exact metadata quorum
// rules. The worker count is bounded, voters are scheduled in canonical order,
// and context cancellation is sent to outstanding fetches as soon as quorum
// is reached. Per-voter transport or validation errors are reported in
// Failures rather than turning an otherwise valid partial quorum into a global
// error. Global input errors and caller context cancellation are returned.
func CollectPartitionOwnershipConsensus(
	ctx context.Context,
	policy TopologyConsensusPolicy,
	expected PartitionOwnership,
	fetch PartitionOwnershipConsensusVoteFetcher,
	options PartitionOwnershipConsensusCollectorOptions,
) (PartitionOwnershipConsensusCollection, error) {
	var result PartitionOwnershipConsensusCollection
	if ctx == nil || fetch == nil {
		return result, fmt.Errorf("%w: context and fetcher are required", ErrPartitionOwnershipConsensusCollectorInvalid)
	}
	voters, _, required, err := preparePartitionOwnershipConsensusVoters(policy)
	if err != nil {
		return result, err
	}
	if len(voters) > MaxPartitionOwnershipConsensusCollectorVoters {
		return result, fmt.Errorf("%w: voter count %d exceeds %d", ErrPartitionOwnershipConsensusCollectorInvalid, len(voters), MaxPartitionOwnershipConsensusCollectorVoters)
	}
	if err := validatePartitionOwnershipConsensusMetadata(expected); err != nil {
		return result, err
	}
	if options.MaxConcurrent < 0 || options.MaxConcurrent > maxPartitionOwnershipConsensusCollectorWorkers {
		return result, fmt.Errorf("%w: max concurrent must be between 0 and %d", ErrPartitionOwnershipConsensusCollectorInvalid, maxPartitionOwnershipConsensusCollectorWorkers)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	workers := options.MaxConcurrent
	if workers == 0 {
		workers = DefaultPartitionOwnershipConsensusMaxConcurrent
	}
	if workers > len(voters) {
		workers = len(voters)
	}

	type outcome struct {
		index int
		vote  PartitionOwnershipConsensusVote
		err   error
	}
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	outcomes := make(chan outcome, len(voters))
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer waitGroup.Done()
			for index := range jobs {
				voter := voters[index]
				vote, fetchErr := fetch(requestCtx, voter, clonePartitionOwnershipConsensusMetadata(expected))
				select {
				case outcomes <- outcome{index: index, vote: vote, err: fetchErr}:
				case <-requestCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for index := range voters {
			select {
			case jobs <- index:
			case <-requestCtx.Done():
				return
			}
		}
	}()
	go func() {
		waitGroup.Wait()
		close(outcomes)
	}()

	validVotes := make([]PartitionOwnershipConsensusVote, 0, len(voters))
	failures := make([]PartitionOwnershipConsensusVoteFailure, 0)
	acknowledged := 0
	quorumCanceled := false
	for item := range outcomes {
		voter := voters[item.index]
		if item.err != nil {
			if !quorumCanceled || !errors.Is(item.err, context.Canceled) {
				failures = append(failures, PartitionOwnershipConsensusVoteFailure{NodeID: voter, Err: item.err})
			}
			continue
		}
		vote := clonePartitionOwnershipConsensusVote(item.vote)
		if err := validateCollectedPartitionOwnershipConsensusVote(policy, expected, voter, vote, options.Authenticator); err != nil {
			failures = append(failures, PartitionOwnershipConsensusVoteFailure{NodeID: voter, Err: err})
			continue
		}
		validVotes = append(validVotes, vote)
		if vote.Accepted && partitionOwnershipConsensusMetadataEqual(expected, vote.Ownership) {
			acknowledged++
			if acknowledged >= required && !quorumCanceled {
				quorumCanceled = true
				cancel()
			}
		}
	}

	sort.Slice(validVotes, func(left, right int) bool { return validVotes[left].NodeID < validVotes[right].NodeID })
	sort.Slice(failures, func(left, right int) bool { return failures[left].NodeID < failures[right].NodeID })
	decision, err := EvaluatePartitionOwnershipConsensus(policy, expected, validVotes)
	if err != nil {
		return PartitionOwnershipConsensusCollection{}, err
	}
	result = PartitionOwnershipConsensusCollection{
		Decision: decision,
		Votes:    validVotes,
		Failures: failures,
	}
	if err := ctx.Err(); err != nil && !quorumCanceled {
		return result, err
	}
	return result, nil
}

func validateCollectedPartitionOwnershipConsensusVote(
	policy TopologyConsensusPolicy,
	expected PartitionOwnership,
	voter string,
	vote PartitionOwnershipConsensusVote,
	authenticator *PartitionOwnershipConsensusAuthenticator,
) error {
	if vote.NodeID != voter {
		return fmt.Errorf("%w: response identity %q does not match voter %q", ErrPartitionOwnershipConsensusCollectorInvalid, vote.NodeID, voter)
	}
	if authenticator != nil {
		if err := authenticator.Verify(vote); err != nil {
			return fmt.Errorf("%w: voter %q: %v", ErrPartitionOwnershipConsensusCollectorInvalid, voter, err)
		}
	}
	if _, err := EvaluatePartitionOwnershipConsensus(policy, expected, []PartitionOwnershipConsensusVote{vote}); err != nil {
		return fmt.Errorf("%w: voter %q: %v", ErrPartitionOwnershipConsensusCollectorInvalid, voter, err)
	}
	return nil
}

func clonePartitionOwnershipConsensusVote(vote PartitionOwnershipConsensusVote) PartitionOwnershipConsensusVote {
	vote.Ownership = clonePartitionOwnershipConsensusMetadata(vote.Ownership)
	vote.Signature = append([]byte(nil), vote.Signature...)
	return vote
}
