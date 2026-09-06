package hatStorage

import (
	"errors"
	"sort"
)

var (
	ErrPartCachePolicyInvalid    = errors.New("hatriecache: part cache policy is invalid")
	ErrPartCacheCandidateInvalid = errors.New("hatriecache: part cache candidate is invalid")
	ErrPartCacheCapacityExceeded = errors.New("hatriecache: part cache capacity is exceeded")
)

// PartCacheCandidate describes one immutable part that may be retained in a
// cache. Accesses is the observed frequency and LastAccess is a caller-owned
// monotone recency value.
type PartCacheCandidate struct {
	Key        string
	SizeBytes  uint64
	Accesses   uint64
	LastAccess uint64
}

// PartCachePolicy contains admission and eviction limits. It has no hidden
// cache state; callers remain responsible for storing or deleting parts.
type PartCachePolicy struct {
	CapacityBytes uint64
	MinAccesses   uint64
}

// NewPartCachePolicy validates a byte capacity and minimum admission count.
func NewPartCachePolicy(capacityBytes, minAccesses uint64) (PartCachePolicy, error) {
	if capacityBytes == 0 {
		return PartCachePolicy{}, ErrPartCachePolicyInvalid
	}
	return PartCachePolicy{CapacityBytes: capacityBytes, MinAccesses: minAccesses}, nil
}

// Admit reports whether a candidate fits and has reached the configured
// access threshold. It performs no allocation and does not mutate state.
func (policy PartCachePolicy) Admit(candidate PartCacheCandidate) bool {
	return policy.CapacityBytes != 0 &&
		candidate.Key != "" &&
		candidate.SizeBytes != 0 &&
		candidate.SizeBytes <= policy.CapacityBytes &&
		candidate.Accesses >= policy.MinAccesses
}

// PlanEvictions returns the smallest deterministic prefix of eviction
// candidates that makes room for incomingBytes. Candidates are ordered by
// least access frequency, oldest access, largest size, and finally key. The
// input slice is never modified.
func (policy PartCachePolicy) PlanEvictions(entries []PartCacheCandidate, incomingBytes uint64) ([]PartCacheCandidate, error) {
	if policy.CapacityBytes == 0 {
		return nil, ErrPartCachePolicyInvalid
	}
	if incomingBytes > policy.CapacityBytes {
		return nil, ErrPartCacheCapacityExceeded
	}
	seen := make(map[string]struct{}, len(entries))
	var totalBytes uint64
	for _, entry := range entries {
		if entry.Key == "" || entry.SizeBytes == 0 {
			return nil, ErrPartCacheCandidateInvalid
		}
		if _, exists := seen[entry.Key]; exists {
			return nil, ErrPartCacheCandidateInvalid
		}
		seen[entry.Key] = struct{}{}
		if totalBytes > ^uint64(0)-entry.SizeBytes {
			return nil, ErrPartCacheCandidateInvalid
		}
		totalBytes += entry.SizeBytes
	}
	available := policy.CapacityBytes - incomingBytes
	if totalBytes <= available {
		return nil, nil
	}
	needed := totalBytes - available
	ordered := make([]PartCacheCandidate, len(entries))
	copy(ordered, entries)
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].Accesses != ordered[right].Accesses {
			return ordered[left].Accesses < ordered[right].Accesses
		}
		if ordered[left].LastAccess != ordered[right].LastAccess {
			return ordered[left].LastAccess < ordered[right].LastAccess
		}
		if ordered[left].SizeBytes != ordered[right].SizeBytes {
			return ordered[left].SizeBytes > ordered[right].SizeBytes
		}
		return ordered[left].Key < ordered[right].Key
	})
	plan := make([]PartCacheCandidate, 0, len(ordered))
	var freed uint64
	for _, candidate := range ordered {
		plan = append(plan, candidate)
		freed += candidate.SizeBytes
		if freed >= needed {
			return plan, nil
		}
	}
	return nil, ErrPartCacheCapacityExceeded
}
