package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

var (
	ErrRemotePartGCInvalid         = errors.New("hatriecache: remote-part garbage-collection plan is invalid")
	ErrRemotePartGCContextRequired = errors.New("hatriecache: remote-part garbage-collection context is required")
	ErrRemotePartGCStoreRequired   = errors.New("hatriecache: remote-part garbage-collection store is required")
)

const (
	// DefaultRemotePartGCRetention protects recently listed objects from a
	// reachability race between manifest publication and object listing.
	DefaultRemotePartGCRetention time.Duration = 24 * time.Hour
	// DefaultRemotePartGCMaxCandidates bounds one reviewed deletion plan.
	DefaultRemotePartGCMaxCandidates = 10_000
	// MaxRemotePartGCCandidates is the hard upper bound for one plan.
	MaxRemotePartGCCandidates = 1_000_000
)

// RemotePartGCObject is one object returned by a caller-owned remote listing.
// LastModified is required because age retention is the safety barrier for an
// eventually consistent manifest/listing view.
type RemotePartGCObject struct {
	ObjectURI    string
	SizeBytes    uint64
	LastModified time.Time
}

// RemotePartGCOptions controls a deterministic reachability plan. A zero
// MinAge uses DefaultRemotePartGCRetention, a zero Now uses the current UTC
// time, and a zero MaxCandidates uses DefaultRemotePartGCMaxCandidates.
type RemotePartGCOptions struct {
	MinAge        time.Duration
	Now           time.Time
	MaxCandidates int
}

// RemotePartGCCandidate is an unreachable object old enough for explicit
// deletion. Candidates are sorted by canonical URI in the returned plan.
type RemotePartGCCandidate struct {
	ObjectURI    string
	SizeBytes    uint64
	LastModified time.Time
	Age          time.Duration
}

// RemotePartGCPlan is a dry-run deletion plan. It contains no store handle and
// performs no network or filesystem operation. Callers should persist, review,
// authorize, or discard it before passing it to ExecuteRemotePartGarbageCollection.
type RemotePartGCPlan struct {
	AsOf             time.Time
	MinAge           time.Duration
	ListedObjects    int
	ReachableObjects int
	Candidates       []RemotePartGCCandidate
	ReclaimableBytes uint64
}

// RemotePartGCStore is the minimal destructive operation needed to apply a
// reviewed plan. Listing and manifest reads intentionally remain caller-owned.
type RemotePartGCStore interface {
	DeleteRemotePart(context.Context, string) error
}

// RemotePartGCResult reports the durable progress of one sequential apply.
// FailedObjectURI is empty on success or when cancellation occurs before a
// delete starts.
type RemotePartGCResult struct {
	DeletedObjects  int
	DeletedBytes    uint64
	FailedObjectURI string
}

// PlanRemotePartGarbageCollection creates a bounded, manifest-aware dry-run
// plan. Reachable references should come from all live manifests; listed must
// come from the caller's remote object store. Duplicate identical listings are
// collapsed, while conflicting metadata is rejected conservatively.
func PlanRemotePartGarbageCollection(reachable []RemotePartReference, listed []RemotePartGCObject, options RemotePartGCOptions) (RemotePartGCPlan, error) {
	asOf := options.Now
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	} else {
		asOf = asOf.UTC()
	}
	minAge := options.MinAge
	if minAge == 0 {
		minAge = DefaultRemotePartGCRetention
	}
	if minAge < 0 {
		return RemotePartGCPlan{}, fmt.Errorf("%w: negative retention", ErrRemotePartGCInvalid)
	}
	maxCandidates := options.MaxCandidates
	if maxCandidates == 0 {
		maxCandidates = DefaultRemotePartGCMaxCandidates
	}
	if maxCandidates < 1 || maxCandidates > MaxRemotePartGCCandidates {
		return RemotePartGCPlan{}, fmt.Errorf("%w: max candidates must be between 1 and %d", ErrRemotePartGCInvalid, MaxRemotePartGCCandidates)
	}

	reachableURIs := make(map[string]struct{}, len(reachable))
	for _, reference := range reachable {
		objectURI, err := normalizeRemotePartGCURI(reference.ObjectURI(), reference.SizeBytes())
		if err != nil {
			return RemotePartGCPlan{}, fmt.Errorf("%w: reachable URI: %v", ErrRemotePartGCInvalid, err)
		}
		reachableURIs[objectURI] = struct{}{}
	}

	listedCandidates := make([]RemotePartGCCandidate, len(listed))
	for index, object := range listed {
		objectURI, err := normalizeRemotePartGCURI(object.ObjectURI, object.SizeBytes)
		if err != nil {
			return RemotePartGCPlan{}, fmt.Errorf("%w: listed URI: %v", ErrRemotePartGCInvalid, err)
		}
		if object.LastModified.IsZero() {
			return RemotePartGCPlan{}, fmt.Errorf("%w: listed object %q has no last-modified time", ErrRemotePartGCInvalid, objectURI)
		}
		listedCandidates[index] = RemotePartGCCandidate{
			ObjectURI:    objectURI,
			SizeBytes:    object.SizeBytes,
			LastModified: object.LastModified.UTC(),
		}
	}
	sort.Slice(listedCandidates, func(left, right int) bool {
		return listedCandidates[left].ObjectURI < listedCandidates[right].ObjectURI
	})

	plan := RemotePartGCPlan{
		AsOf:             asOf,
		MinAge:           minAge,
		ListedObjects:    0,
		ReachableObjects: len(reachableURIs),
		Candidates:       listedCandidates[:0],
	}
	for index := 0; index < len(listedCandidates); {
		groupEnd := index + 1
		for groupEnd < len(listedCandidates) && listedCandidates[groupEnd].ObjectURI == listedCandidates[index].ObjectURI {
			if listedCandidates[groupEnd].SizeBytes != listedCandidates[index].SizeBytes || !listedCandidates[groupEnd].LastModified.Equal(listedCandidates[index].LastModified) {
				return RemotePartGCPlan{}, fmt.Errorf("%w: conflicting duplicate listing for %q", ErrRemotePartGCInvalid, listedCandidates[index].ObjectURI)
			}
			groupEnd++
		}
		plan.ListedObjects++
		candidate := listedCandidates[index]
		index = groupEnd
		if _, live := reachableURIs[candidate.ObjectURI]; live {
			continue
		}
		candidate.Age = asOf.Sub(candidate.LastModified)
		if candidate.Age < minAge {
			continue
		}
		if len(plan.Candidates) >= maxCandidates {
			return RemotePartGCPlan{}, fmt.Errorf("%w: candidate count exceeds %d", ErrRemotePartGCInvalid, maxCandidates)
		}
		plan.Candidates = append(plan.Candidates, candidate)
		if math.MaxUint64-plan.ReclaimableBytes < candidate.SizeBytes {
			return RemotePartGCPlan{}, fmt.Errorf("%w: reclaimable byte count overflows uint64", ErrRemotePartGCInvalid)
		}
		plan.ReclaimableBytes += candidate.SizeBytes
	}
	return plan, nil
}

// ExecuteRemotePartGarbageCollection applies a previously created plan in URI
// order. It validates the complete plan before the first delete, honors
// cancellation between deletes, and leaves already-deleted progress in the
// result when a store operation fails. It never discovers or deletes anything
// outside the supplied plan.
func ExecuteRemotePartGarbageCollection(ctx context.Context, store RemotePartGCStore, plan RemotePartGCPlan) (RemotePartGCResult, error) {
	result := RemotePartGCResult{}
	if ctx == nil {
		return result, ErrRemotePartGCContextRequired
	}
	if store == nil {
		return result, ErrRemotePartGCStoreRequired
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := validateRemotePartGCPlan(plan); err != nil {
		return result, err
	}
	for _, candidate := range plan.Candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := store.DeleteRemotePart(ctx, candidate.ObjectURI); err != nil {
			result.FailedObjectURI = candidate.ObjectURI
			return result, fmt.Errorf("delete remote part %q: %w", candidate.ObjectURI, err)
		}
		result.DeletedObjects++
		result.DeletedBytes += candidate.SizeBytes
	}
	return result, nil
}

func normalizeRemotePartGCURI(objectURI string, sizeBytes uint64) (string, error) {
	trimmed := strings.TrimSpace(objectURI)
	if trimmed == objectURI && isCanonicalRemotePartGCURI(objectURI) {
		return objectURI, nil
	}
	reference, err := NewRemotePartReference(objectURI, "gc/object.json", "sha256:remote-part-gc", sizeBytes)
	if err != nil {
		return "", err
	}
	return reference.ObjectURI(), nil
}

func isCanonicalRemotePartGCURI(objectURI string) bool {
	if objectURI == "" || strings.ContainsAny(objectURI, "\t\r\n #%\\[]\"<>") {
		return false
	}
	schemeEnd := strings.Index(objectURI, "://")
	if schemeEnd <= 0 {
		return false
	}
	switch objectURI[:schemeEnd] {
	case "s3", "gs", "az", "http", "https":
	default:
		return false
	}
	authorityStart := schemeEnd + len("://")
	pathStart := strings.IndexAny(objectURI[authorityStart:], "/?#")
	if pathStart < 1 {
		return false
	}
	pathStart += authorityStart
	if objectURI[pathStart] != '/' || strings.Contains(objectURI[authorityStart:pathStart], "@") {
		return false
	}
	pathEnd := strings.IndexAny(objectURI[pathStart:], "?#")
	if pathEnd < 0 {
		pathEnd = len(objectURI)
	} else {
		pathEnd += pathStart
	}
	return strings.Trim(objectURI[pathStart:pathEnd], "/") != ""
}

func validateRemotePartGCPlan(plan RemotePartGCPlan) error {
	if plan.AsOf.IsZero() || plan.MinAge < 0 || len(plan.Candidates) > MaxRemotePartGCCandidates {
		return fmt.Errorf("%w: invalid plan bounds", ErrRemotePartGCInvalid)
	}
	var reclaimable uint64
	previousURI := ""
	for index, candidate := range plan.Candidates {
		objectURI, err := normalizeRemotePartGCURI(candidate.ObjectURI, candidate.SizeBytes)
		if err != nil || objectURI != candidate.ObjectURI {
			return fmt.Errorf("%w: invalid candidate URI at index %d", ErrRemotePartGCInvalid, index)
		}
		if candidate.LastModified.IsZero() || candidate.LastModified.Location() != time.UTC {
			return fmt.Errorf("%w: candidate %q has non-canonical timestamp", ErrRemotePartGCInvalid, candidate.ObjectURI)
		}
		if candidate.Age < plan.MinAge || candidate.Age != plan.AsOf.Sub(candidate.LastModified) {
			return fmt.Errorf("%w: candidate %q has invalid age", ErrRemotePartGCInvalid, candidate.ObjectURI)
		}
		if index > 0 && candidate.ObjectURI <= previousURI {
			return fmt.Errorf("%w: candidates are not strictly sorted or contain duplicates", ErrRemotePartGCInvalid)
		}
		previousURI = candidate.ObjectURI
		if math.MaxUint64-reclaimable < candidate.SizeBytes {
			return fmt.Errorf("%w: reclaimable byte count overflows uint64", ErrRemotePartGCInvalid)
		}
		reclaimable += candidate.SizeBytes
	}
	if reclaimable != plan.ReclaimableBytes {
		return fmt.Errorf("%w: reclaimable byte accounting mismatch", ErrRemotePartGCInvalid)
	}
	return nil
}
