package hatPipeline

import (
	"context"
	"errors"
	"sort"
)

var (
	ErrSnapshotFrontierRegistryRequired = errors.New("hatPipeline: snapshot frontier registry is required")
	ErrSnapshotFrontierSourcesEmpty     = errors.New("hatPipeline: snapshot frontier sources are empty")
	ErrSnapshotFrontierSourceEmpty      = errors.New("hatPipeline: snapshot frontier source ID is empty")
	ErrSnapshotFrontierSourceDuplicate  = errors.New("hatPipeline: snapshot frontier source ID is duplicated")
	ErrSnapshotFrontierSourceMissing    = errors.New("hatPipeline: snapshot frontier source is missing")
)

// SnapshotFrontierGateOptions describes the named frontiers that form one
// consistent snapshot boundary.
type SnapshotFrontierGateOptions struct {
	Registry  *FrontierRegistry
	SourceIDs []string
}

// SnapshotFrontier is a point-in-time observation in which every configured
// source has closed through Target.
type SnapshotFrontier struct {
	Target  uint64
	Sources []FrontierSnapshot
}

// SnapshotFrontierGate coordinates a fixed set of monotone source frontiers.
// It has no worker or background state; callers publish progress to Registry.
type SnapshotFrontierGate struct {
	registry  *FrontierRegistry
	sourceIDs []string
}

// NewSnapshotFrontierGate validates and freezes a set of registered sources.
// Source IDs are sorted so returned snapshots are deterministic.
func NewSnapshotFrontierGate(options SnapshotFrontierGateOptions) (*SnapshotFrontierGate, error) {
	if options.Registry == nil {
		return nil, ErrSnapshotFrontierRegistryRequired
	}
	if len(options.SourceIDs) == 0 {
		return nil, ErrSnapshotFrontierSourcesEmpty
	}

	sourceIDs := append([]string(nil), options.SourceIDs...)
	seen := make(map[string]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		if id == "" {
			return nil, ErrSnapshotFrontierSourceEmpty
		}
		if _, exists := seen[id]; exists {
			return nil, ErrSnapshotFrontierSourceDuplicate
		}
		seen[id] = struct{}{}
		if _, exists := options.Registry.Snapshot(id); !exists {
			return nil, ErrSnapshotFrontierSourceMissing
		}
	}
	sort.Strings(sourceIDs)
	return &SnapshotFrontierGate{
		registry:  options.Registry,
		sourceIDs: sourceIDs,
	}, nil
}

// Sources returns a copy of the sorted source IDs used by the gate.
func (gate *SnapshotFrontierGate) Sources() []string {
	if gate == nil {
		return []string{}
	}
	return append([]string(nil), gate.sourceIDs...)
}

// Ready reports whether every configured source has closed through target.
func (gate *SnapshotFrontierGate) Ready(target uint64) bool {
	if gate == nil || gate.registry == nil {
		return false
	}
	for _, id := range gate.sourceIDs {
		snapshot, ok := gate.registry.Snapshot(id)
		if !ok || snapshot.Lower < target {
			return false
		}
	}
	return true
}

// Snapshot returns an isolated snapshot when every source has reached target.
func (gate *SnapshotFrontierGate) Snapshot(target uint64) (SnapshotFrontier, bool) {
	if gate == nil || gate.registry == nil {
		return SnapshotFrontier{}, false
	}
	sources := make([]FrontierSnapshot, len(gate.sourceIDs))
	for index, id := range gate.sourceIDs {
		snapshot, ok := gate.registry.Snapshot(id)
		if !ok || snapshot.Lower < target {
			return SnapshotFrontier{}, false
		}
		sources[index] = snapshot
	}
	return SnapshotFrontier{Target: target, Sources: sources}, true
}

// WaitUntil blocks until every source has closed through target, then returns
// the corresponding source snapshots. No goroutine is retained by the gate.
func (gate *SnapshotFrontierGate) WaitUntil(ctx context.Context, target uint64) (SnapshotFrontier, error) {
	if gate == nil || gate.registry == nil {
		return SnapshotFrontier{}, ErrSnapshotFrontierRegistryRequired
	}
	for _, id := range gate.sourceIDs {
		if err := gate.registry.WaitUntil(ctx, id, target); err != nil {
			return SnapshotFrontier{}, err
		}
	}
	sources := make([]FrontierSnapshot, len(gate.sourceIDs))
	for index, id := range gate.sourceIDs {
		snapshot, ok := gate.registry.Snapshot(id)
		if !ok || snapshot.Lower < target {
			return SnapshotFrontier{}, ErrSnapshotFrontierSourceMissing
		}
		sources[index] = snapshot
	}
	return SnapshotFrontier{Target: target, Sources: sources}, nil
}
