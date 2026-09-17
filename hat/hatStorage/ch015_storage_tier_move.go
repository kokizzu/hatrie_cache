package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrStorageTierMoveInvalid = errors.New("hatriecache: storage tier move is invalid")

// StorageTierPart describes one part's current tier and age. Key must be
// unique within one planning call so a move cannot be ambiguous.
type StorageTierPart struct {
	Key         string
	CurrentTier string
	Age         time.Duration
}

// StorageTierMove identifies the source and destination selected for one part.
// The executor owns the actual copy, fsync, rename, and metadata publication.
type StorageTierMove struct {
	Key             string
	SourceTier      string
	SourcePath      string
	DestinationTier string
	DestinationPath string
}

// StorageTierMoveExecutor applies one caller-owned move.
type StorageTierMoveExecutor func(context.Context, StorageTierMove) error

// StorageTierMoveReport records the planned and successfully applied moves.
// Moved can be smaller than Planned when execution is canceled or fails.
type StorageTierMoveReport struct {
	Planned int
	Moved   int
}

// PlanStorageTierMoves computes deterministic moves for parts whose current
// tier differs from the age-selected tier. It performs no I/O or mutation.
func (policy StorageTierPolicy) PlanStorageTierMoves(parts []StorageTierPart) ([]StorageTierMove, error) {
	moves := make([]StorageTierMove, 0, len(parts))
	seenKeys := make(map[string]struct{}, len(parts))
	for index, part := range parts {
		if strings.TrimSpace(part.Key) == "" {
			return nil, fmt.Errorf("%w: part %d key is required", ErrStorageTierMoveInvalid, index)
		}
		if _, exists := seenKeys[part.Key]; exists {
			return nil, fmt.Errorf("%w: duplicate part key %q", ErrStorageTierMoveInvalid, part.Key)
		}
		seenKeys[part.Key] = struct{}{}
		currentTier := strings.TrimSpace(part.CurrentTier)
		if currentTier == "" {
			return nil, fmt.Errorf("%w: part %q current tier is required", ErrStorageTierMoveInvalid, part.Key)
		}
		destination, err := policy.Select(part.Age, part.Key)
		if err != nil {
			return nil, fmt.Errorf("%w: part %q destination: %v", ErrStorageTierMoveInvalid, part.Key, err)
		}
		if currentTier == destination.Tier {
			continue
		}
		source, err := policy.selectStorageTier(currentTier, part.Key)
		if err != nil {
			return nil, fmt.Errorf("%w: part %q source: %v", ErrStorageTierMoveInvalid, part.Key, err)
		}
		moves = append(moves, StorageTierMove{
			Key:             part.Key,
			SourceTier:      source.Tier,
			SourcePath:      source.Path,
			DestinationTier: destination.Tier,
			DestinationPath: destination.Path,
		})
	}
	return moves, nil
}

// ExecuteStorageTierMoves plans parts once and invokes executor in input order.
// It does not retry or reorder partial work, so callers can resume from the
// returned Moved count after fixing the reported error.
func ExecuteStorageTierMoves(ctx context.Context, policy StorageTierPolicy, parts []StorageTierPart, executor StorageTierMoveExecutor) (StorageTierMoveReport, error) {
	if ctx == nil || executor == nil {
		return StorageTierMoveReport{}, ErrStorageTierMoveInvalid
	}
	moves, err := policy.PlanStorageTierMoves(parts)
	if err != nil {
		return StorageTierMoveReport{}, err
	}
	report := StorageTierMoveReport{Planned: len(moves)}
	for _, move := range moves {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := executor(ctx, move); err != nil {
			return report, err
		}
		report.Moved++
	}
	return report, nil
}

func (policy StorageTierPolicy) selectStorageTier(name, key string) (StorageTierSelection, error) {
	for _, rule := range policy.rules {
		if rule.Name != name {
			continue
		}
		path, err := rule.Placement.SelectPath(key)
		if err != nil {
			return StorageTierSelection{}, err
		}
		return StorageTierSelection{Tier: rule.Name, Path: path}, nil
	}
	return StorageTierSelection{}, fmt.Errorf("tier %q is not configured", name)
}
