package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"hatrie_cache/hat/hatMerkle"
)

const (
	// DefaultReplicaPartRepairMaxParts bounds each inventory when callers do
	// not provide a more specific limit.
	DefaultReplicaPartRepairMaxParts = 65536
	// MaxReplicaPartRepairMaxParts prevents an accidental unbounded planner.
	MaxReplicaPartRepairMaxParts = 1 << 20
	// MaxReplicaPartRepairIdentifierBytes bounds replica and part names before
	// they can become part of an operator-visible repair plan.
	MaxReplicaPartRepairIdentifierBytes = 256
)

var (
	// ErrReplicaPartRepairOptionsInvalid reports an invalid planner bound.
	ErrReplicaPartRepairOptionsInvalid = errors.New("hatriecache: replica part repair options are invalid")
	// ErrReplicaPartRepairInvalid reports malformed replica or part metadata.
	ErrReplicaPartRepairInvalid = errors.New("hatriecache: replica part repair inventory is invalid")
	// ErrReplicaPartRepairCapacity reports an inventory larger than its bound.
	ErrReplicaPartRepairCapacity = errors.New("hatriecache: replica part repair inventory exceeds its bound")
)

// ReplicaPartRepairOptions bounds the metadata accepted by one comparison.
// The planner never retains input inventories after it returns.
type ReplicaPartRepairOptions struct {
	MaxParts int
}

// ReplicaPartInventory is a verified active-part snapshot for one replica.
// Parts should originate from hatMerkle.PartCatalog.Snapshot; this package
// compares the supplied manifests but does not read or verify part bytes.
type ReplicaPartInventory struct {
	ReplicaID  string
	Generation uint64
	Parts      []hatMerkle.PartCatalogEntry
}

// ReplicaPartRepairActionKind identifies the caller-owned operation needed to
// make the target inventory match the source inventory.
type ReplicaPartRepairActionKind uint8

const (
	// ReplicaPartRepairCopy requests copying a source-only part to the target.
	ReplicaPartRepairCopy ReplicaPartRepairActionKind = iota + 1
	// ReplicaPartRepairReplace requests replacing a target part with the source
	// version after the caller has safely staged the new bytes.
	ReplicaPartRepairReplace
	// ReplicaPartRepairQuarantine requests detaching a target-only part for
	// operator review; the planner never deletes it.
	ReplicaPartRepairQuarantine
)

// String returns the stable action name used by logs and operator output.
func (kind ReplicaPartRepairActionKind) String() string {
	switch kind {
	case ReplicaPartRepairCopy:
		return "copy"
	case ReplicaPartRepairReplace:
		return "replace"
	case ReplicaPartRepairQuarantine:
		return "quarantine"
	default:
		return "unknown"
	}
}

// ReplicaPartRepairAction is one deterministic, transport-neutral repair
// instruction. Source and Target contain copied metadata; their bytes remain
// owned by the caller's storage and are never read or changed here.
type ReplicaPartRepairAction struct {
	Kind   ReplicaPartRepairActionKind
	Name   string
	Source hatMerkle.PartCatalogEntry
	Target hatMerkle.PartCatalogEntry
}

// ReplicaPartRepairPlan describes the difference between one source and one
// target snapshot. Actions are sorted by part name and are safe to inspect
// before an embedding transport performs any side effect.
type ReplicaPartRepairPlan struct {
	SourceReplica    string
	TargetReplica    string
	SourceGeneration uint64
	TargetGeneration uint64

	EqualParts      int
	SourceOnlyParts int
	TargetOnlyParts int
	ReplacedParts   int
	Actions         []ReplicaPartRepairAction
}

// BuildReplicaPartRepairPlan compares two bounded part inventories. A source
// only part becomes Copy, a checksum mismatch becomes Replace, and a target
// only part becomes Quarantine. The function does not perform I/O, network
// transfer, deletion, or automatic mutation.
func BuildReplicaPartRepairPlan(options ReplicaPartRepairOptions, source, target ReplicaPartInventory) (ReplicaPartRepairPlan, error) {
	maxParts, err := normalizeReplicaPartRepairOptions(options)
	if err != nil {
		return ReplicaPartRepairPlan{}, err
	}
	normalizedSource, err := normalizeReplicaPartInventory(source, maxParts)
	if err != nil {
		return ReplicaPartRepairPlan{}, fmt.Errorf("source: %w", err)
	}
	normalizedTarget, err := normalizeReplicaPartInventory(target, maxParts)
	if err != nil {
		return ReplicaPartRepairPlan{}, fmt.Errorf("target: %w", err)
	}
	if normalizedSource.ReplicaID == normalizedTarget.ReplicaID {
		return ReplicaPartRepairPlan{}, fmt.Errorf("%w: source and target replica are the same", ErrReplicaPartRepairInvalid)
	}

	plan := ReplicaPartRepairPlan{
		SourceReplica:    normalizedSource.ReplicaID,
		TargetReplica:    normalizedTarget.ReplicaID,
		SourceGeneration: normalizedSource.Generation,
		TargetGeneration: normalizedTarget.Generation,
	}
	for sourceIndex, targetIndex := 0, 0; sourceIndex < len(normalizedSource.Parts) || targetIndex < len(normalizedTarget.Parts); {
		if sourceIndex >= len(normalizedSource.Parts) {
			targetPart := normalizedTarget.Parts[targetIndex]
			plan.TargetOnlyParts++
			plan.Actions = append(plan.Actions, ReplicaPartRepairAction{
				Kind:   ReplicaPartRepairQuarantine,
				Name:   targetPart.Name,
				Target: cloneReplicaPartEntry(targetPart),
			})
			targetIndex++
			continue
		}
		if targetIndex >= len(normalizedTarget.Parts) {
			sourcePart := normalizedSource.Parts[sourceIndex]
			plan.SourceOnlyParts++
			plan.Actions = append(plan.Actions, ReplicaPartRepairAction{
				Kind:   ReplicaPartRepairCopy,
				Name:   sourcePart.Name,
				Source: cloneReplicaPartEntry(sourcePart),
			})
			sourceIndex++
			continue
		}

		sourcePart := normalizedSource.Parts[sourceIndex]
		targetPart := normalizedTarget.Parts[targetIndex]
		switch {
		case sourcePart.Name < targetPart.Name:
			plan.SourceOnlyParts++
			plan.Actions = append(plan.Actions, ReplicaPartRepairAction{
				Kind:   ReplicaPartRepairCopy,
				Name:   sourcePart.Name,
				Source: cloneReplicaPartEntry(sourcePart),
			})
			sourceIndex++
		case targetPart.Name < sourcePart.Name:
			plan.TargetOnlyParts++
			plan.Actions = append(plan.Actions, ReplicaPartRepairAction{
				Kind:   ReplicaPartRepairQuarantine,
				Name:   targetPart.Name,
				Target: cloneReplicaPartEntry(targetPart),
			})
			targetIndex++
		default:
			if sourcePart.Manifest.Equal(targetPart.Manifest) {
				plan.EqualParts++
			} else {
				plan.ReplacedParts++
				plan.Actions = append(plan.Actions, ReplicaPartRepairAction{
					Kind:   ReplicaPartRepairReplace,
					Name:   sourcePart.Name,
					Source: cloneReplicaPartEntry(sourcePart),
					Target: cloneReplicaPartEntry(targetPart),
				})
			}
			sourceIndex++
			targetIndex++
		}
	}
	return plan, nil
}

// IsCurrent reports whether the plan's input generation fence still matches
// the latest source and target observations. Callers should re-read both
// inventories before applying actions if either generation changed.
func (plan ReplicaPartRepairPlan) IsCurrent(sourceGeneration, targetGeneration uint64) bool {
	return plan.SourceGeneration == sourceGeneration && plan.TargetGeneration == targetGeneration
}

// Changed reports whether applying the plan would require any action.
func (plan ReplicaPartRepairPlan) Changed() bool {
	return len(plan.Actions) != 0
}

func normalizeReplicaPartRepairOptions(options ReplicaPartRepairOptions) (int, error) {
	if options.MaxParts < 0 {
		return 0, ErrReplicaPartRepairOptionsInvalid
	}
	if options.MaxParts == 0 {
		return DefaultReplicaPartRepairMaxParts, nil
	}
	if options.MaxParts > MaxReplicaPartRepairMaxParts {
		return 0, fmt.Errorf("%w: max parts %d exceeds %d", ErrReplicaPartRepairOptionsInvalid, options.MaxParts, MaxReplicaPartRepairMaxParts)
	}
	return options.MaxParts, nil
}

func normalizeReplicaPartInventory(inventory ReplicaPartInventory, maxParts int) (ReplicaPartInventory, error) {
	replicaID, err := normalizeReplicaPartRepairIdentifier(inventory.ReplicaID, "replica ID")
	if err != nil {
		return ReplicaPartInventory{}, err
	}
	if len(inventory.Parts) > maxParts {
		return ReplicaPartInventory{}, fmt.Errorf("%w: got %d parts, limit %d", ErrReplicaPartRepairCapacity, len(inventory.Parts), maxParts)
	}
	parts := make([]hatMerkle.PartCatalogEntry, len(inventory.Parts))
	for index, part := range inventory.Parts {
		name, err := normalizeReplicaPartRepairIdentifier(part.Name, "part name")
		if err != nil {
			return ReplicaPartInventory{}, fmt.Errorf("%w at part %d: %v", ErrReplicaPartRepairInvalid, index, err)
		}
		part.Name = name
		parts[index] = cloneReplicaPartEntry(part)
	}
	sort.Slice(parts, func(left, right int) bool {
		return parts[left].Name < parts[right].Name
	})
	for index := 1; index < len(parts); index++ {
		if parts[index-1].Name == parts[index].Name {
			return ReplicaPartInventory{}, fmt.Errorf("%w: duplicate part %q", ErrReplicaPartRepairInvalid, parts[index].Name)
		}
	}
	inventory.ReplicaID = replicaID
	inventory.Parts = parts
	return inventory, nil
}

func normalizeReplicaPartRepairIdentifier(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > MaxReplicaPartRepairIdentifierBytes || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%s is empty, too long, or contains a NUL byte", label)
	}
	return value, nil
}

func cloneReplicaPartEntry(entry hatMerkle.PartCatalogEntry) hatMerkle.PartCatalogEntry {
	if entry.Manifest.Columns != nil {
		entry.Manifest.Columns = append([]hatMerkle.PartColumnChecksum(nil), entry.Manifest.Columns...)
	}
	return entry
}
