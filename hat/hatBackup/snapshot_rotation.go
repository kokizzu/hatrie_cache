package hatBackup

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	// DefaultSnapshotRotationCadence is used when an enabled policy omits its
	// cadence. Automatic scheduling remains disabled unless Enabled is true.
	DefaultSnapshotRotationCadence = time.Hour
	// DefaultSnapshotRotationKeepLatest keeps two newest complete chains.
	DefaultSnapshotRotationKeepLatest = 2
	MaxSnapshotRotationKeepLatest     = 10_000
)

var (
	ErrInvalidSnapshotRotationCadence = errors.New("hatriecache: invalid snapshot rotation cadence")
	ErrInvalidSnapshotRotationKeep    = errors.New("hatriecache: invalid snapshot rotation keep count")
	ErrInvalidSnapshotRotationBytes   = errors.New("hatriecache: invalid snapshot rotation byte budget")
)

// SnapshotRotationOptions configures opt-in snapshot scheduling and the
// retention plan returned by SnapshotRotationPolicy.Plan. A zero policy is
// disabled to preserve existing backup behavior; non-zero retention defaults
// are applied when the policy is constructed.
type SnapshotRotationOptions struct {
	Enabled    bool
	Cadence    time.Duration
	KeepLatest int
	MaxBytes   int64
}

// SnapshotRotationPolicy provides a deterministic cadence check and a safe,
// non-destructive retention planner. It never deletes backup payloads.
type SnapshotRotationPolicy struct {
	options SnapshotRotationOptions
}

// SnapshotRotationPlan describes which complete backup chains can be kept and
// which manifests are eligible for deletion by an operator or scheduler.
type SnapshotRotationPlan struct {
	SelectedTips      []string `json:"selected_tips,omitempty"`
	RetainedBackupIDs []string `json:"retained_backup_ids,omitempty"`
	DeleteBackupIDs   []string `json:"delete_backup_ids,omitempty"`
	RetainedBytes     int64    `json:"retained_bytes"`
	DeletedBytes      int64    `json:"deleted_bytes"`
	MaxBytes          int64    `json:"max_bytes,omitempty"`
	OverBudget        bool     `json:"over_budget,omitempty"`
}

// NewSnapshotRotationPolicy validates and normalizes one policy.
func NewSnapshotRotationPolicy(options SnapshotRotationOptions) (SnapshotRotationPolicy, error) {
	normalized, err := normalizeSnapshotRotationOptions(options)
	if err != nil {
		return SnapshotRotationPolicy{}, err
	}
	return SnapshotRotationPolicy{options: normalized}, nil
}

// PlanRotation loads the catalog and returns a non-destructive rotation plan.
func (catalog *BackupManifestCatalog) PlanRotation(policy SnapshotRotationPolicy) (SnapshotRotationPlan, error) {
	if catalog == nil {
		return SnapshotRotationPlan{}, errors.New("hatriecache: backup manifest catalog is nil")
	}
	manifests, err := catalog.Load()
	if err != nil {
		return SnapshotRotationPlan{}, err
	}
	return policy.Plan(manifests)
}

func normalizeSnapshotRotationOptions(options SnapshotRotationOptions) (SnapshotRotationOptions, error) {
	if options.Cadence < 0 {
		return SnapshotRotationOptions{}, fmt.Errorf("%w: %s", ErrInvalidSnapshotRotationCadence, options.Cadence)
	}
	if options.KeepLatest < 0 || options.KeepLatest > MaxSnapshotRotationKeepLatest {
		return SnapshotRotationOptions{}, fmt.Errorf("%w: %d", ErrInvalidSnapshotRotationKeep, options.KeepLatest)
	}
	if options.MaxBytes < 0 {
		return SnapshotRotationOptions{}, fmt.Errorf("%w: %d", ErrInvalidSnapshotRotationBytes, options.MaxBytes)
	}
	if options.Cadence == 0 {
		options.Cadence = DefaultSnapshotRotationCadence
	}
	if options.KeepLatest == 0 {
		options.KeepLatest = DefaultSnapshotRotationKeepLatest
	}
	return options, nil
}

// Options returns the normalized policy options.
func (policy SnapshotRotationPolicy) Options() SnapshotRotationOptions {
	return policy.options
}

// ShouldSnapshot reports whether an enabled policy is due. A zero last time
// means the first snapshot is due immediately. Clock movement backwards does
// not trigger an early snapshot.
func (policy SnapshotRotationPolicy) ShouldSnapshot(lastSnapshot, now time.Time) bool {
	options := policy.options
	if !options.Enabled || options.Cadence <= 0 {
		return false
	}
	if lastSnapshot.IsZero() {
		return true
	}
	if now.Before(lastSnapshot) {
		return false
	}
	return now.Sub(lastSnapshot) >= options.Cadence
}

// Plan computes a retention plan without deleting or mutating any backup.
// Complete incremental chains are kept together. The newest chain is always
// retained, even when it alone exceeds MaxBytes, and OverBudget reports that
// unavoidable condition to the caller.
func (policy SnapshotRotationPolicy) Plan(manifests []BundleManifest) (SnapshotRotationPlan, error) {
	options, err := normalizeSnapshotRotationOptions(policy.options)
	if err != nil {
		return SnapshotRotationPlan{}, err
	}
	plan := SnapshotRotationPlan{MaxBytes: options.MaxBytes}
	if len(manifests) == 0 {
		return plan, nil
	}
	if err := validateBackupManifestCatalog(manifests); err != nil {
		return SnapshotRotationPlan{}, err
	}

	byID := make(map[string]BundleManifest, len(manifests))
	children := make(map[string]int, len(manifests))
	bytesByID := make(map[string]int64, len(manifests))
	for _, manifest := range manifests {
		byID[manifest.BackupID] = manifest
		if manifest.ParentBackupID != "" {
			children[manifest.ParentBackupID]++
		}
		bytes, err := snapshotRotationManifestBytes(manifest)
		if err != nil {
			return SnapshotRotationPlan{}, err
		}
		bytesByID[manifest.BackupID] = bytes
	}

	type chain struct {
		tipID string
		ids   []string
		when  time.Time
		seq   uint64
	}
	chains := make([]chain, 0, len(manifests))
	for _, manifest := range manifests {
		if children[manifest.BackupID] != 0 {
			continue
		}
		ids, err := snapshotRotationChain(byID, manifest.BackupID)
		if err != nil {
			return SnapshotRotationPlan{}, err
		}
		chains = append(chains, chain{
			tipID: manifest.BackupID,
			ids:   ids,
			when:  manifest.CreatedAt,
			seq:   manifest.JournalSequence,
		})
	}
	sort.SliceStable(chains, func(left, right int) bool {
		return snapshotRotationManifestAfter(chains[left].when, chains[left].seq, chains[left].tipID, chains[right].when, chains[right].seq, chains[right].tipID)
	})

	keep := make(map[string]struct{}, len(manifests))
	retainedBytes := int64(0)
	for _, current := range chains {
		if len(plan.SelectedTips) >= options.KeepLatest {
			break
		}
		additionalBytes := int64(0)
		for _, id := range current.ids {
			if _, exists := keep[id]; !exists {
				additionalBytes, err = addSnapshotRotationBytes(additionalBytes, bytesByID[id])
				if err != nil {
					return SnapshotRotationPlan{}, err
				}
			}
		}
		if len(plan.SelectedTips) > 0 && options.MaxBytes > 0 && retainedBytes > options.MaxBytes-additionalBytes {
			continue
		}
		plan.SelectedTips = append(plan.SelectedTips, current.tipID)
		for _, id := range current.ids {
			keep[id] = struct{}{}
		}
		retainedBytes, err = addSnapshotRotationBytes(retainedBytes, additionalBytes)
		if err != nil {
			return SnapshotRotationPlan{}, err
		}
	}

	ordered := append([]BundleManifest(nil), manifests...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return snapshotRotationManifestBefore(ordered[left], ordered[right])
	})
	plan.RetainedBackupIDs = make([]string, 0, len(keep))
	plan.DeleteBackupIDs = make([]string, 0, len(ordered)-len(keep))
	for _, manifest := range ordered {
		if _, exists := keep[manifest.BackupID]; exists {
			plan.RetainedBackupIDs = append(plan.RetainedBackupIDs, manifest.BackupID)
			continue
		}
		plan.DeleteBackupIDs = append(plan.DeleteBackupIDs, manifest.BackupID)
		plan.DeletedBytes, err = addSnapshotRotationBytes(plan.DeletedBytes, bytesByID[manifest.BackupID])
		if err != nil {
			return SnapshotRotationPlan{}, err
		}
	}
	plan.RetainedBytes = retainedBytes
	plan.OverBudget = options.MaxBytes > 0 && retainedBytes > options.MaxBytes
	return plan, nil
}

func snapshotRotationManifestBytes(manifest BundleManifest) (int64, error) {
	if manifest.NewObjectBytes < 0 || manifest.ReusedObjectBytes < 0 {
		return 0, fmt.Errorf("%w: manifest %q has negative object bytes", ErrInvalidSnapshotRotationBytes, manifest.BackupID)
	}
	if manifest.NewObjectBytes > 0 {
		return manifest.NewObjectBytes, nil
	}
	total := int64(0)
	for _, file := range manifest.Files {
		if file.Size < 0 {
			return 0, fmt.Errorf("%w: manifest %q has invalid file size", ErrInvalidSnapshotRotationBytes, manifest.BackupID)
		}
		var err error
		total, err = addSnapshotRotationBytes(total, file.Size)
		if err != nil {
			return 0, fmt.Errorf("%w: manifest %q file sizes overflow", ErrInvalidSnapshotRotationBytes, manifest.BackupID)
		}
	}
	return total, nil
}

func snapshotRotationChain(byID map[string]BundleManifest, tipID string) ([]string, error) {
	reverse := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	currentID := tipID
	for {
		if _, exists := seen[currentID]; exists {
			return nil, fmt.Errorf("hatriecache: backup chain contains a parent cycle at %q", currentID)
		}
		seen[currentID] = struct{}{}
		manifest, exists := byID[currentID]
		if !exists {
			return nil, fmt.Errorf("hatriecache: backup %q parent is missing", currentID)
		}
		reverse = append(reverse, currentID)
		if manifest.ParentBackupID == "" {
			break
		}
		currentID = manifest.ParentBackupID
	}
	ids := make([]string, len(reverse))
	for index := range reverse {
		ids[len(reverse)-1-index] = reverse[index]
	}
	base := byID[ids[0]]
	objectSizes := make(map[string]int64, len(ids))
	for index, id := range ids {
		manifest := byID[id]
		if manifest.StorageBackend != base.StorageBackend || manifest.StorageFormat != base.StorageFormat || manifest.StorageIdentity != base.StorageIdentity || manifest.StorageGeneration != base.StorageGeneration {
			return nil, fmt.Errorf("hatriecache: backup chain storage generation or identity mismatch at %q", manifest.BackupID)
		}
		if index > 0 && manifest.JournalSequence < byID[ids[index-1]].JournalSequence {
			return nil, fmt.Errorf("hatriecache: backup chain sequence %d regresses after %d", manifest.JournalSequence, byID[ids[index-1]].JournalSequence)
		}
		for _, file := range manifest.Files {
			objectKey := backupObjectIdentity(manifest, file)
			if previousSize, exists := objectSizes[objectKey]; exists && previousSize != file.Size {
				return nil, fmt.Errorf("hatriecache: object %q has conflicting sizes", objectKey)
			}
			objectSizes[objectKey] = file.Size
		}
	}
	return ids, nil
}

func addSnapshotRotationBytes(left, right int64) (int64, error) {
	if left < 0 || right < 0 || left > (1<<63-1)-right {
		return 0, ErrInvalidSnapshotRotationBytes
	}
	return left + right, nil
}

func snapshotRotationManifestBefore(left, right BundleManifest) bool {
	return snapshotRotationManifestBeforeValues(left.CreatedAt, left.JournalSequence, left.BackupID, right.CreatedAt, right.JournalSequence, right.BackupID)
}

func snapshotRotationManifestBeforeValues(leftWhen time.Time, leftSequence uint64, leftID string, rightWhen time.Time, rightSequence uint64, rightID string) bool {
	if !leftWhen.Equal(rightWhen) {
		return leftWhen.Before(rightWhen)
	}
	if leftSequence != rightSequence {
		return leftSequence < rightSequence
	}
	return leftID < rightID
}

func snapshotRotationManifestAfter(leftWhen time.Time, leftSequence uint64, leftID string, rightWhen time.Time, rightSequence uint64, rightID string) bool {
	return snapshotRotationManifestBeforeValues(rightWhen, rightSequence, rightID, leftWhen, leftSequence, leftID)
}
