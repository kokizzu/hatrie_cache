package hatBackup

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

var (
	// ErrBackupRotationInvalid reports an invalid rotation policy.
	ErrBackupRotationInvalid = errors.New("hatriecache: backup rotation policy is invalid")
	// ErrBackupRotationBudgetExceeded reports a latest backup that cannot fit
	// inside the configured unique-object budget.
	ErrBackupRotationBudgetExceeded = errors.New("hatriecache: latest backup exceeds the rotation object budget")
)

// BackupRotationPolicy controls a dry-run backup rotation plan. Zero limits
// are disabled. The latest backup is always retained, and no filesystem or
// object-store mutation is performed by PlanBackupRotation.
type BackupRotationPolicy struct {
	// MaxBackups limits retained manifests. Zero retains every eligible chain
	// member.
	MaxBackups int
	// MaxAge excludes manifests older than Now-MaxAge. A zero CreatedAt is
	// treated as unknown and retained conservatively.
	MaxAge time.Duration
	// MaxObjectBytes limits unique content-addressed bytes across retained
	// manifests. Zero disables the byte limit.
	MaxObjectBytes int64
	// Now makes age decisions deterministic. The current UTC time is used when
	// it is zero.
	Now time.Time
}

// PlanBackupRotation combines age, count, and unique-object-byte retention
// constraints into a deterministic BackupRetentionPlan. It walks backward
// from the latest validated backup, so the newest recoverable state wins when
// constraints conflict. Older candidates that would exceed the byte budget
// are skipped; the latest backup is an explicit hard requirement.
func PlanBackupRotation(manifests []BundleManifest, latestID string, policy BackupRotationPolicy) (BackupRetentionPlan, error) {
	if policy.MaxBackups < 0 || policy.MaxAge < 0 || policy.MaxObjectBytes < 0 {
		return BackupRetentionPlan{}, ErrBackupRotationInvalid
	}
	chain, err := PlanBackupChain(manifests, latestID)
	if err != nil {
		return BackupRetentionPlan{}, err
	}
	for _, input := range manifests {
		if input.StorageBackend != chain.StorageBackend || input.StorageFormat != chain.StorageFormat || input.StorageIdentity != chain.StorageIdentity || input.StorageGeneration != chain.StorageGeneration {
			return BackupRetentionPlan{}, fmt.Errorf("%w: mixed storage generations or identities at %q", ErrBackupRotationInvalid, input.BackupID)
		}
	}

	if policy.Now.IsZero() {
		policy.Now = time.Now().UTC()
	} else {
		policy.Now = policy.Now.UTC()
	}
	cutoff := time.Time{}
	if policy.MaxAge > 0 {
		cutoff = policy.Now.Add(-policy.MaxAge)
	}
	maxBackups := len(chain.Manifests)
	if policy.MaxBackups > 0 && policy.MaxBackups < maxBackups {
		maxBackups = policy.MaxBackups
	}

	selected := make(map[string]struct{}, maxBackups)
	selectedObjects := make(map[string]int64)
	latest := chain.Manifests[len(chain.Manifests)-1]
	latestObjects, latestBytes, err := backupRotationNewObjects(selectedObjects, latest)
	if err != nil {
		return BackupRetentionPlan{}, err
	}
	if policy.MaxObjectBytes > 0 && latestBytes > policy.MaxObjectBytes {
		return BackupRetentionPlan{}, fmt.Errorf("%w: need %d bytes, budget is %d", ErrBackupRotationBudgetExceeded, latestBytes, policy.MaxObjectBytes)
	}
	for hash, size := range latestObjects {
		selectedObjects[hash] = size
	}
	selected[latest.BackupID] = struct{}{}
	selectedBytes := latestBytes
	selectedCount := 1

	for index := len(chain.Manifests) - 2; index >= 0 && selectedCount < maxBackups; index-- {
		candidate := chain.Manifests[index]
		if policy.MaxAge > 0 && !candidate.CreatedAt.IsZero() && candidate.CreatedAt.Before(cutoff) {
			continue
		}
		newObjects, newBytes, err := backupRotationNewObjects(selectedObjects, candidate)
		if err != nil {
			return BackupRetentionPlan{}, err
		}
		if policy.MaxObjectBytes > 0 && newBytes > policy.MaxObjectBytes-selectedBytes {
			continue
		}
		for hash, size := range newObjects {
			selectedObjects[hash] = size
		}
		selectedBytes += newBytes
		selected[candidate.BackupID] = struct{}{}
		selectedCount++
	}

	plan := BackupRetentionPlan{Chain: chain, Retain: selectedCount, KeepObjectBytes: selectedBytes}
	for _, manifest := range chain.Manifests {
		if _, ok := selected[manifest.BackupID]; ok {
			plan.KeepBackupIDs = append(plan.KeepBackupIDs, manifest.BackupID)
		}
	}
	for _, input := range manifests {
		if _, ok := selected[input.BackupID]; !ok {
			plan.DeleteBackupIDs = append(plan.DeleteBackupIDs, input.BackupID)
		}
	}
	sort.Strings(plan.DeleteBackupIDs)
	return completeBackupRetentionObjectPlan(plan, manifests, selected)
}

func backupRotationNewObjects(existing map[string]int64, manifest BundleManifest) (map[string]int64, int64, error) {
	newObjects := make(map[string]int64)
	var bytes int64
	for _, file := range manifest.Files {
		objectKey := backupObjectIdentity(manifest, file)
		if _, ok := existing[objectKey]; ok {
			continue
		}
		if _, ok := newObjects[objectKey]; ok {
			continue
		}
		if file.Size > math.MaxInt64-bytes {
			return nil, 0, ErrBackupRotationInvalid
		}
		newObjects[objectKey] = file.Size
		bytes += file.Size
	}
	return newObjects, bytes, nil
}

func completeBackupRetentionObjectPlan(plan BackupRetentionPlan, manifests []BundleManifest, selected map[string]struct{}) (BackupRetentionPlan, error) {
	keepObjects := make(map[string]int64)
	allObjects := make(map[string]int64)
	keepHashes := make(map[string]struct{})
	allHashes := make(map[string]struct{})
	keepObjectKeys := make(map[string]struct{})
	allObjectKeys := make(map[string]struct{})
	for _, input := range manifests {
		for _, file := range input.Files {
			objectKey := backupObjectIdentity(input, file)
			if previous, ok := allObjects[objectKey]; ok && previous != file.Size {
				return BackupRetentionPlan{}, ErrBackupRotationInvalid
			}
			allObjects[objectKey] = file.Size
			allHashes[file.SHA256] = struct{}{}
			allObjectKeys[objectKey] = struct{}{}
			if _, ok := selected[input.BackupID]; ok {
				keepObjects[objectKey] = file.Size
				keepHashes[file.SHA256] = struct{}{}
				keepObjectKeys[objectKey] = struct{}{}
			}
		}
	}
	var keepBytes int64
	for _, size := range keepObjects {
		if size > math.MaxInt64-keepBytes {
			return BackupRetentionPlan{}, ErrBackupRotationInvalid
		}
		keepBytes += size
	}
	plan.KeepObjectBytes = keepBytes
	for hash := range keepHashes {
		plan.KeepObjectHashes = append(plan.KeepObjectHashes, hash)
	}
	for hash := range allHashes {
		if _, ok := keepHashes[hash]; !ok {
			plan.DeleteObjectHashes = append(plan.DeleteObjectHashes, hash)
		}
	}
	sort.Strings(plan.KeepObjectHashes)
	sort.Strings(plan.DeleteObjectHashes)
	for key := range keepObjectKeys {
		plan.KeepObjectKeys = append(plan.KeepObjectKeys, key)
	}
	for key := range allObjectKeys {
		if _, ok := keepObjectKeys[key]; !ok {
			plan.DeleteObjectKeys = append(plan.DeleteObjectKeys, key)
		}
	}
	sort.Strings(plan.KeepObjectKeys)
	sort.Strings(plan.DeleteObjectKeys)
	return plan, nil
}
