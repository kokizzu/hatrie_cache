package hatBackup

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	pathpkg "path"
	"sort"
	"strings"
)

// BackupChainPlan is a validated, base-to-latest view of one incremental
// backup chain. Manifests are independent copies and can be retained by the
// caller without aliasing the input slice.
type BackupChainPlan struct {
	Manifests            []BundleManifest
	BaseBackupID         string
	LatestBackupID       string
	JournalSequenceStart uint64
	JournalSequenceEnd   uint64
	StorageBackend       string
	StorageFormat        string
	StorageIdentity      string
	StorageGeneration    uint64
	ObjectCount          int
	ObjectBytes          int64
}

// BackupRetentionPlan describes which manifests and content-addressed
// objects a retention operation may remove. Objects referenced by a kept
// manifest are never listed for deletion.
type BackupRetentionPlan struct {
	Chain              BackupChainPlan
	Retain             int
	KeepBackupIDs      []string
	DeleteBackupIDs    []string
	KeepObjectHashes   []string
	DeleteObjectHashes []string
}

// PlanBackupChain validates and orders the complete chain ending at latestID.
// It rejects missing or cyclic parents, mixed storage generations, sequence
// regressions, and malformed content-addressed file declarations.
func PlanBackupChain(manifests []BundleManifest, latestID string) (BackupChainPlan, error) {
	latestID = strings.TrimSpace(latestID)
	if latestID == "" {
		return BackupChainPlan{}, errors.New("hatriecache: latest backup id is required")
	}
	if len(manifests) == 0 {
		return BackupChainPlan{}, errors.New("hatriecache: backup chain is empty")
	}

	byID := make(map[string]BundleManifest, len(manifests))
	for _, input := range manifests {
		manifest := cloneBackupManifest(input)
		if err := validateBackupChainManifest(manifest); err != nil {
			return BackupChainPlan{}, err
		}
		if _, exists := byID[manifest.BackupID]; exists {
			return BackupChainPlan{}, fmt.Errorf("hatriecache: duplicate backup id %q", manifest.BackupID)
		}
		byID[manifest.BackupID] = manifest
	}

	if _, ok := byID[latestID]; !ok {
		return BackupChainPlan{}, fmt.Errorf("hatriecache: latest backup %q is not present", latestID)
	}
	reverse := make([]BundleManifest, 0, len(manifests))
	seen := make(map[string]struct{}, len(manifests))
	currentID := latestID
	for {
		if _, exists := seen[currentID]; exists {
			return BackupChainPlan{}, fmt.Errorf("hatriecache: backup chain contains a parent cycle at %q", currentID)
		}
		seen[currentID] = struct{}{}
		manifest, exists := byID[currentID]
		if !exists {
			return BackupChainPlan{}, fmt.Errorf("hatriecache: backup %q parent is missing", currentID)
		}
		reverse = append(reverse, manifest)
		if manifest.ParentBackupID == "" {
			break
		}
		currentID = manifest.ParentBackupID
	}

	chain := make([]BundleManifest, len(reverse))
	for index := range reverse {
		chain[len(reverse)-1-index] = reverse[index]
	}
	plan := BackupChainPlan{
		Manifests:            chain,
		BaseBackupID:         chain[0].BackupID,
		LatestBackupID:       chain[len(chain)-1].BackupID,
		StorageBackend:       chain[0].StorageBackend,
		StorageFormat:        chain[0].StorageFormat,
		StorageIdentity:      chain[0].StorageIdentity,
		StorageGeneration:    chain[0].StorageGeneration,
		JournalSequenceStart: chain[0].JournalSequence,
		JournalSequenceEnd:   chain[len(chain)-1].JournalSequence,
	}
	objectSizes := make(map[string]int64)
	for index, manifest := range chain {
		if manifest.StorageBackend != plan.StorageBackend || manifest.StorageFormat != plan.StorageFormat || manifest.StorageIdentity != plan.StorageIdentity || manifest.StorageGeneration != plan.StorageGeneration {
			return BackupChainPlan{}, fmt.Errorf("hatriecache: backup chain storage generation or identity mismatch at %q", manifest.BackupID)
		}
		if index > 0 {
			previous := chain[index-1]
			if manifest.JournalSequence < previous.JournalSequence {
				return BackupChainPlan{}, fmt.Errorf("hatriecache: backup chain sequence %d regresses after %d", manifest.JournalSequence, previous.JournalSequence)
			}
		}
		for _, file := range manifest.Files {
			if previousSize, exists := objectSizes[file.SHA256]; exists {
				if previousSize != file.Size {
					return BackupChainPlan{}, fmt.Errorf("hatriecache: object %q has conflicting sizes", file.SHA256)
				}
				continue
			}
			objectSizes[file.SHA256] = file.Size
		}
	}
	plan.ObjectCount = len(objectSizes)
	for _, size := range objectSizes {
		if size > math.MaxInt64-plan.ObjectBytes {
			return BackupChainPlan{}, errors.New("hatriecache: backup chain object bytes overflow")
		}
		plan.ObjectBytes += size
	}
	return plan, nil
}

// PlanBackupRetention validates the complete chain and computes a deterministic
// retention plan. Retention is counted by manifests; each kept manifest lists
// its complete checkpoint object set, so deleting an older manifest cannot
// delete an object still needed by a kept manifest.
func PlanBackupRetention(manifests []BundleManifest, latestID string, retain int) (BackupRetentionPlan, error) {
	if retain < 1 {
		return BackupRetentionPlan{}, errors.New("hatriecache: backup retention must be positive")
	}
	chain, err := PlanBackupChain(manifests, latestID)
	if err != nil {
		return BackupRetentionPlan{}, err
	}
	for _, input := range manifests {
		if input.StorageBackend != chain.StorageBackend || input.StorageFormat != chain.StorageFormat || input.StorageIdentity != chain.StorageIdentity || input.StorageGeneration != chain.StorageGeneration {
			return BackupRetentionPlan{}, fmt.Errorf("hatriecache: backup retention contains mixed storage generations or identities at %q", input.BackupID)
		}
	}
	if retain > len(chain.Manifests) {
		retain = len(chain.Manifests)
	}
	keep := make(map[string]struct{}, retain)
	plan := BackupRetentionPlan{Chain: chain, Retain: retain}
	start := len(chain.Manifests) - retain
	for _, manifest := range chain.Manifests[start:] {
		keep[manifest.BackupID] = struct{}{}
		plan.KeepBackupIDs = append(plan.KeepBackupIDs, manifest.BackupID)
	}
	for _, input := range manifests {
		if _, exists := keep[input.BackupID]; !exists {
			plan.DeleteBackupIDs = append(plan.DeleteBackupIDs, input.BackupID)
		}
	}
	sort.Strings(plan.DeleteBackupIDs)

	keepObjects := make(map[string]struct{})
	allObjects := make(map[string]struct{})
	for _, input := range manifests {
		for _, file := range input.Files {
			allObjects[file.SHA256] = struct{}{}
		}
		if _, exists := keep[input.BackupID]; exists {
			for _, file := range input.Files {
				keepObjects[file.SHA256] = struct{}{}
			}
		}
	}
	for hash := range keepObjects {
		plan.KeepObjectHashes = append(plan.KeepObjectHashes, hash)
	}
	for hash := range allObjects {
		if _, exists := keepObjects[hash]; !exists {
			plan.DeleteObjectHashes = append(plan.DeleteObjectHashes, hash)
		}
	}
	sort.Strings(plan.KeepObjectHashes)
	sort.Strings(plan.DeleteObjectHashes)
	return plan, nil
}

func validateBackupChainManifest(manifest BundleManifest) error {
	if manifest.Version != BundleVersion {
		return fmt.Errorf("hatriecache: backup %q has unsupported manifest version %d", manifest.BackupID, manifest.Version)
	}
	if manifest.Mode != ModePebbleIncremental {
		return fmt.Errorf("hatriecache: backup %q is not an incremental Pebble manifest", manifest.BackupID)
	}
	if manifest.BackupID == "" || strings.TrimSpace(manifest.BackupID) != manifest.BackupID {
		return errors.New("hatriecache: backup id is required and must not contain surrounding whitespace")
	}
	if manifest.ParentBackupID != "" && strings.TrimSpace(manifest.ParentBackupID) != manifest.ParentBackupID {
		return fmt.Errorf("hatriecache: backup %q parent id contains surrounding whitespace", manifest.BackupID)
	}
	if manifest.ParentBackupID == manifest.BackupID {
		return fmt.Errorf("hatriecache: backup %q cannot be its own parent", manifest.BackupID)
	}
	if manifest.ParentBackupID == "" && manifest.Incremental {
		return fmt.Errorf("hatriecache: base backup %q cannot be marked incremental", manifest.BackupID)
	}
	if manifest.ParentBackupID != "" && !manifest.Incremental {
		return fmt.Errorf("hatriecache: child backup %q must be marked incremental", manifest.BackupID)
	}
	if manifest.Store == "" || manifest.StorageBackend == "" || manifest.StorageFormat == "" || manifest.StorageIdentity == "" || manifest.StorageGeneration == 0 {
		return fmt.Errorf("hatriecache: backup %q is missing storage identity or generation", manifest.BackupID)
	}
	for _, value := range []string{manifest.Store, manifest.StorageBackend, manifest.StorageFormat, manifest.StorageIdentity} {
		if strings.TrimSpace(value) != value {
			return fmt.Errorf("hatriecache: backup %q contains storage metadata with surrounding whitespace", manifest.BackupID)
		}
	}
	paths := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		if err := validateBackupChainFile(file); err != nil {
			return fmt.Errorf("hatriecache: backup %q: %w", manifest.BackupID, err)
		}
		if _, exists := paths[file.Path]; exists {
			return fmt.Errorf("hatriecache: backup %q declares duplicate file %q", manifest.BackupID, file.Path)
		}
		paths[file.Path] = struct{}{}
	}
	return nil
}

func validateBackupChainFile(file BundleFile) error {
	clean := pathpkg.Clean(file.Path)
	if file.Path == "" || clean != file.Path || pathpkg.IsAbs(file.Path) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(file.Path, "\\") {
		return fmt.Errorf("backup file path %q is unsafe", file.Path)
	}
	if file.Size < 0 {
		return fmt.Errorf("backup file %q has a negative size", file.Path)
	}
	if len(file.SHA256) != sha256.Size*2 || strings.ToLower(file.SHA256) != file.SHA256 {
		return fmt.Errorf("backup file %q has an invalid SHA-256", file.Path)
	}
	if _, err := hex.DecodeString(file.SHA256); err != nil {
		return fmt.Errorf("backup file %q has an invalid SHA-256", file.Path)
	}
	return nil
}

func cloneBackupManifest(input BundleManifest) BundleManifest {
	output := input
	output.Files = append([]BundleFile(nil), input.Files...)
	output.Partition = ClonePartitionMetadata(input.Partition)
	return output
}
