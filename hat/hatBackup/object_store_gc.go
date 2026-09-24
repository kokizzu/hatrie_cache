package hatBackup

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
)

var (
	// ErrObjectStoreGarbageCollectionUnsupported means the store does not
	// expose the optional list or delete capability required by GC.
	ErrObjectStoreGarbageCollectionUnsupported = errors.New("hatriecache: object store garbage collection is unsupported")
	// ErrObjectStoreGarbageCollectionUnsafe means the retention plan is not a
	// content-addressed incremental plan that can be checked safely.
	ErrObjectStoreGarbageCollectionUnsafe = errors.New("hatriecache: object store garbage collection plan is unsafe")
	// ErrObjectStoreGarbageCollectionPlanInvalid means an apply plan is not
	// bound to this target or contains an invalid physical key.
	ErrObjectStoreGarbageCollectionPlanInvalid = errors.New("hatriecache: object store garbage collection plan is invalid")
	// ErrObjectStoreGarbageCollectionKeepMissing means listing did not prove
	// that every retained object is present, so deletion is refused.
	ErrObjectStoreGarbageCollectionKeepMissing = errors.New("hatriecache: retained object is missing from object-store listing")
)

// ObjectStoreObject describes one object returned by an optional listing
// capability. Size is advisory and may be zero when the store does not expose
// object metadata.
type ObjectStoreObject struct {
	Key  string
	Size int64
}

// ObjectStoreObjectLister is an optional object-store capability used by the
// garbage-collection planner. The prefix is a physical object-key prefix.
type ObjectStoreObjectLister interface {
	List(ctx context.Context, prefix string) ([]ObjectStoreObject, error)
}

// ObjectStoreObjectDeleter is an optional object-store capability used by the
// garbage-collection applier.
type ObjectStoreObjectDeleter interface {
	Delete(ctx context.Context, key string) error
}

// ObjectStoreGarbageCollectionPlan contains the exact physical keys reviewed
// by PlanGarbageCollection. ApplyGarbageCollection deletes only
// DeleteObjectKeys; unknown object names are retained in SkippedObjectKeys.
type ObjectStoreGarbageCollectionPlan struct {
	Prefix            string
	LatestBackupID    string
	RetainedBackupIDs []string
	KeepObjectKeys    []string
	DeleteObjectKeys  []string
	SkippedObjectKeys []string
}

// PlanGarbageCollection lists content-addressed objects and creates a safe,
// deterministic deletion plan from a validated backup retention plan. It
// refuses path-addressed backups and refuses to plan deletion if any retained
// object is absent from the listing.
func (target *ObjectStoreTarget) PlanGarbageCollection(ctx context.Context, retention BackupRetentionPlan) (ObjectStoreGarbageCollectionPlan, error) {
	if target == nil {
		return ObjectStoreGarbageCollectionPlan{}, ErrObjectStoreGarbageCollectionPlanInvalid
	}
	if err := target.validate(ctx); err != nil {
		return ObjectStoreGarbageCollectionPlan{}, err
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return ObjectStoreGarbageCollectionPlan{}, err
	}
	lister, ok := target.store.(ObjectStoreObjectLister)
	if !ok {
		return ObjectStoreGarbageCollectionPlan{}, ErrObjectStoreGarbageCollectionUnsupported
	}
	chain, keepIDs, keepRelative, err := validateGarbageCollectionRetention(retention)
	if err != nil {
		return ObjectStoreGarbageCollectionPlan{}, err
	}
	objects, err := lister.List(ctx, target.objectKey(objectStoreContentPrefix)+"/")
	if err != nil {
		return ObjectStoreGarbageCollectionPlan{}, err
	}
	plan := ObjectStoreGarbageCollectionPlan{
		Prefix:            target.prefix,
		LatestBackupID:    chain.LatestBackupID,
		RetainedBackupIDs: append([]string(nil), keepIDs...),
	}
	seenKeep := make(map[string]struct{}, len(keepRelative))
	seenPhysical := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if err := checkObjectStoreContext(ctx); err != nil {
			return ObjectStoreGarbageCollectionPlan{}, err
		}
		key := strings.TrimSpace(object.Key)
		relative, valid := target.canonicalContentObjectRelative(key)
		if !valid {
			if key != "" {
				plan.SkippedObjectKeys = appendUniqueString(plan.SkippedObjectKeys, key)
			}
			continue
		}
		physical := target.objectKey(relative)
		if _, duplicate := seenPhysical[physical]; duplicate {
			continue
		}
		seenPhysical[physical] = struct{}{}
		if _, keep := keepRelative[relative]; keep {
			plan.KeepObjectKeys = append(plan.KeepObjectKeys, physical)
			seenKeep[relative] = struct{}{}
			continue
		}
		plan.DeleteObjectKeys = append(plan.DeleteObjectKeys, physical)
	}
	for relative := range keepRelative {
		if _, present := seenKeep[relative]; !present {
			return ObjectStoreGarbageCollectionPlan{}, errors.Join(ErrObjectStoreGarbageCollectionKeepMissing, errors.New(relative))
		}
	}
	sort.Strings(plan.KeepObjectKeys)
	sort.Strings(plan.DeleteObjectKeys)
	sort.Strings(plan.SkippedObjectKeys)
	return plan, nil
}

// ApplyGarbageCollection executes an exact reviewed plan. It is idempotent
// when the underlying Delete operation treats missing keys as success.
func (target *ObjectStoreTarget) ApplyGarbageCollection(ctx context.Context, plan ObjectStoreGarbageCollectionPlan) (int, error) {
	if target == nil {
		return 0, ErrObjectStoreGarbageCollectionPlanInvalid
	}
	if err := target.validate(ctx); err != nil {
		return 0, err
	}
	if err := checkObjectStoreContext(ctx); err != nil {
		return 0, err
	}
	if plan.Prefix != target.prefix {
		return 0, ErrObjectStoreGarbageCollectionPlanInvalid
	}
	deleter, ok := target.store.(ObjectStoreObjectDeleter)
	if !ok {
		return 0, ErrObjectStoreGarbageCollectionUnsupported
	}
	keep := make(map[string]struct{}, len(plan.KeepObjectKeys))
	for _, key := range plan.KeepObjectKeys {
		canonical, valid := target.canonicalContentObjectKey(key)
		if !valid || canonical != key {
			return 0, ErrObjectStoreGarbageCollectionPlanInvalid
		}
		keep[key] = struct{}{}
	}
	seen := make(map[string]struct{}, len(plan.DeleteObjectKeys))
	for _, key := range plan.DeleteObjectKeys {
		canonical, valid := target.canonicalContentObjectKey(key)
		if !valid || canonical != key {
			return 0, ErrObjectStoreGarbageCollectionPlanInvalid
		}
		if _, retained := keep[key]; retained {
			return 0, ErrObjectStoreGarbageCollectionPlanInvalid
		}
		if _, duplicate := seen[key]; duplicate {
			return 0, ErrObjectStoreGarbageCollectionPlanInvalid
		}
		seen[key] = struct{}{}
		if err := checkObjectStoreContext(ctx); err != nil {
			return 0, err
		}
		if err := deleter.Delete(ctx, key); err != nil {
			return len(seen) - 1, err
		}
	}
	return len(seen), nil
}

func validateGarbageCollectionRetention(retention BackupRetentionPlan) (BackupChainPlan, []string, map[string]struct{}, error) {
	if retention.Retain < 1 || len(retention.Chain.Manifests) == 0 || retention.Chain.LatestBackupID == "" {
		return BackupChainPlan{}, nil, nil, ErrObjectStoreGarbageCollectionUnsafe
	}
	chain, err := PlanBackupChain(retention.Chain.Manifests, retention.Chain.LatestBackupID)
	if err != nil {
		return BackupChainPlan{}, nil, nil, errors.Join(ErrObjectStoreGarbageCollectionUnsafe, err)
	}
	if retention.Retain > len(chain.Manifests) {
		return BackupChainPlan{}, nil, nil, ErrObjectStoreGarbageCollectionUnsafe
	}
	start := len(chain.Manifests) - retention.Retain
	keepIDs := make([]string, retention.Retain)
	keepIDSet := make(map[string]struct{}, retention.Retain)
	for index := range keepIDs {
		keepIDs[index] = chain.Manifests[start+index].BackupID
		keepIDSet[keepIDs[index]] = struct{}{}
	}
	if len(retention.KeepBackupIDs) != len(keepIDs) {
		return BackupChainPlan{}, nil, nil, ErrObjectStoreGarbageCollectionUnsafe
	}
	for index, id := range retention.KeepBackupIDs {
		if id != keepIDs[index] {
			return BackupChainPlan{}, nil, nil, ErrObjectStoreGarbageCollectionUnsafe
		}
	}
	keepRelative := make(map[string]struct{})
	for _, manifest := range chain.Manifests {
		layout, err := normalizeObjectStoreLayout(ObjectStoreLayout(manifest.ObjectLayout))
		if err != nil || layout != ObjectStoreLayoutContentAddressed {
			return BackupChainPlan{}, nil, nil, ErrObjectStoreGarbageCollectionUnsafe
		}
		if _, keep := keepIDSet[manifest.BackupID]; !keep {
			continue
		}
		for _, file := range manifest.Files {
			for _, relative := range backupObjectIdentities(manifest, file) {
				canonical, err := normalizeContentObjectRelative(relative)
				if err != nil {
					return BackupChainPlan{}, nil, nil, errors.Join(ErrObjectStoreGarbageCollectionUnsafe, err)
				}
				keepRelative[canonical] = struct{}{}
			}
		}
	}
	return chain, keepIDs, keepRelative, nil
}

func (target *ObjectStoreTarget) canonicalContentObjectKey(key string) (string, bool) {
	prefix := target.objectKey(objectStoreContentPrefix) + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	relative, ok := target.canonicalContentObjectRelative(key)
	if !ok {
		return "", false
	}
	return target.objectKey(relative), true
}

func (target *ObjectStoreTarget) canonicalContentObjectRelative(key string) (string, bool) {
	prefix := target.objectKey(objectStoreContentPrefix) + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	relative, err := normalizeContentObjectRelative(strings.TrimPrefix(key, target.prefix+"/"))
	if err != nil {
		return "", false
	}
	if target.objectKey(relative) != key {
		return "", false
	}
	return relative, true
}

func normalizeContentObjectRelative(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || path.Clean(value) != value || strings.HasPrefix(value, "/") {
		return "", ErrObjectStoreGarbageCollectionPlanInvalid
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 && len(parts) != 3 || parts[0] != objectStoreContentPrefix {
		return "", ErrObjectStoreGarbageCollectionPlanInvalid
	}
	keyID := ""
	if len(parts) == 3 {
		var err error
		keyID, err = normalizeObjectStoreEncryptionKeyID(parts[1])
		if err != nil {
			return "", err
		}
	}
	relative, err := contentObjectRelative(parts[len(parts)-1], keyID)
	if err != nil {
		return "", err
	}
	return relative, nil
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
