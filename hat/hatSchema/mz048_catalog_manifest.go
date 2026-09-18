package hatSchema

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// SpaceCatalogManifestVersion is the current manifest schema version.
	SpaceCatalogManifestVersion uint64 = 1
	// DefaultSpaceCatalogManifestMaxBytes bounds one persisted catalog file.
	DefaultSpaceCatalogManifestMaxBytes = 8 << 20
	maxSpaceCatalogManifestBytes        = 64 << 20
	maxSpaceCatalogManifestVersion      = 64
	spaceCatalogManifestFormatVersion   = 1
)

var (
	// ErrSpaceCatalogManifestInvalid indicates malformed manifest data.
	ErrSpaceCatalogManifestInvalid = errors.New("hatSchema: space catalog manifest is invalid")
	// ErrSpaceCatalogManifestChecksum indicates that the persisted payload was
	// modified or truncated after publication.
	ErrSpaceCatalogManifestChecksum = errors.New("hatSchema: space catalog manifest checksum mismatch")
	// ErrSpaceCatalogManifestSize indicates a payload or configured limit that
	// exceeds the supported bound.
	ErrSpaceCatalogManifestSize = errors.New("hatSchema: space catalog manifest size limit exceeded")
	// ErrSpaceCatalogManifestPath indicates an unsafe or non-regular path.
	ErrSpaceCatalogManifestPath = errors.New("hatSchema: space catalog manifest path is invalid")
	// ErrSpaceCatalogManifestVersionInvalid indicates an unsupported version
	// direction or version value.
	ErrSpaceCatalogManifestVersionInvalid = errors.New("hatSchema: space catalog manifest version is invalid")
	// ErrSpaceCatalogManifestMigrationInvalid indicates a migration that does
	// not advance exactly one version.
	ErrSpaceCatalogManifestMigrationInvalid = errors.New("hatSchema: space catalog manifest migration is invalid")
	// ErrSpaceCatalogManifestMigrationExists indicates a duplicate migration
	// registration for one source version.
	ErrSpaceCatalogManifestMigrationExists = errors.New("hatSchema: space catalog manifest migration already exists")
	// ErrSpaceCatalogManifestMigrationMissing indicates that a version step was
	// not registered.
	ErrSpaceCatalogManifestMigrationMissing = errors.New("hatSchema: space catalog manifest migration is missing")
	// ErrSpaceCatalogManifestStoreNil indicates a method call on a nil store.
	ErrSpaceCatalogManifestStoreNil = errors.New("hatSchema: space catalog manifest store is nil")
)

// SpaceCatalogManifest is the durable, versioned representation of a
// SpaceCatalog. Spaces are normalized and sorted before publication.
type SpaceCatalogManifest struct {
	Version    uint64            `json:"version"`
	Generation uint64            `json:"generation,omitempty"`
	Spaces     []SpaceDefinition `json:"spaces,omitempty"`
}

// SpaceCatalogManifestMigration advances one manifest version. The callback
// owns the supplied value and must return Version+1.
type SpaceCatalogManifestMigration func(SpaceCatalogManifest) (SpaceCatalogManifest, error)

// SpaceCatalogManifestMigrator holds bounded, one-version migration steps.
// Registration is normally completed during process setup and migration is
// safe to call concurrently with reads after that setup phase.
type SpaceCatalogManifestMigrator struct {
	mu    sync.RWMutex
	steps map[uint64]SpaceCatalogManifestMigration
}

// NewSpaceCatalogManifestMigrator creates a migrator with the built-in v0 to
// v1 legacy normalization step.
func NewSpaceCatalogManifestMigrator() *SpaceCatalogManifestMigrator {
	return &SpaceCatalogManifestMigrator{
		steps: map[uint64]SpaceCatalogManifestMigration{
			0: func(manifest SpaceCatalogManifest) (SpaceCatalogManifest, error) {
				manifest.Version = SpaceCatalogManifestVersion
				return manifest, nil
			},
		},
	}
}

// Register adds one migration from fromVersion to fromVersion+1.
func (migrator *SpaceCatalogManifestMigrator) Register(fromVersion uint64, migration SpaceCatalogManifestMigration) error {
	if migrator == nil || migration == nil || fromVersion >= maxSpaceCatalogManifestVersion {
		return ErrSpaceCatalogManifestMigrationInvalid
	}
	migrator.mu.Lock()
	defer migrator.mu.Unlock()
	if migrator.steps == nil {
		migrator.steps = make(map[uint64]SpaceCatalogManifestMigration)
	}
	if _, exists := migrator.steps[fromVersion]; exists {
		return ErrSpaceCatalogManifestMigrationExists
	}
	migrator.steps[fromVersion] = migration
	return nil
}

// Migrate applies registered steps until targetVersion is reached. Every
// intermediate manifest is normalized and validated before the next step.
func (migrator *SpaceCatalogManifestMigrator) Migrate(manifest SpaceCatalogManifest, targetVersion uint64) (SpaceCatalogManifest, error) {
	if migrator == nil || targetVersion == 0 || targetVersion > maxSpaceCatalogManifestVersion || manifest.Version > targetVersion {
		return SpaceCatalogManifest{}, ErrSpaceCatalogManifestVersionInvalid
	}
	for steps := uint64(0); manifest.Version < targetVersion; steps++ {
		if steps >= maxSpaceCatalogManifestVersion {
			return SpaceCatalogManifest{}, ErrSpaceCatalogManifestVersionInvalid
		}
		migrator.mu.RLock()
		migration, ok := migrator.steps[manifest.Version]
		migrator.mu.RUnlock()
		if !ok {
			return SpaceCatalogManifest{}, fmt.Errorf("%w: version %d", ErrSpaceCatalogManifestMigrationMissing, manifest.Version)
		}
		previousVersion := manifest.Version
		migrated, err := migration(cloneSpaceCatalogManifest(manifest))
		if err != nil {
			return SpaceCatalogManifest{}, fmt.Errorf("migrate version %d: %w", previousVersion, err)
		}
		if migrated.Version != previousVersion+1 {
			return SpaceCatalogManifest{}, fmt.Errorf("%w: step from %d returned %d", ErrSpaceCatalogManifestMigrationInvalid, previousVersion, migrated.Version)
		}
		manifest, err = normalizeSpaceCatalogManifest(migrated)
		if err != nil {
			return SpaceCatalogManifest{}, err
		}
	}
	manifest, err := normalizeSpaceCatalogManifest(manifest)
	if err != nil {
		return SpaceCatalogManifest{}, err
	}
	return manifest, nil
}

// SpaceCatalogManifestStoreOptions configures a persisted manifest store.
// Zero values select the current version, default migrator, and default byte
// limit.
type SpaceCatalogManifestStoreOptions struct {
	MaxBytes      int
	TargetVersion uint64
	Migrator      *SpaceCatalogManifestMigrator
}

// SpaceCatalogManifestStore atomically publishes and loads one manifest path.
// A store serializes its own operations; callers sharing a path across
// processes should provide process-level ownership.
type SpaceCatalogManifestStore struct {
	mu            sync.Mutex
	path          string
	maxBytes      int
	targetVersion uint64
	migrator      *SpaceCatalogManifestMigrator
}

type spaceCatalogManifestEnvelope struct {
	Format   uint64          `json:"format"`
	Manifest json.RawMessage `json:"manifest"`
	SHA256   string          `json:"sha256"`
}

// NewSpaceCatalogManifestStore validates a regular-file destination and
// creates a store. Parent directories are created by the first Publish.
func NewSpaceCatalogManifestStore(path string, options SpaceCatalogManifestStoreOptions) (*SpaceCatalogManifestStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrSpaceCatalogManifestPath
	}
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return nil, ErrSpaceCatalogManifestPath
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSpaceCatalogManifestMaxBytes
	}
	if maxBytes < 1 || maxBytes > maxSpaceCatalogManifestBytes {
		return nil, ErrSpaceCatalogManifestSize
	}
	targetVersion := options.TargetVersion
	if targetVersion == 0 {
		targetVersion = SpaceCatalogManifestVersion
	}
	if targetVersion > maxSpaceCatalogManifestVersion {
		return nil, ErrSpaceCatalogManifestVersionInvalid
	}
	if err := validateSpaceCatalogManifestPath(path); err != nil {
		return nil, err
	}
	migrator := options.Migrator
	if migrator == nil {
		migrator = NewSpaceCatalogManifestMigrator()
	}
	return &SpaceCatalogManifestStore{
		path:          path,
		maxBytes:      maxBytes,
		targetVersion: targetVersion,
		migrator:      migrator,
	}, nil
}

// Load reads and validates the current persisted manifest without applying
// migrations. Use LoadAndMigrate when an older version must be upgraded.
func (store *SpaceCatalogManifestStore) Load() (SpaceCatalogManifest, error) {
	if store == nil {
		return SpaceCatalogManifest{}, ErrSpaceCatalogManifestStoreNil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	manifest, exists, _, err := store.loadLocked()
	if err != nil {
		return SpaceCatalogManifest{}, err
	}
	if !exists {
		return SpaceCatalogManifest{Version: store.targetVersion}, nil
	}
	return manifest, nil
}

// LoadAndMigrate loads the manifest, applies all registered steps to the
// configured target version, and atomically publishes only after all steps
// and schema validation succeed.
func (store *SpaceCatalogManifestStore) LoadAndMigrate() (SpaceCatalogManifest, error) {
	if store == nil {
		return SpaceCatalogManifest{}, ErrSpaceCatalogManifestStoreNil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	manifest, exists, canonical, err := store.loadLocked()
	if err != nil {
		return SpaceCatalogManifest{}, err
	}
	if !exists {
		return SpaceCatalogManifest{Version: store.targetVersion}, nil
	}
	migrated, err := store.migrator.Migrate(manifest, store.targetVersion)
	if err != nil {
		return SpaceCatalogManifest{}, err
	}
	if manifest.Version != migrated.Version || !canonical {
		if err := store.publishLocked(migrated); err != nil {
			return SpaceCatalogManifest{}, err
		}
	}
	return migrated, nil
}

// Publish validates and atomically replaces the persisted manifest. The
// manifest version must equal the store target version.
func (store *SpaceCatalogManifestStore) Publish(manifest SpaceCatalogManifest) error {
	if store == nil {
		return ErrSpaceCatalogManifestStoreNil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.publishLocked(manifest)
}

func (store *SpaceCatalogManifestStore) loadLocked() (SpaceCatalogManifest, bool, bool, error) {
	if err := validateSpaceCatalogManifestPath(store.path); err != nil {
		return SpaceCatalogManifest{}, false, false, err
	}
	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return SpaceCatalogManifest{}, false, false, nil
	}
	if err != nil {
		return SpaceCatalogManifest{}, false, false, fmt.Errorf("read space catalog manifest: %w", err)
	}
	if len(data) > store.maxBytes {
		return SpaceCatalogManifest{}, false, false, ErrSpaceCatalogManifestSize
	}
	manifest, canonical, err := decodeSpaceCatalogManifest(data)
	if err != nil {
		return SpaceCatalogManifest{}, false, false, err
	}
	if manifest.Version > store.targetVersion {
		return SpaceCatalogManifest{}, false, false, ErrSpaceCatalogManifestVersionInvalid
	}
	if manifest.Version == 0 {
		return manifest, true, canonical, nil
	}
	normalized, err := normalizeSpaceCatalogManifest(manifest)
	if err != nil {
		return SpaceCatalogManifest{}, false, false, err
	}
	return normalized, true, canonical, nil
}

func (store *SpaceCatalogManifestStore) publishLocked(manifest SpaceCatalogManifest) error {
	normalized, err := normalizeSpaceCatalogManifest(manifest)
	if err != nil {
		return err
	}
	if normalized.Version != store.targetVersion {
		return ErrSpaceCatalogManifestVersionInvalid
	}
	data, err := encodeSpaceCatalogManifest(normalized)
	if err != nil {
		return err
	}
	if len(data) > store.maxBytes {
		return ErrSpaceCatalogManifestSize
	}
	if err := validateSpaceCatalogManifestPath(store.path); err != nil {
		return err
	}
	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create space catalog manifest directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create space catalog manifest temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure space catalog manifest temporary file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write space catalog manifest temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync space catalog manifest temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close space catalog manifest temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("publish space catalog manifest: %w", err)
	}
	if err := syncSpaceCatalogManifestDirectory(directory); err != nil {
		return fmt.Errorf("sync space catalog manifest directory: %w", err)
	}
	return nil
}

func normalizeSpaceCatalogManifest(manifest SpaceCatalogManifest) (SpaceCatalogManifest, error) {
	if manifest.Version == 0 || manifest.Version > maxSpaceCatalogManifestVersion {
		return SpaceCatalogManifest{}, ErrSpaceCatalogManifestVersionInvalid
	}
	if len(manifest.Spaces) > MaxSpaceCatalogSpaces {
		return SpaceCatalogManifest{}, fmt.Errorf("%w: maximum spaces %d exceeded", ErrSpaceCatalogLimit, MaxSpaceCatalogSpaces)
	}
	spaces := make([]SpaceDefinition, len(manifest.Spaces))
	for index, definition := range manifest.Spaces {
		normalized, err := normalizeSpaceDefinition(definition)
		if err != nil {
			return SpaceCatalogManifest{}, err
		}
		spaces[index] = normalized
	}
	sort.Slice(spaces, func(left, right int) bool { return spaces[left].Name < spaces[right].Name })
	for index := 1; index < len(spaces); index++ {
		if spaces[index-1].Name == spaces[index].Name {
			return SpaceCatalogManifest{}, fmt.Errorf("%w: duplicate space %q", ErrSpaceCatalogInvalid, spaces[index].Name)
		}
	}
	return SpaceCatalogManifest{
		Version:    manifest.Version,
		Generation: manifest.Generation,
		Spaces:     spaces,
	}, nil
}

func cloneSpaceCatalogManifest(manifest SpaceCatalogManifest) SpaceCatalogManifest {
	clone := manifest
	clone.Spaces = make([]SpaceDefinition, len(manifest.Spaces))
	for index, definition := range manifest.Spaces {
		clone.Spaces[index] = cloneSpaceDefinition(definition)
	}
	return clone
}

func encodeSpaceCatalogManifest(manifest SpaceCatalogManifest) ([]byte, error) {
	body, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("encode space catalog manifest body: %w", err)
	}
	digest := sha256.Sum256(body)
	var digestHex [sha256.Size * 2]byte
	hex.Encode(digestHex[:], digest[:])
	const prefix = `{"format":1,"manifest":`
	const checksumPrefix = `,"sha256":"`
	encoded := make([]byte, 0, len(prefix)+len(body)+len(checksumPrefix)+len(digestHex)+2)
	encoded = append(encoded, prefix...)
	encoded = append(encoded, body...)
	encoded = append(encoded, checksumPrefix...)
	encoded = append(encoded, digestHex[:]...)
	encoded = append(encoded, '"', '}')
	return encoded, nil
}

func decodeSpaceCatalogManifest(data []byte) (SpaceCatalogManifest, bool, error) {
	if len(data) == 0 || len(data) > maxSpaceCatalogManifestBytes {
		return SpaceCatalogManifest{}, false, ErrSpaceCatalogManifestSize
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return SpaceCatalogManifest{}, false, ErrSpaceCatalogManifestInvalid
	}
	if _, envelope := fields["format"]; !envelope {
		var legacy SpaceCatalogManifest
		if err := json.Unmarshal(data, &legacy); err != nil {
			return SpaceCatalogManifest{}, false, ErrSpaceCatalogManifestInvalid
		}
		return legacy, false, nil
	}
	var envelope spaceCatalogManifestEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Format != spaceCatalogManifestFormatVersion || len(envelope.Manifest) == 0 {
		return SpaceCatalogManifest{}, false, ErrSpaceCatalogManifestInvalid
	}
	var manifest SpaceCatalogManifest
	if err := json.Unmarshal(envelope.Manifest, &manifest); err != nil {
		return SpaceCatalogManifest{}, false, ErrSpaceCatalogManifestInvalid
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return SpaceCatalogManifest{}, false, ErrSpaceCatalogManifestInvalid
	}
	digest := sha256.Sum256(body)
	expected := hex.EncodeToString(digest[:])
	provided := strings.TrimSpace(envelope.SHA256)
	if len(provided) != len(expected) || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		return SpaceCatalogManifest{}, false, ErrSpaceCatalogManifestChecksum
	}
	return manifest, true, nil
}

func validateSpaceCatalogManifestPath(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect space catalog manifest path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return ErrSpaceCatalogManifestPath
	}
	return nil
}

func syncSpaceCatalogManifestDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
