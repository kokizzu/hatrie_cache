// Package hatExtension defines the importable, transport-neutral contract for
// optional native extensions. It validates and selects extensions but never
// loads shared objects or executes native code.
package hatExtension

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// NativeExtensionABIV1 is the first stable ABI manifest version understood by
// this package. A loader must reject an extension with another ABI before it
// hands the extension to Registry.
const NativeExtensionABIV1 = "hatrie-cache/native/v1"

// Capability declares the kind of work an extension can perform. Unknown
// capabilities are accepted when they use the same safe token syntax, so the
// boundary can evolve without changing this package.
type Capability string

const (
	CapabilitySQLFunction Capability = "sql.function"
	CapabilitySQLSource   Capability = "sql.source"
	CapabilitySQLIndex    Capability = "sql.index"
	CapabilitySQLOutput   Capability = "sql.output"
)

var (
	// ErrRegistryNil reports a method call on a nil Registry.
	ErrRegistryNil = errors.New("native extension registry is nil")
	// ErrExtensionNil reports a nil extension value.
	ErrExtensionNil = errors.New("native extension is nil")
	// ErrLoaderNil reports a nil caller-owned loader.
	ErrLoaderNil = errors.New("native extension loader is nil")
	// ErrManifestInvalid reports malformed or incomplete manifest data.
	ErrManifestInvalid = errors.New("native extension manifest is invalid")
	// ErrABIMismatch reports a manifest ABI this package does not understand.
	ErrABIMismatch = errors.New("native extension ABI is unsupported")
	// ErrExtensionDuplicate reports an initial registration for an occupied name.
	ErrExtensionDuplicate = errors.New("native extension already exists")
	// ErrVersionRequired reports a replacement or unregister without fencing.
	ErrVersionRequired = errors.New("native extension expected version is required")
	// ErrVersionConflict reports an optimistic version check failure.
	ErrVersionConflict = errors.New("native extension version conflict")
	// ErrVersionUnchanged reports a replacement with the active version.
	ErrVersionUnchanged = errors.New("native extension version is unchanged")
	// ErrExtensionNotFound reports an unknown extension name.
	ErrExtensionNotFound = errors.New("native extension was not found")
	// ErrManifestMismatch reports a loader returning a different extension.
	ErrManifestMismatch = errors.New("native extension manifest mismatch")
)

// Manifest is the attested identity and capability description of an
// extension. ChecksumSHA256 is the digest of the native artifact as verified
// by the caller-owned loader, not a digest calculated by this package.
type Manifest struct {
	Name           string
	Version        string
	ABI            string
	EntryPoint     string
	Platform       string
	ChecksumSHA256 string
	Capabilities   []Capability
}

// Normalize validates a manifest and returns an owned, deterministic copy.
// The input capability slice is never retained.
func (manifest Manifest) Normalize() (Manifest, error) {
	normalized := manifest
	normalized.Name = strings.TrimSpace(manifest.Name)
	normalized.Version = strings.TrimSpace(manifest.Version)
	normalized.ABI = strings.TrimSpace(manifest.ABI)
	normalized.EntryPoint = strings.TrimSpace(manifest.EntryPoint)
	normalized.Platform = strings.TrimSpace(manifest.Platform)
	normalized.ChecksumSHA256 = strings.ToLower(strings.TrimSpace(manifest.ChecksumSHA256))

	if !validToken(normalized.Name, "._-") || !validToken(normalized.Version, "._-+") || !validToken(normalized.EntryPoint, "._-") {
		return Manifest{}, fmt.Errorf("%w: name, version, and entry point must be safe tokens", ErrManifestInvalid)
	}
	if normalized.ABI != NativeExtensionABIV1 {
		return Manifest{}, fmt.Errorf("%w: %q", ErrABIMismatch, normalized.ABI)
	}
	if !validPlatform(normalized.Platform) {
		return Manifest{}, fmt.Errorf("%w: platform must use os/arch form", ErrManifestInvalid)
	}
	if len(normalized.ChecksumSHA256) != 64 {
		return Manifest{}, fmt.Errorf("%w: checksum must be a SHA-256 hex digest", ErrManifestInvalid)
	}
	if _, err := hex.DecodeString(normalized.ChecksumSHA256); err != nil {
		return Manifest{}, fmt.Errorf("%w: checksum must be a SHA-256 hex digest", ErrManifestInvalid)
	}
	if len(manifest.Capabilities) == 0 {
		return Manifest{}, fmt.Errorf("%w: at least one capability is required", ErrManifestInvalid)
	}

	normalized.Capabilities = make([]Capability, len(manifest.Capabilities))
	seen := make(map[Capability]struct{}, len(manifest.Capabilities))
	for index, capability := range manifest.Capabilities {
		capability = Capability(strings.TrimSpace(string(capability)))
		if !validToken(string(capability), "._-") {
			return Manifest{}, fmt.Errorf("%w: capability %q is not a safe token", ErrManifestInvalid, capability)
		}
		if _, exists := seen[capability]; exists {
			return Manifest{}, fmt.Errorf("%w: capability %q is duplicated", ErrManifestInvalid, capability)
		}
		seen[capability] = struct{}{}
		normalized.Capabilities[index] = capability
	}
	sort.Slice(normalized.Capabilities, func(left, right int) bool {
		return normalized.Capabilities[left] < normalized.Capabilities[right]
	})
	return normalized, nil
}

// Validate checks the manifest without returning its normalized copy.
func (manifest Manifest) Validate() error {
	_, err := manifest.Normalize()
	return err
}

// HasCapability reports whether the manifest declares capability.
func (manifest Manifest) HasCapability(capability Capability) bool {
	capability = Capability(strings.TrimSpace(string(capability)))
	for _, declared := range manifest.Capabilities {
		if declared == capability {
			return true
		}
	}
	return false
}

// NativeExtension is implemented by a caller-owned adapter. Invoke is an
// opaque request/response boundary; Registry never calls it automatically.
type NativeExtension interface {
	Manifest() Manifest
	Invoke(context.Context, []byte) ([]byte, error)
}

// Loader is implemented by the application that owns native loading policy.
// A Loader may use cgo, a subprocess, WASI, or another mechanism. This
// package only verifies the returned extension's manifest before registration.
type Loader interface {
	Load(context.Context, Manifest) (NativeExtension, error)
}

// LoaderFunc adapts a function to Loader.
type LoaderFunc func(context.Context, Manifest) (NativeExtension, error)

// Load implements Loader.
func (loader LoaderFunc) Load(ctx context.Context, manifest Manifest) (NativeExtension, error) {
	if loader == nil {
		return nil, ErrLoaderNil
	}
	return loader(ctx, manifest)
}

// Metadata identifies one registered extension generation.
type Metadata struct {
	Manifest   Manifest
	Generation uint64
}

type extensionEntry struct {
	extension NativeExtension
	metadata  Metadata
}

// Registry atomically selects named, versioned extensions. It is deliberately
// a control-plane component: callers load and invoke extensions themselves.
type Registry struct {
	mu             sync.RWMutex
	extensions     map[string]extensionEntry
	nextGeneration uint64
}

// NewRegistry creates an empty extension registry.
func NewRegistry() *Registry {
	return &Registry{extensions: make(map[string]extensionEntry)}
}

// Register installs an already-loaded extension. An empty expectedVersion is
// valid only for an initial registration; replacements require the active
// version observed by the caller.
func (registry *Registry) Register(extension NativeExtension, expectedVersion string) (Metadata, error) {
	if registry == nil {
		return Metadata{}, ErrRegistryNil
	}
	normalized, err := extensionManifest(extension)
	if err != nil {
		return Metadata{}, err
	}
	return registry.registerNormalized(extension, normalized, expectedVersion)
}

// Load asks a caller-owned Loader to produce an extension, verifies that it
// returned the requested manifest, and then applies the same version fence as
// Register. It does not load a shared object itself.
func (registry *Registry) Load(ctx context.Context, loader Loader, requested Manifest, expectedVersion string) (Metadata, error) {
	if registry == nil {
		return Metadata{}, ErrRegistryNil
	}
	if isNilInterface(loader) {
		return Metadata{}, ErrLoaderNil
	}
	normalized, err := requested.Normalize()
	if err != nil {
		return Metadata{}, err
	}
	extension, err := loader.Load(ctx, normalized)
	if err != nil {
		return Metadata{}, err
	}
	loaded, err := extensionManifest(extension)
	if err != nil {
		return Metadata{}, err
	}
	if !sameManifest(normalized, loaded) {
		return Metadata{}, ErrManifestMismatch
	}
	return registry.registerNormalized(extension, loaded, expectedVersion)
}

// Resolve returns the active extension without copying metadata. This is the
// allocation-free lookup path; use Metadata when identity details are needed.
func (registry *Registry) Resolve(name string) (NativeExtension, bool) {
	if registry == nil {
		return nil, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, false
	}
	registry.mu.RLock()
	entry, found := registry.extensions[name]
	registry.mu.RUnlock()
	if !found {
		return nil, false
	}
	return entry.extension, true
}

// Metadata returns a copy of the active extension metadata.
func (registry *Registry) Metadata(name string) (Metadata, bool) {
	if registry == nil {
		return Metadata{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return Metadata{}, false
	}
	registry.mu.RLock()
	entry, found := registry.extensions[name]
	registry.mu.RUnlock()
	if !found {
		return Metadata{}, false
	}
	return cloneMetadata(entry.metadata), true
}

// Snapshot returns active metadata in deterministic name order.
func (registry *Registry) Snapshot() []Metadata {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	snapshot := make([]Metadata, 0, len(registry.extensions))
	for _, entry := range registry.extensions {
		snapshot = append(snapshot, cloneMetadata(entry.metadata))
	}
	registry.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool {
		return snapshot[left].Manifest.Name < snapshot[right].Manifest.Name
	})
	return snapshot
}

// Unregister removes an extension only when expectedVersion matches its
// current version, preventing a stale owner from removing a replacement.
func (registry *Registry) Unregister(name, expectedVersion string) error {
	if registry == nil {
		return ErrRegistryNil
	}
	name = strings.TrimSpace(name)
	expectedVersion = strings.TrimSpace(expectedVersion)
	if name == "" {
		return ErrManifestInvalid
	}
	if expectedVersion == "" {
		return ErrVersionRequired
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, found := registry.extensions[name]
	if !found {
		return ErrExtensionNotFound
	}
	if entry.metadata.Manifest.Version != expectedVersion {
		return ErrVersionConflict
	}
	delete(registry.extensions, name)
	return nil
}

func (registry *Registry) registerNormalized(extension NativeExtension, manifest Manifest, expectedVersion string) (Metadata, error) {
	expectedVersion = strings.TrimSpace(expectedVersion)
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.extensions == nil {
		registry.extensions = make(map[string]extensionEntry)
	}
	current, exists := registry.extensions[manifest.Name]
	if !exists {
		if expectedVersion != "" {
			return Metadata{}, ErrVersionConflict
		}
		return registry.installLocked(extension, manifest), nil
	}
	if expectedVersion == "" {
		return Metadata{}, ErrVersionRequired
	}
	if expectedVersion != current.metadata.Manifest.Version {
		return Metadata{}, ErrVersionConflict
	}
	if manifest.Version == current.metadata.Manifest.Version {
		return Metadata{}, ErrVersionUnchanged
	}
	return registry.installLocked(extension, manifest), nil
}

func (registry *Registry) installLocked(extension NativeExtension, manifest Manifest) Metadata {
	registry.nextGeneration++
	metadata := Metadata{Manifest: cloneManifest(manifest), Generation: registry.nextGeneration}
	registry.extensions[manifest.Name] = extensionEntry{extension: extension, metadata: metadata}
	return cloneMetadata(metadata)
}

func extensionManifest(extension NativeExtension) (Manifest, error) {
	if isNilInterface(extension) {
		return Manifest{}, ErrExtensionNil
	}
	manifest, err := extension.Manifest().Normalize()
	if err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func cloneManifest(manifest Manifest) Manifest {
	manifest.Capabilities = append([]Capability(nil), manifest.Capabilities...)
	return manifest
}

func cloneMetadata(metadata Metadata) Metadata {
	metadata.Manifest = cloneManifest(metadata.Manifest)
	return metadata
}

func sameManifest(left, right Manifest) bool {
	if left.Name != right.Name || left.Version != right.Version || left.ABI != right.ABI || left.EntryPoint != right.EntryPoint || left.Platform != right.Platform || left.ChecksumSHA256 != right.ChecksumSHA256 {
		return false
	}
	if len(left.Capabilities) != len(right.Capabilities) {
		return false
	}
	for index := range left.Capabilities {
		if left.Capabilities[index] != right.Capabilities[index] {
			return false
		}
	}
	return true
}

func validPlatform(platform string) bool {
	parts := strings.Split(platform, "/")
	return len(parts) == 2 && validToken(parts[0], "._-") && validToken(parts[1], "._-")
}

func validToken(value, extra string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune(extra, character) {
			continue
		}
		return false
	}
	return true
}

func isNilInterface(value interface{}) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
