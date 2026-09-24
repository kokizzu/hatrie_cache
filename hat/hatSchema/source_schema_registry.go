package hatSchema

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// DefaultSourceSchemaRegistryMaxSources bounds the number of independent
	// source histories in a registry created with zero-value options.
	DefaultSourceSchemaRegistryMaxSources = 1024
	// DefaultSourceSchemaRegistryMaxVersionsPerSource bounds retained rollback
	// points for each source created with zero-value options.
	DefaultSourceSchemaRegistryMaxVersionsPerSource = 8
	// MaxSourceSchemaRegistrySourceBytes bounds one source identity.
	MaxSourceSchemaRegistrySourceBytes = 256
	// MaxSourceSchemaRegistryFingerprintBytes bounds one schema fingerprint.
	MaxSourceSchemaRegistryFingerprintBytes = 256
)

// SourceSchemaCompatibilityMode selects the compatibility direction used by
// a caller-provided validator when a fingerprint changes.
type SourceSchemaCompatibilityMode uint8

const (
	// SourceSchemaCompatibilityExact accepts only an unchanged fingerprint.
	SourceSchemaCompatibilityExact SourceSchemaCompatibilityMode = iota
	// SourceSchemaCompatibilityBackward lets new readers consume old data.
	SourceSchemaCompatibilityBackward
	// SourceSchemaCompatibilityForward lets old readers consume new data.
	SourceSchemaCompatibilityForward
	// SourceSchemaCompatibilityFull requires both compatibility directions.
	SourceSchemaCompatibilityFull
)

// String returns the stable configuration spelling.
func (mode SourceSchemaCompatibilityMode) String() string {
	switch mode {
	case SourceSchemaCompatibilityExact:
		return "exact"
	case SourceSchemaCompatibilityBackward:
		return "backward"
	case SourceSchemaCompatibilityForward:
		return "forward"
	case SourceSchemaCompatibilityFull:
		return "full"
	default:
		return "unknown"
	}
}

func (mode SourceSchemaCompatibilityMode) valid() bool {
	return mode <= SourceSchemaCompatibilityFull
}

// SourceSchemaVersion is the secret-free identity of one source schema
// version. The registry deliberately retains no raw schema bytes or rows.
type SourceSchemaVersion struct {
	Source      string `json:"source"`
	Version     uint64 `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

// SourceSchemaCompatibilityValidator adapts a format-specific registry
// validator, such as Avro or Protobuf, to the generic source registry. The
// callback receives metadata only; implementations may resolve raw schemas
// from their own bounded cache. Its error is intentionally not exposed.
type SourceSchemaCompatibilityValidator func(previous, candidate SourceSchemaVersion, mode SourceSchemaCompatibilityMode) error

// SourceSchemaRegistryOptions controls memory bounds and compatibility policy.
// Zero limits use the documented defaults. Exact matching remains the safe
// default when ValidateCompatibility is nil.
type SourceSchemaRegistryOptions struct {
	MaxSources            int
	MaxVersionsPerSource  int
	CompatibilityMode     SourceSchemaCompatibilityMode
	ValidateCompatibility SourceSchemaCompatibilityValidator
}

// SourceSchemaValidationReason explains an admission result without exposing
// raw schema material or a format-specific parser error.
type SourceSchemaValidationReason uint8

const (
	SourceSchemaAccepted SourceSchemaValidationReason = iota
	SourceSchemaRejectedInvalid
	SourceSchemaRejectedConflict
	SourceSchemaRejectedStale
	SourceSchemaRejectedIncompatible
	SourceSchemaRejectedCapacity
	SourceSchemaRejectedNotFound
	SourceSchemaRejectedRollbackUnavailable
)

// String returns the stable diagnostic reason spelling.
func (reason SourceSchemaValidationReason) String() string {
	switch reason {
	case SourceSchemaAccepted:
		return "accepted"
	case SourceSchemaRejectedInvalid:
		return "invalid"
	case SourceSchemaRejectedConflict:
		return "version conflict"
	case SourceSchemaRejectedStale:
		return "stale version"
	case SourceSchemaRejectedIncompatible:
		return "incompatible schema"
	case SourceSchemaRejectedCapacity:
		return "registry capacity reached"
	case SourceSchemaRejectedNotFound:
		return "version not found"
	case SourceSchemaRejectedRollbackUnavailable:
		return "rollback unavailable"
	default:
		return "unknown"
	}
}

// SourceSchemaValidationResult is returned by validation, activation, and
// rollback operations. Fingerprints are intentionally omitted from this
// result so it is safe to publish in operational logs.
type SourceSchemaValidationResult struct {
	Accepted         bool                         `json:"accepted"`
	Reason           SourceSchemaValidationReason `json:"reason"`
	Source           string                       `json:"source,omitempty"`
	ActiveVersion    uint64                       `json:"active_version,omitempty"`
	CandidateVersion uint64                       `json:"candidate_version,omitempty"`
}

var (
	// ErrSourceSchemaInvalid reports malformed source metadata or options.
	ErrSourceSchemaInvalid = errors.New("hatSchema: invalid source schema metadata")
	// ErrSourceSchemaVersionConflict reports reuse of a version with a new
	// fingerprint.
	ErrSourceSchemaVersionConflict = errors.New("hatSchema: source schema version conflict")
	// ErrSourceSchemaVersionStale reports an attempt to activate an older
	// version through the forward activation path.
	ErrSourceSchemaVersionStale = errors.New("hatSchema: source schema version is stale")
	// ErrSourceSchemaIncompatible reports a failed compatibility admission.
	ErrSourceSchemaIncompatible = errors.New("hatSchema: source schema is incompatible")
	// ErrSourceSchemaRegistryCapacity reports a bounded registry limit.
	ErrSourceSchemaRegistryCapacity = errors.New("hatSchema: source schema registry capacity reached")
	// ErrSourceSchemaSourceNotFound reports an unknown source identity.
	ErrSourceSchemaSourceNotFound = errors.New("hatSchema: source schema source not found")
	// ErrSourceSchemaVersionNotFound reports an evicted or unknown rollback
	// point.
	ErrSourceSchemaVersionNotFound = errors.New("hatSchema: source schema version not found")
	// ErrSourceSchemaRollbackUnavailable reports a forward target passed to
	// Rollback.
	ErrSourceSchemaRollbackUnavailable = errors.New("hatSchema: source schema rollback unavailable")
)

// SourceSchemaValidationError is a bounded, secret-safe admission error.
// It never wraps the format-specific validator error.
type SourceSchemaValidationError struct {
	Reason           SourceSchemaValidationReason
	Source           string
	ActiveVersion    uint64
	CandidateVersion uint64
}

func (err *SourceSchemaValidationError) Error() string {
	if err == nil {
		return "hatSchema: source schema validation failed"
	}
	return fmt.Sprintf("hatSchema: source schema %q rejected: %s (active version %d, candidate version %d)", err.Source, err.Reason.String(), err.ActiveVersion, err.CandidateVersion)
}

func (err *SourceSchemaValidationError) Unwrap() error {
	if err == nil {
		return nil
	}
	switch err.Reason {
	case SourceSchemaRejectedInvalid:
		return ErrSourceSchemaInvalid
	case SourceSchemaRejectedConflict:
		return ErrSourceSchemaVersionConflict
	case SourceSchemaRejectedStale:
		return ErrSourceSchemaVersionStale
	case SourceSchemaRejectedIncompatible:
		return ErrSourceSchemaIncompatible
	case SourceSchemaRejectedCapacity:
		return ErrSourceSchemaRegistryCapacity
	case SourceSchemaRejectedNotFound:
		return ErrSourceSchemaVersionNotFound
	case SourceSchemaRejectedRollbackUnavailable:
		return ErrSourceSchemaRollbackUnavailable
	default:
		return nil
	}
}

// SourceSchemaRegistryStats contains bounded operational counters and current
// retention sizes.
type SourceSchemaRegistryStats struct {
	Sources          int    `json:"sources"`
	RetainedVersions int    `json:"retained_versions"`
	Activations      uint64 `json:"activations"`
	Rollbacks        uint64 `json:"rollbacks"`
	Rejections       uint64 `json:"rejections"`
}

// SourceSchemaHistorySnapshot is one deterministic source history snapshot.
type SourceSchemaHistorySnapshot struct {
	Source        string                `json:"source"`
	ActiveVersion uint64                `json:"active_version"`
	Versions      []SourceSchemaVersion `json:"versions"`
}

// SourceSchemaRegistrySnapshot is a copy-safe, deterministic registry view.
type SourceSchemaRegistrySnapshot struct {
	Sources []SourceSchemaHistorySnapshot `json:"sources"`
}

type sourceSchemaHistory struct {
	active   uint64
	versions map[uint64]SourceSchemaVersion
}

// SourceSchemaRegistry admits source schema versions and retains bounded
// rollback points. It is safe for concurrent readers and writers.
type SourceSchemaRegistry struct {
	mu      sync.RWMutex
	options SourceSchemaRegistryOptions
	sources map[string]*sourceSchemaHistory
	stats   SourceSchemaRegistryStats
}

// NewSourceSchemaRegistry creates a bounded source registry. The registry is
// intentionally opt-in; it has no effect on existing CDC or SQL paths until a
// caller activates a candidate version.
func NewSourceSchemaRegistry(options SourceSchemaRegistryOptions) (*SourceSchemaRegistry, error) {
	if options.MaxSources == 0 {
		options.MaxSources = DefaultSourceSchemaRegistryMaxSources
	}
	if options.MaxVersionsPerSource == 0 {
		options.MaxVersionsPerSource = DefaultSourceSchemaRegistryMaxVersionsPerSource
	}
	if options.MaxSources < 1 || options.MaxSources > 1<<20 {
		return nil, fmt.Errorf("%w: max sources must be between 1 and %d", ErrSourceSchemaInvalid, 1<<20)
	}
	if options.MaxVersionsPerSource < 1 || options.MaxVersionsPerSource > 1<<16 {
		return nil, fmt.Errorf("%w: max versions per source must be between 1 and %d", ErrSourceSchemaInvalid, 1<<16)
	}
	if !options.CompatibilityMode.valid() {
		return nil, fmt.Errorf("%w: unknown compatibility mode %d", ErrSourceSchemaInvalid, options.CompatibilityMode)
	}
	return &SourceSchemaRegistry{
		options: options,
		sources: make(map[string]*sourceSchemaHistory),
	}, nil
}

// Options returns the immutable registry configuration.
func (registry *SourceSchemaRegistry) Options() SourceSchemaRegistryOptions {
	if registry == nil {
		return SourceSchemaRegistryOptions{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return registry.options
}

// Validate checks a candidate without changing the active version.
func (registry *SourceSchemaRegistry) Validate(candidate SourceSchemaVersion) (SourceSchemaValidationResult, error) {
	if registry == nil {
		return invalidSourceSchemaResult(candidate), &SourceSchemaValidationError{Reason: SourceSchemaRejectedInvalid, CandidateVersion: candidate.Version}
	}
	normalized, err := normalizeSourceSchemaVersion(candidate)
	if err != nil {
		return invalidSourceSchemaResult(candidate), err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	history, ok := registry.sources[normalized.Source]
	if !ok {
		if len(registry.sources) >= registry.options.MaxSources {
			return sourceSchemaRejection(normalized, SourceSchemaRejectedCapacity, 0)
		}
		return acceptedSourceSchemaResult(normalized, 0), nil
	}
	return registry.validateExistingLocked(history, normalized)
}

// Activate validates and atomically publishes a candidate. Repeating the
// active version with the same fingerprint is idempotent.
func (registry *SourceSchemaRegistry) Activate(candidate SourceSchemaVersion) (SourceSchemaValidationResult, error) {
	if registry == nil {
		return invalidSourceSchemaResult(candidate), &SourceSchemaValidationError{Reason: SourceSchemaRejectedInvalid, CandidateVersion: candidate.Version}
	}
	normalized, err := normalizeSourceSchemaVersion(candidate)
	if err != nil {
		return invalidSourceSchemaResult(candidate), err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	history, ok := registry.sources[normalized.Source]
	if !ok {
		if len(registry.sources) >= registry.options.MaxSources {
			return registry.rejectLocked(normalized, SourceSchemaRejectedCapacity, 0)
		}
		history = &sourceSchemaHistory{versions: make(map[uint64]SourceSchemaVersion)}
		registry.sources[normalized.Source] = history
	}
	result, err := registry.validateExistingLocked(history, normalized)
	if err != nil {
		registry.stats.Rejections++
		return result, err
	}
	if history.active != normalized.Version {
		registry.stats.Activations++
	}
	history.active = normalized.Version
	history.versions[normalized.Version] = normalized
	registry.trimHistoryLocked(history)
	result.ActiveVersion = history.active
	return result, nil
}

// Rollback activates a retained older version without invoking a format
// validator. Every retained version was already admitted by Activate.
func (registry *SourceSchemaRegistry) Rollback(source string, targetVersion uint64) (SourceSchemaValidationResult, error) {
	if registry == nil {
		return SourceSchemaValidationResult{Reason: SourceSchemaRejectedInvalid, CandidateVersion: targetVersion}, &SourceSchemaValidationError{Reason: SourceSchemaRejectedInvalid, CandidateVersion: targetVersion}
	}
	normalizedSource, err := normalizeSourceSchemaName(source)
	if err != nil || targetVersion == 0 {
		return SourceSchemaValidationResult{Source: normalizedSource, Reason: SourceSchemaRejectedInvalid, CandidateVersion: targetVersion}, &SourceSchemaValidationError{Reason: SourceSchemaRejectedInvalid, Source: normalizedSource, CandidateVersion: targetVersion}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	history, ok := registry.sources[normalizedSource]
	if !ok {
		return registry.rejectLocked(SourceSchemaVersion{Source: normalizedSource, Version: targetVersion}, SourceSchemaRejectedNotFound, 0)
	}
	target, ok := history.versions[targetVersion]
	if !ok {
		return registry.rejectLocked(SourceSchemaVersion{Source: normalizedSource, Version: targetVersion}, SourceSchemaRejectedNotFound, history.active)
	}
	if targetVersion > history.active {
		return registry.rejectLocked(target, SourceSchemaRejectedRollbackUnavailable, history.active)
	}
	if targetVersion == history.active {
		return acceptedSourceSchemaResult(target, history.active), nil
	}
	history.active = targetVersion
	registry.stats.Rollbacks++
	return acceptedSourceSchemaResult(target, history.active), nil
}

// Current returns the active version for one source.
func (registry *SourceSchemaRegistry) Current(source string) (SourceSchemaVersion, error) {
	if registry == nil {
		return SourceSchemaVersion{}, ErrSourceSchemaInvalid
	}
	normalizedSource, err := normalizeSourceSchemaName(source)
	if err != nil {
		return SourceSchemaVersion{}, err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	history, ok := registry.sources[normalizedSource]
	if !ok {
		return SourceSchemaVersion{}, ErrSourceSchemaSourceNotFound
	}
	return history.versions[history.active], nil
}

// Snapshot returns a deep, deterministic copy of all retained metadata.
func (registry *SourceSchemaRegistry) Snapshot() SourceSchemaRegistrySnapshot {
	if registry == nil {
		return SourceSchemaRegistrySnapshot{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	sources := make([]string, 0, len(registry.sources))
	for source := range registry.sources {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	out := SourceSchemaRegistrySnapshot{Sources: make([]SourceSchemaHistorySnapshot, 0, len(sources))}
	for _, source := range sources {
		history := registry.sources[source]
		versions := make([]SourceSchemaVersion, 0, len(history.versions))
		for _, version := range history.versions {
			versions = append(versions, version)
		}
		sort.Slice(versions, func(i, j int) bool { return versions[i].Version < versions[j].Version })
		out.Sources = append(out.Sources, SourceSchemaHistorySnapshot{
			Source:        source,
			ActiveVersion: history.active,
			Versions:      versions,
		})
	}
	return out
}

// Stats returns current bounded sizes and operation counters.
func (registry *SourceSchemaRegistry) Stats() SourceSchemaRegistryStats {
	if registry == nil {
		return SourceSchemaRegistryStats{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	stats := registry.stats
	stats.Sources = len(registry.sources)
	for _, history := range registry.sources {
		stats.RetainedVersions += len(history.versions)
	}
	return stats
}

func (registry *SourceSchemaRegistry) validateExistingLocked(history *sourceSchemaHistory, candidate SourceSchemaVersion) (SourceSchemaValidationResult, error) {
	if history.active == 0 {
		return acceptedSourceSchemaResult(candidate, 0), nil
	}
	active := history.versions[history.active]
	if existing, ok := history.versions[candidate.Version]; ok && existing.Fingerprint != candidate.Fingerprint {
		return sourceSchemaRejection(candidate, SourceSchemaRejectedConflict, history.active)
	}
	if candidate.Version == history.active {
		return acceptedSourceSchemaResult(candidate, history.active), nil
	}
	if candidate.Version < history.active {
		return sourceSchemaRejection(candidate, SourceSchemaRejectedStale, history.active)
	}
	if candidate.Fingerprint == active.Fingerprint {
		return acceptedSourceSchemaResult(candidate, history.active), nil
	}
	if registry.options.CompatibilityMode == SourceSchemaCompatibilityExact || registry.options.ValidateCompatibility == nil {
		return sourceSchemaRejection(candidate, SourceSchemaRejectedIncompatible, history.active)
	}
	if err := registry.options.ValidateCompatibility(active, candidate, registry.options.CompatibilityMode); err != nil {
		return sourceSchemaRejection(candidate, SourceSchemaRejectedIncompatible, history.active)
	}
	return acceptedSourceSchemaResult(candidate, history.active), nil
}

func (registry *SourceSchemaRegistry) rejectLocked(candidate SourceSchemaVersion, reason SourceSchemaValidationReason, activeVersion uint64) (SourceSchemaValidationResult, error) {
	registry.stats.Rejections++
	return sourceSchemaRejection(candidate, reason, activeVersion)
}

func sourceSchemaRejection(candidate SourceSchemaVersion, reason SourceSchemaValidationReason, activeVersion uint64) (SourceSchemaValidationResult, error) {
	result := SourceSchemaValidationResult{
		Accepted:         false,
		Reason:           reason,
		Source:           candidate.Source,
		ActiveVersion:    activeVersion,
		CandidateVersion: candidate.Version,
	}
	return result, &SourceSchemaValidationError{
		Reason:           reason,
		Source:           candidate.Source,
		ActiveVersion:    activeVersion,
		CandidateVersion: candidate.Version,
	}
}

func (registry *SourceSchemaRegistry) trimHistoryLocked(history *sourceSchemaHistory) {
	for len(history.versions) > registry.options.MaxVersionsPerSource {
		oldest := uint64(0)
		found := false
		for version := range history.versions {
			if version == history.active {
				continue
			}
			if !found || version < oldest {
				oldest = version
				found = true
			}
		}
		if !found {
			return
		}
		delete(history.versions, oldest)
	}
}

func acceptedSourceSchemaResult(candidate SourceSchemaVersion, activeVersion uint64) SourceSchemaValidationResult {
	if activeVersion == 0 {
		activeVersion = candidate.Version
	}
	return SourceSchemaValidationResult{
		Accepted:         true,
		Reason:           SourceSchemaAccepted,
		Source:           candidate.Source,
		ActiveVersion:    activeVersion,
		CandidateVersion: candidate.Version,
	}
}

func invalidSourceSchemaResult(candidate SourceSchemaVersion) SourceSchemaValidationResult {
	return SourceSchemaValidationResult{
		Accepted:         false,
		Reason:           SourceSchemaRejectedInvalid,
		Source:           strings.TrimSpace(candidate.Source),
		CandidateVersion: candidate.Version,
	}
}

func normalizeSourceSchemaVersion(candidate SourceSchemaVersion) (SourceSchemaVersion, error) {
	source, err := normalizeSourceSchemaName(candidate.Source)
	if err != nil {
		return SourceSchemaVersion{}, err
	}
	fingerprint := strings.TrimSpace(candidate.Fingerprint)
	if fingerprint == "" || len(fingerprint) > MaxSourceSchemaRegistryFingerprintBytes || !utf8.ValidString(fingerprint) {
		return SourceSchemaVersion{}, fmt.Errorf("%w: fingerprint must be valid, non-empty, and at most %d bytes", ErrSourceSchemaInvalid, MaxSourceSchemaRegistryFingerprintBytes)
	}
	if candidate.Version == 0 {
		return SourceSchemaVersion{}, fmt.Errorf("%w: version must be positive", ErrSourceSchemaInvalid)
	}
	return SourceSchemaVersion{Source: source, Version: candidate.Version, Fingerprint: fingerprint}, nil
}

func normalizeSourceSchemaName(source string) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" || len(source) > MaxSourceSchemaRegistrySourceBytes || !utf8.ValidString(source) {
		return "", fmt.Errorf("%w: source must be valid, non-empty, and at most %d bytes", ErrSourceSchemaInvalid, MaxSourceSchemaRegistrySourceBytes)
	}
	return source, nil
}
