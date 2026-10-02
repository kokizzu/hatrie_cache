package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrRemotePartAttachmentNil                 = errors.New("hatriecache: remote-part attachment catalog is nil")
	ErrRemotePartAttachmentInvalid             = errors.New("hatriecache: remote-part attachment is invalid")
	ErrRemotePartAttachmentContextRequired     = errors.New("hatriecache: remote-part attachment context is required")
	ErrRemotePartAttachmentVerifierRequired    = errors.New("hatriecache: remote-part attachment verifier is required")
	ErrRemotePartAttachmentNotFound            = errors.New("hatriecache: remote-part attachment is not found")
	ErrRemotePartAttachmentAlreadyAttached     = errors.New("hatriecache: remote-part attachment is already active")
	ErrRemotePartAttachmentQuarantined         = errors.New("hatriecache: remote-part attachment is quarantined")
	ErrRemotePartAttachmentNotQuarantined      = errors.New("hatriecache: remote-part attachment is not quarantined")
	ErrRemotePartAttachmentStale               = errors.New("hatriecache: remote-part attachment generation is stale")
	ErrRemotePartAttachmentVerificationFailed  = errors.New("hatriecache: remote-part replacement verification failed")
	ErrRemotePartAttachmentGenerationExhausted = errors.New("hatriecache: remote-part attachment generation exhausted")
)

const (
	MaxRemotePartAttachmentKeyBytes    = 1024
	MaxRemotePartAttachmentReasonBytes = 4096
)

// RemotePartAttachmentVerifier verifies that a candidate immutable part is
// readable and matches the caller's integrity policy. It runs outside the
// catalog lock and owns all filesystem or object-storage I/O.
type RemotePartAttachmentVerifier func(context.Context, RemotePartReference) error

// RemotePartAttachment is a copy-safe publication snapshot. A quarantined
// entry remains discoverable for operator inspection but is not active.
type RemotePartAttachment struct {
	Key         string              `json:"key"`
	Reference   RemotePartReference `json:"reference"`
	Generation  uint64              `json:"generation"`
	Quarantined bool                `json:"quarantined"`
	Reason      string              `json:"reason,omitempty"`
}

// RemotePartAttachmentCatalog owns the active/quarantined state of immutable
// logical parts. It is deliberately a control-plane registry: it does not
// delete data, move files, or perform remote I/O.
type RemotePartAttachmentCatalog struct {
	mu             sync.RWMutex
	entries        map[string]RemotePartAttachment
	nextGeneration uint64
}

// NewRemotePartAttachmentCatalog creates an empty attachment registry.
func NewRemotePartAttachmentCatalog() *RemotePartAttachmentCatalog {
	return &RemotePartAttachmentCatalog{entries: make(map[string]RemotePartAttachment)}
}

// Attach verifies and publishes an initial active part. A quarantined part
// must be replaced with AttachReplacement so the quarantine generation is
// explicitly fenced.
func (catalog *RemotePartAttachmentCatalog) Attach(ctx context.Context, key string, reference RemotePartReference, verify RemotePartAttachmentVerifier) (RemotePartAttachment, error) {
	if catalog == nil {
		return RemotePartAttachment{}, ErrRemotePartAttachmentNil
	}
	if err := validateRemotePartAttachmentContext(ctx); err != nil {
		return RemotePartAttachment{}, err
	}
	normalizedKey, err := normalizeRemotePartAttachmentKey(key)
	if err != nil {
		return RemotePartAttachment{}, err
	}
	normalizedReference, err := normalizeRemotePartAttachmentReference(reference)
	if err != nil {
		return RemotePartAttachment{}, err
	}
	if verify == nil {
		return RemotePartAttachment{}, ErrRemotePartAttachmentVerifierRequired
	}
	if err := verify(ctx, normalizedReference); err != nil {
		return RemotePartAttachment{}, fmt.Errorf("%w: %w", ErrRemotePartAttachmentVerificationFailed, err)
	}
	if err := ctx.Err(); err != nil {
		return RemotePartAttachment{}, err
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if current, ok := catalog.entries[normalizedKey]; ok {
		if current.Quarantined {
			return RemotePartAttachment{}, fmt.Errorf("%w: use AttachReplacement for %q", ErrRemotePartAttachmentQuarantined, normalizedKey)
		}
		return RemotePartAttachment{}, fmt.Errorf("%w: %q", ErrRemotePartAttachmentAlreadyAttached, normalizedKey)
	}
	generation, err := catalog.nextGenerationLocked()
	if err != nil {
		return RemotePartAttachment{}, err
	}
	entry := RemotePartAttachment{Key: normalizedKey, Reference: normalizedReference, Generation: generation}
	catalog.entries[normalizedKey] = entry
	return entry, nil
}

// Detach moves an active part to quarantine without deleting or moving its
// bytes. expectedGeneration may be zero for an unconditional operator action;
// a nonzero value protects a read-modify-write workflow from stale callers.
func (catalog *RemotePartAttachmentCatalog) Detach(ctx context.Context, key string, expectedGeneration uint64, reason string) (RemotePartAttachment, error) {
	if catalog == nil {
		return RemotePartAttachment{}, ErrRemotePartAttachmentNil
	}
	if err := validateRemotePartAttachmentContext(ctx); err != nil {
		return RemotePartAttachment{}, err
	}
	normalizedKey, err := normalizeRemotePartAttachmentKey(key)
	if err != nil {
		return RemotePartAttachment{}, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > MaxRemotePartAttachmentReasonBytes {
		return RemotePartAttachment{}, fmt.Errorf("%w: quarantine reason is required and must be at most %d bytes", ErrRemotePartAttachmentInvalid, MaxRemotePartAttachmentReasonBytes)
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	current, ok := catalog.entries[normalizedKey]
	if !ok {
		return RemotePartAttachment{}, fmt.Errorf("%w: %q", ErrRemotePartAttachmentNotFound, normalizedKey)
	}
	if expectedGeneration != 0 && current.Generation != expectedGeneration {
		return RemotePartAttachment{}, fmt.Errorf("%w: expected %d, current %d", ErrRemotePartAttachmentStale, expectedGeneration, current.Generation)
	}
	if current.Quarantined {
		return current, nil
	}
	generation, err := catalog.nextGenerationLocked()
	if err != nil {
		return RemotePartAttachment{}, err
	}
	current.Generation = generation
	current.Quarantined = true
	current.Reason = reason
	catalog.entries[normalizedKey] = current
	return current, nil
}

// AttachReplacement verifies a replacement before atomically publishing it
// over a quarantined entry. The expected quarantine generation is mandatory so
// a delayed operator cannot revive a part after another repair completed.
func (catalog *RemotePartAttachmentCatalog) AttachReplacement(ctx context.Context, key string, expectedGeneration uint64, replacement RemotePartReference, verify RemotePartAttachmentVerifier) (RemotePartAttachment, error) {
	if catalog == nil {
		return RemotePartAttachment{}, ErrRemotePartAttachmentNil
	}
	if err := validateRemotePartAttachmentContext(ctx); err != nil {
		return RemotePartAttachment{}, err
	}
	if expectedGeneration == 0 {
		return RemotePartAttachment{}, fmt.Errorf("%w: replacement generation is required", ErrRemotePartAttachmentInvalid)
	}
	normalizedKey, err := normalizeRemotePartAttachmentKey(key)
	if err != nil {
		return RemotePartAttachment{}, err
	}
	normalizedReplacement, err := normalizeRemotePartAttachmentReference(replacement)
	if err != nil {
		return RemotePartAttachment{}, err
	}
	if verify == nil {
		return RemotePartAttachment{}, ErrRemotePartAttachmentVerifierRequired
	}
	if err := catalog.checkReplacementState(normalizedKey, expectedGeneration); err != nil {
		return RemotePartAttachment{}, err
	}
	if err := verify(ctx, normalizedReplacement); err != nil {
		return RemotePartAttachment{}, fmt.Errorf("%w: %w", ErrRemotePartAttachmentVerificationFailed, err)
	}
	if err := ctx.Err(); err != nil {
		return RemotePartAttachment{}, err
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	current, ok := catalog.entries[normalizedKey]
	if !ok {
		return RemotePartAttachment{}, fmt.Errorf("%w: %q", ErrRemotePartAttachmentNotFound, normalizedKey)
	}
	if !current.Quarantined {
		return RemotePartAttachment{}, fmt.Errorf("%w: %q", ErrRemotePartAttachmentNotQuarantined, normalizedKey)
	}
	if current.Generation != expectedGeneration {
		return RemotePartAttachment{}, fmt.Errorf("%w: expected %d, current %d", ErrRemotePartAttachmentStale, expectedGeneration, current.Generation)
	}
	generation, err := catalog.nextGenerationLocked()
	if err != nil {
		return RemotePartAttachment{}, err
	}
	current.Reference = normalizedReplacement
	current.Generation = generation
	current.Quarantined = false
	current.Reason = ""
	catalog.entries[normalizedKey] = current
	return current, nil
}

// Lookup returns the current state for one logical part. It returns
// quarantined entries as well as active entries so operators can inspect the
// old reference before attaching a replacement.
func (catalog *RemotePartAttachmentCatalog) Lookup(key string) (RemotePartAttachment, bool) {
	if catalog == nil {
		return RemotePartAttachment{}, false
	}
	normalizedKey, err := normalizeRemotePartAttachmentKey(key)
	if err != nil {
		return RemotePartAttachment{}, false
	}
	catalog.mu.RLock()
	entry, ok := catalog.entries[normalizedKey]
	catalog.mu.RUnlock()
	return entry, ok
}

// Quarantined returns detached snapshots sorted by logical key.
func (catalog *RemotePartAttachmentCatalog) Quarantined() []RemotePartAttachment {
	if catalog == nil {
		return nil
	}
	catalog.mu.RLock()
	entries := make([]RemotePartAttachment, 0, len(catalog.entries))
	for _, entry := range catalog.entries {
		if entry.Quarantined {
			entries = append(entries, entry)
		}
	}
	catalog.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool { return entries[left].Key < entries[right].Key })
	return entries
}

func (catalog *RemotePartAttachmentCatalog) checkReplacementState(key string, expectedGeneration uint64) error {
	catalog.mu.RLock()
	current, ok := catalog.entries[key]
	catalog.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrRemotePartAttachmentNotFound, key)
	}
	if !current.Quarantined {
		return fmt.Errorf("%w: %q", ErrRemotePartAttachmentNotQuarantined, key)
	}
	if current.Generation != expectedGeneration {
		return fmt.Errorf("%w: expected %d, current %d", ErrRemotePartAttachmentStale, expectedGeneration, current.Generation)
	}
	return nil
}

func (catalog *RemotePartAttachmentCatalog) nextGenerationLocked() (uint64, error) {
	if catalog.nextGeneration == ^uint64(0) {
		return 0, ErrRemotePartAttachmentGenerationExhausted
	}
	catalog.nextGeneration++
	return catalog.nextGeneration, nil
}

func validateRemotePartAttachmentContext(ctx context.Context) error {
	if ctx == nil {
		return ErrRemotePartAttachmentContextRequired
	}
	return ctx.Err()
}

func normalizeRemotePartAttachmentKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" || len(key) > MaxRemotePartAttachmentKeyBytes {
		return "", fmt.Errorf("%w: key is required and must be at most %d bytes", ErrRemotePartAttachmentInvalid, MaxRemotePartAttachmentKeyBytes)
	}
	return key, nil
}

func normalizeRemotePartAttachmentReference(reference RemotePartReference) (RemotePartReference, error) {
	normalized, err := NewRemotePartReference(
		reference.ObjectURI(),
		reference.LocalMetadataPath(),
		reference.Checksum(),
		reference.SizeBytes(),
	)
	if err != nil {
		return RemotePartReference{}, fmt.Errorf("%w: reference: %v", ErrRemotePartAttachmentInvalid, err)
	}
	return normalized, nil
}
