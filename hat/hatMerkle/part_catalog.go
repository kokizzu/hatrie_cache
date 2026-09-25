package hatMerkle

import (
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrPartCatalogNil reports a method call on a nil catalog.
	ErrPartCatalogNil = errors.New("hatriecache: part catalog is nil")
	// ErrPartCatalogOptionsInvalid reports an invalid catalog bound.
	ErrPartCatalogOptionsInvalid = errors.New("hatriecache: part catalog options are invalid")
	// ErrPartCatalogNameRequired reports an empty part name.
	ErrPartCatalogNameRequired = errors.New("hatriecache: part catalog name is required")
	// ErrPartCatalogVerifierRequired reports an attach without verification.
	ErrPartCatalogVerifierRequired = errors.New("hatriecache: part catalog verifier is required")
	// ErrPartCatalogFileRequired reports an attach with a nil file.
	ErrPartCatalogFileRequired = errors.New("hatriecache: part catalog file is required")
	// ErrPartCatalogAlreadyAttached reports a name that is already active.
	ErrPartCatalogAlreadyAttached = errors.New("hatriecache: part is already attached")
	// ErrPartCatalogNotFound reports a missing active or quarantined part.
	ErrPartCatalogNotFound = errors.New("hatriecache: part was not found")
	// ErrPartCatalogCapacity reports that the bounded catalog is full.
	ErrPartCatalogCapacity = errors.New("hatriecache: part catalog capacity reached")
	// ErrPartCatalogQuarantineConflict reports an attempt to quarantine a name
	// that already has an older detached entry.
	ErrPartCatalogQuarantineConflict = errors.New("hatriecache: part quarantine already contains this name")
)

// DefaultPartCatalogMaxEntries bounds active and quarantined metadata when a
// caller does not provide an explicit limit.
const DefaultPartCatalogMaxEntries = 1024

// PartCatalogOptions bounds the in-memory lifecycle metadata. The catalog
// never stores part bytes; callers own the immutable storage identified by
// PartCatalogEntry.Location.
type PartCatalogOptions struct {
	MaxEntries int
}

// PartCatalogEntry identifies one immutable part and its integrity metadata.
// Generation changes whenever the entry changes lifecycle state.
type PartCatalogEntry struct {
	Name       string
	Location   string
	Manifest   PartManifest
	Generation uint64
}

// PartCatalogVerifier validates the bytes at entry.Location before the entry
// becomes visible. A verifier may use PartManifest.Validate for memory-backed
// data or VerifyImmutablePartFile for a file-backed part.
type PartCatalogVerifier func(PartCatalogEntry) error

// PartCatalog tracks attached parts and detached quarantine entries. It is a
// transport-neutral operator primitive: detach removes a part from the active
// set atomically, while attach verifies a replacement before publishing it.
// Part bytes are never copied or retained by the catalog.
type PartCatalog struct {
	mu          sync.RWMutex
	maxEntries  int
	active      map[string]PartCatalogEntry
	quarantined map[string]PartCatalogEntry
	generation  uint64
}

// NewPartCatalog creates a bounded part lifecycle catalog. Zero selects
// DefaultPartCatalogMaxEntries.
func NewPartCatalog(options PartCatalogOptions) (*PartCatalog, error) {
	if options.MaxEntries < 0 {
		return nil, ErrPartCatalogOptionsInvalid
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultPartCatalogMaxEntries
	}
	return &PartCatalog{
		maxEntries:  options.MaxEntries,
		active:      make(map[string]PartCatalogEntry),
		quarantined: make(map[string]PartCatalogEntry),
	}, nil
}

// Attach verifies entry and then publishes it as active. Existing active
// entries are never replaced implicitly; callers must Detach first.
func (catalog *PartCatalog) Attach(entry PartCatalogEntry, verify PartCatalogVerifier) error {
	if catalog == nil {
		return ErrPartCatalogNil
	}
	if verify == nil {
		return ErrPartCatalogVerifierRequired
	}
	entry, err := normalizePartCatalogEntry(entry)
	if err != nil {
		return err
	}
	entry = clonePartCatalogEntry(entry)
	if err := verify(entry); err != nil {
		return err
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, exists := catalog.active[entry.Name]; exists {
		return ErrPartCatalogAlreadyAttached
	}
	if len(catalog.active)+len(catalog.quarantined) >= catalog.maxEntries {
		return ErrPartCatalogCapacity
	}
	catalog.generation++
	entry.Generation = catalog.generation
	catalog.active[entry.Name] = clonePartCatalogEntry(entry)
	return nil
}

// AttachFile verifies the remaining bytes in file against entry.Manifest's
// whole-part checksum and publishes the entry without retaining the payload.
// Verification uses the file's current offset and preserves that offset.
func (catalog *PartCatalog) AttachFile(entry PartCatalogEntry, file *os.File) error {
	if file == nil {
		return ErrPartCatalogFileRequired
	}
	return catalog.Attach(entry, func(candidate PartCatalogEntry) error {
		_, err := VerifyImmutablePartFile(file, candidate.Manifest.Checksum)
		return err
	})
}

// Detach atomically moves an active part into quarantine and returns its
// descriptor. It does not delete or modify the underlying bytes.
func (catalog *PartCatalog) Detach(name string) (PartCatalogEntry, error) {
	if catalog == nil {
		return PartCatalogEntry{}, ErrPartCatalogNil
	}
	name, err := normalizePartCatalogName(name)
	if err != nil {
		return PartCatalogEntry{}, err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	entry, exists := catalog.active[name]
	if !exists {
		return PartCatalogEntry{}, ErrPartCatalogNotFound
	}
	if _, exists := catalog.quarantined[name]; exists {
		return PartCatalogEntry{}, ErrPartCatalogQuarantineConflict
	}
	delete(catalog.active, name)
	catalog.generation++
	entry.Generation = catalog.generation
	entry = clonePartCatalogEntry(entry)
	catalog.quarantined[name] = entry
	return clonePartCatalogEntry(entry), nil
}

// Restore verifies a quarantined part and atomically publishes it as active.
// A failed verification leaves quarantine unchanged.
func (catalog *PartCatalog) Restore(name string, verify PartCatalogVerifier) error {
	if catalog == nil {
		return ErrPartCatalogNil
	}
	if verify == nil {
		return ErrPartCatalogVerifierRequired
	}
	name, err := normalizePartCatalogName(name)
	if err != nil {
		return err
	}

	catalog.mu.RLock()
	entry, exists := catalog.quarantined[name]
	active := false
	if exists {
		_, active = catalog.active[name]
	}
	catalog.mu.RUnlock()
	if !exists {
		return ErrPartCatalogNotFound
	}
	if active {
		return ErrPartCatalogAlreadyAttached
	}
	entry = clonePartCatalogEntry(entry)
	if err := verify(entry); err != nil {
		return err
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, active := catalog.active[name]; active {
		return ErrPartCatalogAlreadyAttached
	}
	entry, exists = catalog.quarantined[name]
	if !exists {
		return ErrPartCatalogNotFound
	}
	delete(catalog.quarantined, name)
	catalog.generation++
	entry.Generation = catalog.generation
	catalog.active[name] = clonePartCatalogEntry(entry)
	return nil
}

// Get returns a copy of one active entry.
func (catalog *PartCatalog) Get(name string) (PartCatalogEntry, bool) {
	if catalog == nil {
		return PartCatalogEntry{}, false
	}
	name, err := normalizePartCatalogName(name)
	if err != nil {
		return PartCatalogEntry{}, false
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, ok := catalog.active[name]
	return clonePartCatalogEntry(entry), ok
}

// GetQuarantined returns a copy of one detached entry.
func (catalog *PartCatalog) GetQuarantined(name string) (PartCatalogEntry, bool) {
	if catalog == nil {
		return PartCatalogEntry{}, false
	}
	name, err := normalizePartCatalogName(name)
	if err != nil {
		return PartCatalogEntry{}, false
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, ok := catalog.quarantined[name]
	return clonePartCatalogEntry(entry), ok
}

// RemoveQuarantined permanently removes one detached descriptor and reports
// whether it existed. It does not touch the underlying bytes.
func (catalog *PartCatalog) RemoveQuarantined(name string) bool {
	if catalog == nil {
		return false
	}
	name, err := normalizePartCatalogName(name)
	if err != nil {
		return false
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, exists := catalog.quarantined[name]; !exists {
		return false
	}
	delete(catalog.quarantined, name)
	catalog.generation++
	return true
}

// Snapshot returns deterministic copies of active and quarantined entries.
func (catalog *PartCatalog) Snapshot() (active, quarantined []PartCatalogEntry) {
	if catalog == nil {
		return nil, nil
	}
	catalog.mu.RLock()
	active = make([]PartCatalogEntry, 0, len(catalog.active))
	for _, entry := range catalog.active {
		active = append(active, clonePartCatalogEntry(entry))
	}
	quarantined = make([]PartCatalogEntry, 0, len(catalog.quarantined))
	for _, entry := range catalog.quarantined {
		quarantined = append(quarantined, clonePartCatalogEntry(entry))
	}
	catalog.mu.RUnlock()
	sortPartCatalogEntries(active)
	sortPartCatalogEntries(quarantined)
	return active, quarantined
}

// Len returns the number of active parts.
func (catalog *PartCatalog) Len() int {
	if catalog == nil {
		return 0
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	return len(catalog.active)
}

// QuarantineLen returns the number of detached descriptors retained for
// rollback or operator inspection.
func (catalog *PartCatalog) QuarantineLen() int {
	if catalog == nil {
		return 0
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	return len(catalog.quarantined)
}

// Generation returns the latest catalog mutation number.
func (catalog *PartCatalog) Generation() uint64 {
	if catalog == nil {
		return 0
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	return catalog.generation
}

func normalizePartCatalogEntry(entry PartCatalogEntry) (PartCatalogEntry, error) {
	name, err := normalizePartCatalogName(entry.Name)
	if err != nil {
		return PartCatalogEntry{}, err
	}
	if entry.Manifest.DeleteBitmap != nil {
		if err := entry.Manifest.DeleteBitmap.Validate(); err != nil {
			return PartCatalogEntry{}, err
		}
	}
	entry.Name = name
	return entry, nil
}

func normalizePartCatalogName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrPartCatalogNameRequired
	}
	return name, nil
}

func clonePartCatalogEntry(entry PartCatalogEntry) PartCatalogEntry {
	if entry.Manifest.Columns != nil {
		entry.Manifest.Columns = append([]PartColumnChecksum(nil), entry.Manifest.Columns...)
	}
	if entry.Manifest.DeleteBitmap != nil {
		bitmap := *entry.Manifest.DeleteBitmap
		entry.Manifest.DeleteBitmap = &bitmap
	}
	return entry
}

func sortPartCatalogEntries(entries []PartCatalogEntry) {
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Name != entries[right].Name {
			return entries[left].Name < entries[right].Name
		}
		return entries[left].Generation < entries[right].Generation
	})
}
