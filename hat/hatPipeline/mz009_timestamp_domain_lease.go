package hatPipeline

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultTimestampDomainLeaseMaxDomains bounds the default number of
	// independent source domains retained by one registry.
	DefaultTimestampDomainLeaseMaxDomains = 64
	// DefaultTimestampDomainLeaseMaxTTL prevents forgotten leases from holding
	// a domain indefinitely when no explicit maximum is supplied.
	DefaultTimestampDomainLeaseMaxTTL = 24 * time.Hour
	MaxTimestampDomainLeaseMaxDomains = 100_000
	MaxTimestampDomainLeaseMaxTTL     = 365 * 24 * time.Hour
	MaxTimestampDomainLeaseTextBytes  = 256
)

var (
	ErrTimestampDomainLeaseNil         = errors.New("timestamp domain lease registry is nil")
	ErrTimestampDomainLeaseOptions     = errors.New("timestamp domain lease options are invalid")
	ErrTimestampDomainLeaseDomainEmpty = errors.New("timestamp domain lease domain is empty")
	ErrTimestampDomainLeaseOwnerEmpty  = errors.New("timestamp domain lease owner is empty")
	ErrTimestampDomainLeaseTextLimit   = errors.New("timestamp domain lease text exceeds the limit")
	ErrTimestampDomainLeaseTTLInvalid  = errors.New("timestamp domain lease TTL is invalid")
	ErrTimestampDomainLeaseCapacity    = errors.New("timestamp domain lease domain capacity exceeded")
	ErrTimestampDomainLeaseActive      = errors.New("timestamp domain already has an active lease")
	ErrTimestampDomainLeaseNotFound    = errors.New("timestamp domain lease domain was not found")
	ErrTimestampDomainLeaseStale       = errors.New("timestamp domain lease is stale")
	ErrTimestampDomainLeaseExpired     = errors.New("timestamp domain lease has expired")
	ErrTimestampDomainLeaseRegression  = errors.New("timestamp domain timestamp regressed")
	ErrTimestampDomainLeaseExhausted   = errors.New("timestamp domain timestamp space is exhausted")
)

// TimestampDomainLeaseOptions bounds one lease registry. Clock is injectable
// for deterministic tests and should return UTC-compatible time values.
type TimestampDomainLeaseOptions struct {
	MaxDomains int
	MaxTTL     time.Duration
	Clock      func() time.Time
}

// TimestampDomainLease is a fencing token for one source timestamp domain.
// Epoch changes whenever ownership is reacquired after release or expiry.
type TimestampDomainLease struct {
	Domain    string
	Owner     string
	Epoch     uint64
	ExpiresAt time.Time
}

// TimestampDomainSnapshot is a detached view of one domain's lease and
// monotonic timestamp watermark.
type TimestampDomainSnapshot struct {
	Domain        string
	Lease         TimestampDomainLease
	Active        bool
	LastTimestamp uint64
	HasTimestamp  bool
}

type timestampDomainLeaseEntry struct {
	lease         TimestampDomainLease
	active        bool
	lastTimestamp uint64
	hasTimestamp  bool
}

// TimestampDomainLeaseRegistry fences independent source clocks and preserves
// one monotonic logical timestamp sequence per domain. It is opt-in and does
// not alter existing source or query behavior.
type TimestampDomainLeaseRegistry struct {
	mu         sync.Mutex
	maxDomains int
	maxTTL     time.Duration
	clock      func() time.Time
	nextEpoch  uint64
	domains    map[string]*timestampDomainLeaseEntry
}

// NewTimestampDomainLeaseRegistry creates a bounded timestamp-domain lease
// registry.
func NewTimestampDomainLeaseRegistry(options TimestampDomainLeaseOptions) (*TimestampDomainLeaseRegistry, error) {
	if options.MaxDomains < 0 || options.MaxTTL < 0 {
		return nil, ErrTimestampDomainLeaseOptions
	}
	if options.MaxDomains == 0 {
		options.MaxDomains = DefaultTimestampDomainLeaseMaxDomains
	}
	if options.MaxTTL == 0 {
		options.MaxTTL = DefaultTimestampDomainLeaseMaxTTL
	}
	if options.MaxDomains > MaxTimestampDomainLeaseMaxDomains || options.MaxTTL > MaxTimestampDomainLeaseMaxTTL {
		return nil, ErrTimestampDomainLeaseOptions
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &TimestampDomainLeaseRegistry{
		maxDomains: options.MaxDomains,
		maxTTL:     options.MaxTTL,
		clock:      options.Clock,
		domains:    make(map[string]*timestampDomainLeaseEntry, options.MaxDomains),
	}, nil
}

// Acquire obtains ownership of a domain until the supplied TTL expires.
// Expired ownership can be replaced, while the domain's watermark is retained.
func (registry *TimestampDomainLeaseRegistry) Acquire(domain, owner string, ttl time.Duration) (TimestampDomainLease, error) {
	if registry == nil {
		return TimestampDomainLease{}, ErrTimestampDomainLeaseNil
	}
	domain, err := normalizeTimestampDomainLeaseText(domain, ErrTimestampDomainLeaseDomainEmpty)
	if err != nil {
		return TimestampDomainLease{}, err
	}
	owner, err = normalizeTimestampDomainLeaseText(owner, ErrTimestampDomainLeaseOwnerEmpty)
	if err != nil {
		return TimestampDomainLease{}, err
	}
	if err := registry.validateTTL(ttl); err != nil {
		return TimestampDomainLease{}, err
	}
	now := registry.now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.domains[domain]
	if exists && entry.active {
		if now.Before(entry.lease.ExpiresAt) {
			return TimestampDomainLease{}, ErrTimestampDomainLeaseActive
		}
		entry.active = false
		entry.lease = TimestampDomainLease{}
	}
	if !exists {
		if len(registry.domains) >= registry.maxDomains {
			return TimestampDomainLease{}, ErrTimestampDomainLeaseCapacity
		}
		entry = &timestampDomainLeaseEntry{}
		registry.domains[domain] = entry
	}
	if registry.nextEpoch == ^uint64(0) {
		return TimestampDomainLease{}, ErrTimestampDomainLeaseExhausted
	}
	registry.nextEpoch++
	lease := TimestampDomainLease{
		Domain:    domain,
		Owner:     owner,
		Epoch:     registry.nextEpoch,
		ExpiresAt: now.Add(ttl),
	}
	entry.lease = lease
	entry.active = true
	return lease, nil
}

// Renew extends an active lease without changing its fencing epoch.
func (registry *TimestampDomainLeaseRegistry) Renew(lease TimestampDomainLease, ttl time.Duration) (TimestampDomainLease, error) {
	if registry == nil {
		return TimestampDomainLease{}, ErrTimestampDomainLeaseNil
	}
	if err := registry.validateTTL(ttl); err != nil {
		return TimestampDomainLease{}, err
	}
	now := registry.now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, err := registry.validateLeaseLocked(lease, now)
	if err != nil {
		return TimestampDomainLease{}, err
	}
	entry.lease.ExpiresAt = now.Add(ttl)
	return entry.lease, nil
}

// Release relinquishes a lease while retaining its timestamp watermark.
func (registry *TimestampDomainLeaseRegistry) Release(lease TimestampDomainLease) error {
	if registry == nil {
		return ErrTimestampDomainLeaseNil
	}
	now := registry.now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, err := registry.validateLeaseLocked(lease, now)
	if err != nil {
		return err
	}
	entry.active = false
	entry.lease = TimestampDomainLease{}
	return nil
}

// Next returns the next monotonic timestamp for a valid lease.
func (registry *TimestampDomainLeaseRegistry) Next(lease TimestampDomainLease) (uint64, error) {
	if registry == nil {
		return 0, ErrTimestampDomainLeaseNil
	}
	now := registry.now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, err := registry.validateLeaseLocked(lease, now)
	if err != nil {
		return 0, err
	}
	if entry.hasTimestamp && entry.lastTimestamp == ^uint64(0) {
		return 0, ErrTimestampDomainLeaseExhausted
	}
	entry.lastTimestamp++
	entry.hasTimestamp = true
	return entry.lastTimestamp, nil
}

// Observe advances a domain watermark to an externally supplied timestamp.
// Equal observations are idempotent; regressions are rejected.
func (registry *TimestampDomainLeaseRegistry) Observe(lease TimestampDomainLease, timestamp uint64) error {
	if registry == nil {
		return ErrTimestampDomainLeaseNil
	}
	now := registry.now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, err := registry.validateLeaseLocked(lease, now)
	if err != nil {
		return err
	}
	if entry.hasTimestamp && timestamp < entry.lastTimestamp {
		return ErrTimestampDomainLeaseRegression
	}
	entry.lastTimestamp = timestamp
	entry.hasTimestamp = true
	return nil
}

// Forget removes an inactive domain and releases its bounded registry slot.
func (registry *TimestampDomainLeaseRegistry) Forget(domain string) error {
	if registry == nil {
		return ErrTimestampDomainLeaseNil
	}
	domain, err := normalizeTimestampDomainLeaseText(domain, ErrTimestampDomainLeaseDomainEmpty)
	if err != nil {
		return err
	}
	now := registry.now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, ok := registry.domains[domain]
	if !ok {
		return ErrTimestampDomainLeaseNotFound
	}
	if entry.active {
		if now.Before(entry.lease.ExpiresAt) {
			return ErrTimestampDomainLeaseActive
		}
		entry.active = false
		entry.lease = TimestampDomainLease{}
	}
	delete(registry.domains, domain)
	return nil
}

// Snapshot returns all domains sorted by domain name. Expired leases are
// retired as part of the snapshot, while their watermarks remain visible.
func (registry *TimestampDomainLeaseRegistry) Snapshot() []TimestampDomainSnapshot {
	if registry == nil {
		return nil
	}
	now := registry.now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	domains := make([]string, 0, len(registry.domains))
	for domain, entry := range registry.domains {
		if entry.active && !now.Before(entry.lease.ExpiresAt) {
			entry.active = false
			entry.lease = TimestampDomainLease{}
		}
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	snapshot := make([]TimestampDomainSnapshot, 0, len(domains))
	for _, domain := range domains {
		entry := registry.domains[domain]
		snapshot = append(snapshot, TimestampDomainSnapshot{
			Domain:        domain,
			Lease:         entry.lease,
			Active:        entry.active,
			LastTimestamp: entry.lastTimestamp,
			HasTimestamp:  entry.hasTimestamp,
		})
	}
	return snapshot
}

func (registry *TimestampDomainLeaseRegistry) validateTTL(ttl time.Duration) error {
	if ttl <= 0 || ttl > registry.maxTTL {
		return ErrTimestampDomainLeaseTTLInvalid
	}
	return nil
}

func (registry *TimestampDomainLeaseRegistry) now() time.Time {
	return registry.clock().UTC()
}

func (registry *TimestampDomainLeaseRegistry) validateLeaseLocked(lease TimestampDomainLease, now time.Time) (*timestampDomainLeaseEntry, error) {
	entry, ok := registry.domains[lease.Domain]
	if !ok {
		return nil, ErrTimestampDomainLeaseNotFound
	}
	if !entry.active || entry.lease.Epoch != lease.Epoch || entry.lease.Owner != lease.Owner {
		return nil, ErrTimestampDomainLeaseStale
	}
	if !now.Before(entry.lease.ExpiresAt) {
		entry.active = false
		entry.lease = TimestampDomainLease{}
		return nil, ErrTimestampDomainLeaseExpired
	}
	return entry, nil
}

func normalizeTimestampDomainLeaseText(value string, empty error) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", empty
	}
	if len(value) > MaxTimestampDomainLeaseTextBytes {
		return "", ErrTimestampDomainLeaseTextLimit
	}
	return value, nil
}
