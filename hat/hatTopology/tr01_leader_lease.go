package hatTopology

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultLeaderLeaseTTL is used when Acquire or Renew receives a zero TTL.
	DefaultLeaderLeaseTTL = 15 * time.Second
	// DefaultLeaderLeaseMaxTTL bounds one lease so a forgotten holder cannot
	// retain leadership indefinitely.
	DefaultLeaderLeaseMaxTTL = time.Minute
)

var (
	ErrLeaderLeaseName     = errors.New("hatriecache: leader lease name is required")
	ErrLeaderLeaseHolder   = errors.New("hatriecache: leader lease holder is required")
	ErrLeaderLeaseTTL      = errors.New("hatriecache: leader lease TTL is invalid")
	ErrLeaderLeaseHeld     = errors.New("hatriecache: leader lease is held by another node")
	ErrLeaderLeaseExpired  = errors.New("hatriecache: leader lease has expired")
	ErrLeaderLeaseFenced   = errors.New("hatriecache: leader lease token or holder is fenced")
	ErrLeaderLeaseNotFound = errors.New("hatriecache: leader lease was not found")
	ErrLeaderLeaseToken    = errors.New("hatriecache: leader lease token overflow")
)

// LeaderLeaseOptions configures an in-memory lease authority. The authority
// is deliberately transport-neutral: callers must serialize it through a
// single authority, quorum, or consensus system before treating it as a
// distributed split-brain solution.
type LeaderLeaseOptions struct {
	Now    func() time.Time
	MaxTTL time.Duration
}

// LeaderLease is a fenced, time-bounded leadership grant.
type LeaderLease struct {
	Name      string    `json:"name"`
	Holder    string    `json:"holder"`
	Token     uint64    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// LeaderLeaseStore serializes lease acquisition, renewal, release, and
// validation. It has no background goroutine and does not persist state.
type LeaderLeaseStore struct {
	mu        sync.Mutex
	now       func() time.Time
	maxTTL    time.Duration
	nextToken uint64
	leases    map[string]LeaderLease
}

// NewLeaderLeaseStore creates an empty lease authority.
func NewLeaderLeaseStore(options LeaderLeaseOptions) (*LeaderLeaseStore, error) {
	if options.MaxTTL < 0 {
		return nil, ErrLeaderLeaseTTL
	}
	maxTTL := options.MaxTTL
	if maxTTL == 0 {
		maxTTL = DefaultLeaderLeaseMaxTTL
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &LeaderLeaseStore{
		now:    now,
		maxTTL: maxTTL,
		leases: make(map[string]LeaderLease),
	}, nil
}

// Acquire grants name to holder when no unexpired lease exists. Repeating an
// acquire by the current holder is idempotent and returns the existing token;
// use Renew to extend its expiry.
func (store *LeaderLeaseStore) Acquire(name, holder string, ttl time.Duration) (LeaderLease, error) {
	if store == nil {
		return LeaderLease{}, ErrLeaderLeaseNotFound
	}
	name, holder, ttl, err := store.normalizeRequest(name, holder, ttl)
	if err != nil {
		return LeaderLease{}, err
	}
	now := store.now()
	store.mu.Lock()
	defer store.mu.Unlock()
	if current, ok := store.leases[name]; ok && now.Before(current.ExpiresAt) {
		if current.Holder == holder {
			return current, nil
		}
		return LeaderLease{}, fmt.Errorf("%s: %w", current.Holder, ErrLeaderLeaseHeld)
	}
	token, err := store.nextTokenLocked()
	if err != nil {
		return LeaderLease{}, err
	}
	lease := LeaderLease{Name: name, Holder: holder, Token: token, ExpiresAt: now.Add(ttl)}
	store.leases[name] = lease
	return lease, nil
}

// Renew extends an unexpired lease only when holder and token still match.
func (store *LeaderLeaseStore) Renew(name, holder string, token uint64, ttl time.Duration) (LeaderLease, error) {
	if store == nil {
		return LeaderLease{}, ErrLeaderLeaseNotFound
	}
	name, holder, ttl, err := store.normalizeRequest(name, holder, ttl)
	if err != nil {
		return LeaderLease{}, err
	}
	now := store.now()
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, ok := store.leases[name]
	if !ok {
		return LeaderLease{}, ErrLeaderLeaseNotFound
	}
	if !now.Before(lease.ExpiresAt) {
		delete(store.leases, name)
		return LeaderLease{}, ErrLeaderLeaseExpired
	}
	if lease.Holder != holder || lease.Token != token {
		return LeaderLease{}, ErrLeaderLeaseFenced
	}
	lease.ExpiresAt = now.Add(ttl)
	store.leases[name] = lease
	return lease, nil
}

// Release removes a lease only when holder and token still match.
func (store *LeaderLeaseStore) Release(name, holder string, token uint64) error {
	if store == nil {
		return ErrLeaderLeaseNotFound
	}
	name, holder, err := normalizeLeaderLeaseIdentity(name, holder)
	if err != nil {
		return err
	}
	now := store.now()
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, ok := store.leases[name]
	if !ok {
		return ErrLeaderLeaseNotFound
	}
	if !now.Before(lease.ExpiresAt) {
		delete(store.leases, name)
		return ErrLeaderLeaseExpired
	}
	if lease.Holder != holder || lease.Token != token {
		return ErrLeaderLeaseFenced
	}
	delete(store.leases, name)
	return nil
}

// Validate checks that holder still owns the current, unexpired fence token.
func (store *LeaderLeaseStore) Validate(name, holder string, token uint64) error {
	if store == nil {
		return ErrLeaderLeaseNotFound
	}
	name, holder, err := normalizeLeaderLeaseIdentity(name, holder)
	if err != nil {
		return err
	}
	now := store.now()
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, ok := store.leases[name]
	if !ok {
		return ErrLeaderLeaseNotFound
	}
	if !now.Before(lease.ExpiresAt) {
		delete(store.leases, name)
		return ErrLeaderLeaseExpired
	}
	if lease.Holder != holder || lease.Token != token {
		return ErrLeaderLeaseFenced
	}
	return nil
}

// Current returns an independent copy of the current lease when present. An
// expired lease is treated as absent and removed.
func (store *LeaderLeaseStore) Current(name string) (LeaderLease, bool) {
	if store == nil {
		return LeaderLease{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return LeaderLease{}, false
	}
	now := store.now()
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, ok := store.leases[name]
	if !ok || !now.Before(lease.ExpiresAt) {
		if ok {
			delete(store.leases, name)
		}
		return LeaderLease{}, false
	}
	return lease, true
}

func (store *LeaderLeaseStore) normalizeRequest(name, holder string, ttl time.Duration) (string, string, time.Duration, error) {
	name, holder, err := normalizeLeaderLeaseIdentity(name, holder)
	if err != nil {
		return "", "", 0, err
	}
	if ttl == 0 {
		ttl = DefaultLeaderLeaseTTL
		if ttl > store.maxTTL {
			ttl = store.maxTTL
		}
	}
	if ttl < 0 || ttl > store.maxTTL {
		return "", "", 0, ErrLeaderLeaseTTL
	}
	return name, holder, ttl, nil
}

func (store *LeaderLeaseStore) nextTokenLocked() (uint64, error) {
	if store.nextToken == math.MaxUint64 {
		return 0, ErrLeaderLeaseToken
	}
	store.nextToken++
	return store.nextToken, nil
}

func normalizeLeaderLeaseIdentity(name, holder string) (string, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "", ErrLeaderLeaseName
	}
	holder = strings.TrimSpace(holder)
	if holder == "" {
		return "", "", ErrLeaderLeaseHolder
	}
	return name, holder, nil
}
