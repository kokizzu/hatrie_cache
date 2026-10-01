package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"
)

const (
	// DefaultSnapshotWALBootstrapMaxSessions keeps the coordinator bounded when
	// a caller feeds it untrusted or disconnected join attempts.
	DefaultSnapshotWALBootstrapMaxSessions = 64
	// MaxSnapshotWALBootstrapSessions is an upper bound for one coordinator.
	MaxSnapshotWALBootstrapSessions   = 4096
	snapshotWALBootstrapMaxLabelSize  = 256
	snapshotWALBootstrapMaxReasonSize = 512
)

var (
	ErrSnapshotWALBootstrapInvalidOptions       = errors.New("hatrie_cache: invalid snapshot/WAL bootstrap options")
	ErrSnapshotWALBootstrapInvalidRequest       = errors.New("hatrie_cache: invalid snapshot/WAL bootstrap request")
	ErrSnapshotWALBootstrapLimitReached         = errors.New("hatrie_cache: snapshot/WAL bootstrap session limit reached")
	ErrSnapshotWALBootstrapAlreadyExists        = errors.New("hatrie_cache: snapshot/WAL bootstrap session already exists")
	ErrSnapshotWALBootstrapNotFound             = errors.New("hatrie_cache: snapshot/WAL bootstrap session not found")
	ErrSnapshotWALBootstrapInvalidTransition    = errors.New("hatrie_cache: invalid snapshot/WAL bootstrap transition")
	ErrSnapshotWALBootstrapFenceMismatch        = errors.New("hatrie_cache: snapshot/WAL bootstrap fencing token mismatch")
	ErrSnapshotWALBootstrapSequenceRegression   = errors.New("hatrie_cache: snapshot/WAL bootstrap sequence regressed")
	ErrSnapshotWALBootstrapSequenceAhead        = errors.New("hatrie_cache: snapshot/WAL bootstrap applied sequence is ahead of source")
	ErrSnapshotWALBootstrapSnapshotMismatch     = errors.New("hatrie_cache: snapshot/WAL bootstrap snapshot mismatch")
	ErrSnapshotWALBootstrapSnapshotNotInstalled = errors.New("hatrie_cache: snapshot/WAL bootstrap snapshot is not installed")
	ErrSnapshotWALBootstrapSourceNotFenced      = errors.New("hatrie_cache: snapshot/WAL bootstrap source is not fenced")
	ErrSnapshotWALBootstrapNotCaughtUp          = errors.New("hatrie_cache: snapshot/WAL bootstrap is not caught up")
)

// SnapshotWALBootstrapPhase is the locally observed phase of one join.
//
// The coordinator is transport-neutral. It does not copy snapshot bytes or
// replay WAL records; it makes the caller's transfer and activation steps
// explicit and rejects stale or unsafe transitions.
type SnapshotWALBootstrapPhase uint8

const (
	SnapshotWALBootstrapPhasePending SnapshotWALBootstrapPhase = iota + 1
	SnapshotWALBootstrapPhaseSnapshotInstalled
	SnapshotWALBootstrapPhaseCatchingUp
	SnapshotWALBootstrapPhaseSourceFenced
	SnapshotWALBootstrapPhaseActivated
	SnapshotWALBootstrapPhaseAborted
)

func (p SnapshotWALBootstrapPhase) String() string {
	switch p {
	case SnapshotWALBootstrapPhasePending:
		return "pending"
	case SnapshotWALBootstrapPhaseSnapshotInstalled:
		return "snapshot_installed"
	case SnapshotWALBootstrapPhaseCatchingUp:
		return "catching_up"
	case SnapshotWALBootstrapPhaseSourceFenced:
		return "source_fenced"
	case SnapshotWALBootstrapPhaseActivated:
		return "activated"
	case SnapshotWALBootstrapPhaseAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// SnapshotWALBootstrapOptions bounds one in-memory coordinator.
type SnapshotWALBootstrapOptions struct {
	// MaxSessions is the maximum number of unfinished or completed sessions
	// retained by the coordinator. Zero selects the bounded default.
	MaxSessions int
}

// SnapshotWALBootstrapRequest identifies a snapshot and the source fence that
// must remain valid until activation.
type SnapshotWALBootstrapRequest struct {
	ID               string
	Node             string
	Source           string
	FencingToken     uint64
	SnapshotID       string
	SnapshotSequence uint64
}

// SnapshotWALBootstrapStatus is a detached snapshot of one join session.
type SnapshotWALBootstrapStatus struct {
	ID               string
	Node             string
	Source           string
	FencingToken     uint64
	SnapshotID       string
	SnapshotSequence uint64
	SourceSequence   uint64
	AppliedSequence  uint64
	Phase            SnapshotWALBootstrapPhase
	AbortReason      string
}

// SnapshotWALBootstrapActivation is the immutable activation decision returned
// by Activate. Repeating the same activation is idempotent.
type SnapshotWALBootstrapActivation struct {
	ID           string
	Node         string
	Source       string
	FencingToken uint64
	Sequence     uint64
}

type snapshotWALBootstrapSession struct {
	status     SnapshotWALBootstrapStatus
	activation SnapshotWALBootstrapActivation
}

// SnapshotWALBootstrapCoordinator is a bounded, transport-neutral join
// state machine. It is caller-owned and does not automatically join a cluster.
type SnapshotWALBootstrapCoordinator struct {
	mu          sync.RWMutex
	maxSessions int
	sessions    map[string]*snapshotWALBootstrapSession
}

// NewSnapshotWALBootstrapCoordinator creates a bounded join coordinator.
func NewSnapshotWALBootstrapCoordinator(options SnapshotWALBootstrapOptions) (*SnapshotWALBootstrapCoordinator, error) {
	maxSessions := options.MaxSessions
	if maxSessions == 0 {
		maxSessions = DefaultSnapshotWALBootstrapMaxSessions
	}
	if maxSessions < 0 || maxSessions > MaxSnapshotWALBootstrapSessions {
		return nil, fmt.Errorf("%w: max sessions %d", ErrSnapshotWALBootstrapInvalidOptions, maxSessions)
	}
	return &SnapshotWALBootstrapCoordinator{
		maxSessions: maxSessions,
		sessions:    make(map[string]*snapshotWALBootstrapSession, maxSessions),
	}, nil
}

// Begin registers a join attempt at the exact snapshot sequence supplied by
// the caller. The source sequence starts at that snapshot boundary.
func (c *SnapshotWALBootstrapCoordinator) Begin(request SnapshotWALBootstrapRequest) (SnapshotWALBootstrapStatus, error) {
	if c == nil {
		return SnapshotWALBootstrapStatus{}, ErrSnapshotWALBootstrapInvalidOptions
	}
	var err error
	if request.ID, err = normalizeSnapshotWALBootstrapLabel(request.ID, "id"); err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}
	if request.Node, err = normalizeSnapshotWALBootstrapLabel(request.Node, "node"); err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}
	if request.Source, err = normalizeSnapshotWALBootstrapLabel(request.Source, "source"); err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}
	if request.SnapshotID, err = normalizeSnapshotWALBootstrapLabel(request.SnapshotID, "snapshot id"); err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}
	if err := validateSnapshotWALBootstrapRequest(request); err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sessions) >= c.maxSessions {
		return SnapshotWALBootstrapStatus{}, ErrSnapshotWALBootstrapLimitReached
	}
	if _, exists := c.sessions[request.ID]; exists {
		return SnapshotWALBootstrapStatus{}, fmt.Errorf("%w: %q", ErrSnapshotWALBootstrapAlreadyExists, request.ID)
	}
	status := SnapshotWALBootstrapStatus{
		ID:               request.ID,
		Node:             request.Node,
		Source:           request.Source,
		FencingToken:     request.FencingToken,
		SnapshotID:       request.SnapshotID,
		SnapshotSequence: request.SnapshotSequence,
		SourceSequence:   request.SnapshotSequence,
		Phase:            SnapshotWALBootstrapPhasePending,
	}
	c.sessions[request.ID] = &snapshotWALBootstrapSession{status: status}
	return status, nil
}

// MarkSnapshotInstalled acknowledges that the requested snapshot is installed
// and makes its sequence the applied WAL boundary.
func (c *SnapshotWALBootstrapCoordinator) MarkSnapshotInstalled(id, snapshotID string, sequence uint64) error {
	id, err := normalizeSnapshotWALBootstrapLabel(id, "id")
	if err != nil {
		return err
	}
	snapshotID, err = normalizeSnapshotWALBootstrapLabel(snapshotID, "snapshot id")
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionLocked(id)
	if err != nil {
		return err
	}
	if session.status.SnapshotID != snapshotID || session.status.SnapshotSequence != sequence {
		return ErrSnapshotWALBootstrapSnapshotMismatch
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseAborted {
		return ErrSnapshotWALBootstrapInvalidTransition
	}
	if session.status.Phase != SnapshotWALBootstrapPhasePending {
		return nil
	}
	session.status.AppliedSequence = sequence
	if session.status.SourceSequence > sequence {
		session.status.Phase = SnapshotWALBootstrapPhaseCatchingUp
	} else {
		session.status.Phase = SnapshotWALBootstrapPhaseSnapshotInstalled
	}
	return nil
}

// AdvanceSource records a monotonic source WAL boundary while the source fence
// is still open. The caller must fence the source before activation.
func (c *SnapshotWALBootstrapCoordinator) AdvanceSource(id string, fencingToken, sequence uint64) error {
	id, err := normalizeSnapshotWALBootstrapLabel(id, "id")
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionLocked(id)
	if err != nil {
		return err
	}
	if err := session.checkFence(fencingToken); err != nil {
		return err
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseAborted || session.status.Phase == SnapshotWALBootstrapPhaseSourceFenced || session.status.Phase == SnapshotWALBootstrapPhaseActivated {
		return ErrSnapshotWALBootstrapInvalidTransition
	}
	if sequence < session.status.SourceSequence {
		return ErrSnapshotWALBootstrapSequenceRegression
	}
	if sequence == session.status.SourceSequence {
		return nil
	}
	session.status.SourceSequence = sequence
	if session.status.Phase == SnapshotWALBootstrapPhaseSnapshotInstalled {
		session.status.Phase = SnapshotWALBootstrapPhaseCatchingUp
	}
	return nil
}

// AdvanceApplied records monotonic WAL replay after the snapshot is installed.
func (c *SnapshotWALBootstrapCoordinator) AdvanceApplied(id string, fencingToken, sequence uint64) error {
	id, err := normalizeSnapshotWALBootstrapLabel(id, "id")
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionLocked(id)
	if err != nil {
		return err
	}
	if err := session.checkFence(fencingToken); err != nil {
		return err
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseAborted {
		return ErrSnapshotWALBootstrapInvalidTransition
	}
	if session.status.Phase == SnapshotWALBootstrapPhasePending {
		return ErrSnapshotWALBootstrapSnapshotNotInstalled
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseActivated {
		if sequence == session.status.AppliedSequence {
			return nil
		}
		return ErrSnapshotWALBootstrapInvalidTransition
	}
	if sequence < session.status.AppliedSequence {
		return ErrSnapshotWALBootstrapSequenceRegression
	}
	if sequence > session.status.SourceSequence {
		return ErrSnapshotWALBootstrapSequenceAhead
	}
	session.status.AppliedSequence = sequence
	if session.status.Phase == SnapshotWALBootstrapPhaseSnapshotInstalled && sequence > session.status.SnapshotSequence {
		session.status.Phase = SnapshotWALBootstrapPhaseCatchingUp
	}
	return nil
}

// FenceSource captures the final source sequence. No source progress can be
// recorded after this transition.
func (c *SnapshotWALBootstrapCoordinator) FenceSource(id string, fencingToken uint64) (SnapshotWALBootstrapStatus, error) {
	id, err := normalizeSnapshotWALBootstrapLabel(id, "id")
	if err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionLocked(id)
	if err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}
	if err := session.checkFence(fencingToken); err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}
	switch session.status.Phase {
	case SnapshotWALBootstrapPhaseSourceFenced, SnapshotWALBootstrapPhaseActivated:
		return session.status, nil
	case SnapshotWALBootstrapPhasePending:
		return SnapshotWALBootstrapStatus{}, ErrSnapshotWALBootstrapSnapshotNotInstalled
	case SnapshotWALBootstrapPhaseSnapshotInstalled, SnapshotWALBootstrapPhaseCatchingUp:
		session.status.Phase = SnapshotWALBootstrapPhaseSourceFenced
		return session.status, nil
	default:
		return SnapshotWALBootstrapStatus{}, ErrSnapshotWALBootstrapInvalidTransition
	}
}

// Activate atomically transitions a fenced and caught-up session. Repeating
// the activation with the same token returns the original decision.
func (c *SnapshotWALBootstrapCoordinator) Activate(id string, fencingToken uint64) (SnapshotWALBootstrapActivation, error) {
	id, err := normalizeSnapshotWALBootstrapLabel(id, "id")
	if err != nil {
		return SnapshotWALBootstrapActivation{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionLocked(id)
	if err != nil {
		return SnapshotWALBootstrapActivation{}, err
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseAborted {
		return SnapshotWALBootstrapActivation{}, ErrSnapshotWALBootstrapInvalidTransition
	}
	if err := session.checkFence(fencingToken); err != nil {
		return SnapshotWALBootstrapActivation{}, err
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseActivated {
		return session.activation, nil
	}
	switch session.status.Phase {
	case SnapshotWALBootstrapPhasePending:
		return SnapshotWALBootstrapActivation{}, ErrSnapshotWALBootstrapSnapshotNotInstalled
	case SnapshotWALBootstrapPhaseSnapshotInstalled, SnapshotWALBootstrapPhaseCatchingUp:
		return SnapshotWALBootstrapActivation{}, ErrSnapshotWALBootstrapSourceNotFenced
	case SnapshotWALBootstrapPhaseSourceFenced:
		if session.status.AppliedSequence < session.status.SourceSequence {
			return SnapshotWALBootstrapActivation{}, ErrSnapshotWALBootstrapNotCaughtUp
		}
		session.status.Phase = SnapshotWALBootstrapPhaseActivated
		session.activation = SnapshotWALBootstrapActivation{
			ID:           session.status.ID,
			Node:         session.status.Node,
			Source:       session.status.Source,
			FencingToken: session.status.FencingToken,
			Sequence:     session.status.SourceSequence,
		}
		return session.activation, nil
	default:
		return SnapshotWALBootstrapActivation{}, ErrSnapshotWALBootstrapInvalidTransition
	}
}

// Abort permanently stops a session. Repeating an abort with the same reason
// is idempotent, which makes retrying a failed transfer safe.
func (c *SnapshotWALBootstrapCoordinator) Abort(id, reason string) error {
	id, err := normalizeSnapshotWALBootstrapLabel(id, "id")
	if err != nil {
		return err
	}
	reason, err = normalizeSnapshotWALBootstrapReason(reason)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionLocked(id)
	if err != nil {
		return err
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseAborted {
		if session.status.AbortReason == reason {
			return nil
		}
		return ErrSnapshotWALBootstrapInvalidTransition
	}
	if session.status.Phase == SnapshotWALBootstrapPhaseActivated {
		return ErrSnapshotWALBootstrapInvalidTransition
	}
	session.status.Phase = SnapshotWALBootstrapPhaseAborted
	session.status.AbortReason = reason
	return nil
}

// Status returns a detached status snapshot for one session.
func (c *SnapshotWALBootstrapCoordinator) Status(id string) (SnapshotWALBootstrapStatus, error) {
	id, err := normalizeSnapshotWALBootstrapLabel(id, "id")
	if err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	session, err := c.sessionLockedRead(id)
	if err != nil {
		return SnapshotWALBootstrapStatus{}, err
	}
	return session.status, nil
}

// Statuses returns deterministic, detached status snapshots sorted by ID.
func (c *SnapshotWALBootstrapCoordinator) Statuses() []SnapshotWALBootstrapStatus {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	statuses := make([]SnapshotWALBootstrapStatus, 0, len(c.sessions))
	for _, session := range c.sessions {
		statuses = append(statuses, session.status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	return statuses
}

func (c *SnapshotWALBootstrapCoordinator) sessionLocked(id string) (*snapshotWALBootstrapSession, error) {
	if c == nil {
		return nil, ErrSnapshotWALBootstrapInvalidOptions
	}
	session, ok := c.sessions[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrSnapshotWALBootstrapNotFound, id)
	}
	return session, nil
}

func (c *SnapshotWALBootstrapCoordinator) sessionLockedRead(id string) (*snapshotWALBootstrapSession, error) {
	if c == nil {
		return nil, ErrSnapshotWALBootstrapInvalidOptions
	}
	session, ok := c.sessions[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrSnapshotWALBootstrapNotFound, id)
	}
	return session, nil
}

func (s *snapshotWALBootstrapSession) checkFence(fencingToken uint64) error {
	if s.status.FencingToken != fencingToken {
		return ErrSnapshotWALBootstrapFenceMismatch
	}
	return nil
}

func validateSnapshotWALBootstrapRequest(request SnapshotWALBootstrapRequest) error {
	if request.ID == "" || request.Node == "" || request.Source == "" || request.SnapshotID == "" || request.FencingToken == 0 {
		return ErrSnapshotWALBootstrapInvalidRequest
	}
	return nil
}

func normalizeSnapshotWALBootstrapLabel(value, field string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) || len(value) > snapshotWALBootstrapMaxLabelSize {
		return "", fmt.Errorf("%w: invalid %s", ErrSnapshotWALBootstrapInvalidRequest, field)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("%w: invalid %s", ErrSnapshotWALBootstrapInvalidRequest, field)
		}
	}
	return value, nil
}

func normalizeSnapshotWALBootstrapReason(reason string) (string, error) {
	if reason == "" || reason != strings.TrimSpace(reason) || len(reason) > snapshotWALBootstrapMaxReasonSize {
		return "", fmt.Errorf("%w: invalid abort reason", ErrSnapshotWALBootstrapInvalidRequest)
	}
	for _, r := range reason {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("%w: invalid abort reason", ErrSnapshotWALBootstrapInvalidRequest)
		}
	}
	return reason, nil
}
