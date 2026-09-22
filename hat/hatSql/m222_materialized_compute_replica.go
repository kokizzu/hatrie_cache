package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// MaxMaterializedViewComputeReplicas bounds the number of independently
	// maintained in-process compute replicas in one set.
	MaxMaterializedViewComputeReplicas         = 16
	maxMaterializedViewComputeReplicaNameBytes = 128
)

var (
	// ErrMaterializedViewComputeReplicaSetInvalid identifies malformed replica
	// definitions or an empty replica set.
	ErrMaterializedViewComputeReplicaSetInvalid = errors.New("materialized view compute replica set is invalid")
	// ErrMaterializedViewComputeReplicaNoHealthy indicates that every replica
	// has been fenced from serving or refreshing.
	ErrMaterializedViewComputeReplicaNoHealthy = errors.New("materialized view compute replica set has no healthy replica")
	// ErrMaterializedViewComputeReplicaUnknown identifies a name not registered
	// in the set.
	ErrMaterializedViewComputeReplicaUnknown = errors.New("materialized view compute replica is unknown")
)

// MaterializedViewComputeReplicaState describes whether a replica may serve
// lookups and receive refresh work.
type MaterializedViewComputeReplicaState string

const (
	MaterializedViewComputeReplicaStateHealthy     MaterializedViewComputeReplicaState = "healthy"
	MaterializedViewComputeReplicaStateUnavailable MaterializedViewComputeReplicaState = "unavailable"
)

// MaterializedViewComputeReplica binds one independently maintained view
// registry to a replica name. The caller owns creation and source wiring for
// each registry; the set owns routing and health fencing.
type MaterializedViewComputeReplica struct {
	Name  string
	Views *MaterializedViews
}

// MaterializedViewComputeReplicaStatus is a point-in-time health and refresh
// view for one compute replica.
type MaterializedViewComputeReplicaStatus struct {
	Name          string
	State         MaterializedViewComputeReplicaState
	Refreshes     uint64
	Failures      uint64
	LastRefreshAt time.Time
	LastError     string
}

// MaterializedViewComputeReplicaRefresh records one replica's result from a
// refresh fanout. An unavailable replica is omitted because it was fenced
// before work was dispatched.
type MaterializedViewComputeReplicaRefresh struct {
	Name      string
	Refreshed []MaterializedViewStatus
	Err       error
}

// MaterializedViewComputeReplicaRefreshReport summarizes one refresh fanout.
type MaterializedViewComputeReplicaRefreshReport struct {
	Replicas []MaterializedViewComputeReplicaRefresh
}

// MaterializedViewComputeReplicaSet routes maintained point lookups across
// independent MaterializedViews registries and refreshes healthy replicas.
// It is opt-in and process-local: durable source ordering and replica
// placement remain the caller's responsibility.
type MaterializedViewComputeReplicaSet struct {
	mu       sync.RWMutex
	replicas []materializedViewComputeReplicaState
	byName   map[string]int
	next     uint64
}

type materializedViewComputeReplicaState struct {
	name          string
	views         *MaterializedViews
	state         MaterializedViewComputeReplicaState
	refreshes     uint64
	failures      uint64
	lastRefreshAt time.Time
	lastError     string
}

// NewMaterializedViewComputeReplicaSet creates an opt-in maintained-view
// replica set. Replica names must be unique, non-empty, valid UTF-8 strings.
func NewMaterializedViewComputeReplicaSet(replicas ...MaterializedViewComputeReplica) (*MaterializedViewComputeReplicaSet, error) {
	if len(replicas) == 0 || len(replicas) > MaxMaterializedViewComputeReplicas {
		return nil, ErrMaterializedViewComputeReplicaSetInvalid
	}
	set := &MaterializedViewComputeReplicaSet{
		replicas: make([]materializedViewComputeReplicaState, 0, len(replicas)),
		byName:   make(map[string]int, len(replicas)),
	}
	for _, replica := range replicas {
		name, err := normalizeMaterializedViewComputeReplicaName(replica.Name)
		if err != nil || replica.Views == nil {
			return nil, ErrMaterializedViewComputeReplicaSetInvalid
		}
		if _, exists := set.byName[name]; exists {
			return nil, ErrMaterializedViewComputeReplicaSetInvalid
		}
		set.byName[name] = len(set.replicas)
		set.replicas = append(set.replicas, materializedViewComputeReplicaState{
			name:  name,
			views: replica.Views,
			state: MaterializedViewComputeReplicaStateHealthy,
		})
	}
	return set, nil
}

// SetReplicaAvailable fences or unfences one replica. Fenced replicas do not
// receive refreshes and are never selected for reads; unfencing does not
// synthesize missing state, so callers should refresh it before relying on it.
func (set *MaterializedViewComputeReplicaSet) SetReplicaAvailable(name string, available bool) error {
	if set == nil {
		return ErrMaterializedViewComputeReplicaSetInvalid
	}
	name, err := normalizeMaterializedViewComputeReplicaName(name)
	if err != nil {
		return err
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	index, exists := set.byName[name]
	if !exists {
		return fmt.Errorf("%w: %q", ErrMaterializedViewComputeReplicaUnknown, name)
	}
	replica := &set.replicas[index]
	if available {
		replica.state = MaterializedViewComputeReplicaStateHealthy
		replica.lastError = ""
	} else {
		replica.state = MaterializedViewComputeReplicaStateUnavailable
	}
	return nil
}

// Status returns replica statuses sorted by name.
func (set *MaterializedViewComputeReplicaSet) Status() []MaterializedViewComputeReplicaStatus {
	if set == nil {
		return nil
	}
	set.mu.RLock()
	statuses := make([]MaterializedViewComputeReplicaStatus, 0, len(set.replicas))
	for _, replica := range set.replicas {
		statuses = append(statuses, MaterializedViewComputeReplicaStatus{
			Name:          replica.name,
			State:         replica.state,
			Refreshes:     replica.refreshes,
			Failures:      replica.failures,
			LastRefreshAt: replica.lastRefreshAt,
			LastError:     replica.lastError,
		})
	}
	set.mu.RUnlock()
	sort.Slice(statuses, func(left, right int) bool { return statuses[left].Name < statuses[right].Name })
	return statuses
}

// RefreshChanged refreshes every healthy replica from the same source
// snapshot contract. The returned report includes each dispatched replica;
// the first refresh error is returned after all replicas have had a chance to
// process the batch, allowing healthy replicas to continue serving.
func (set *MaterializedViewComputeReplicaSet) RefreshChanged(ctx context.Context, changed []string, resolver SourceResolver, options QueryOptions) (MaterializedViewComputeReplicaRefreshReport, error) {
	if set == nil {
		return MaterializedViewComputeReplicaRefreshReport{}, ErrMaterializedViewComputeReplicaSetInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	replicas := set.healthyReplicaSnapshots()
	if len(replicas) == 0 {
		return MaterializedViewComputeReplicaRefreshReport{}, ErrMaterializedViewComputeReplicaNoHealthy
	}
	report := MaterializedViewComputeReplicaRefreshReport{
		Replicas: make([]MaterializedViewComputeReplicaRefresh, 0, len(replicas)),
	}
	var firstErr error
	for _, replica := range replicas {
		refreshed, err := replica.views.RefreshChanged(ctx, changed, resolver, options)
		result := MaterializedViewComputeReplicaRefresh{
			Name:      replica.name,
			Refreshed: refreshed,
			Err:       err,
		}
		report.Replicas = append(report.Replicas, result)
		set.recordRefresh(replica.name, refreshed, err)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("refresh compute replica %q: %w", replica.name, err)
		}
	}
	sort.Slice(report.Replicas, func(left, right int) bool { return report.Replicas[left].Name < report.Replicas[right].Name })
	return report, firstErr
}

// LookupPoint returns a point lookup result from a healthy replica. It tries
// every healthy replica when a replica is behind and does not find the key,
// which makes a partially refreshed set safe for reads during recovery.
func (set *MaterializedViewComputeReplicaSet) LookupPoint(indexName, key string) (QueryResult, bool, error) {
	if set == nil {
		return QueryResult{}, false, ErrMaterializedViewComputeReplicaSetInvalid
	}
	replicas := set.orderedHealthyReplicaSnapshots()
	if len(replicas) == 0 {
		return QueryResult{}, false, ErrMaterializedViewComputeReplicaNoHealthy
	}
	var missingErr error
	anyUsable := false
	for _, replica := range replicas {
		result, found, err := replica.views.LookupPoint(indexName, key)
		if err != nil {
			if errors.Is(err, ErrMaterializedViewPointLookupIndexMissing) {
				missingErr = err
				continue
			}
			return QueryResult{}, false, fmt.Errorf("lookup compute replica %q: %w", replica.name, err)
		}
		anyUsable = true
		if found {
			return result, true, nil
		}
	}
	if anyUsable {
		return QueryResult{}, false, nil
	}
	if missingErr != nil {
		return QueryResult{}, false, missingErr
	}
	return QueryResult{}, false, nil
}

type materializedViewComputeReplicaSnapshot struct {
	name  string
	views *MaterializedViews
}

func (set *MaterializedViewComputeReplicaSet) healthyReplicaSnapshots() []materializedViewComputeReplicaSnapshot {
	set.mu.RLock()
	replicas := make([]materializedViewComputeReplicaSnapshot, 0, len(set.replicas))
	for _, replica := range set.replicas {
		if replica.state == MaterializedViewComputeReplicaStateHealthy {
			replicas = append(replicas, materializedViewComputeReplicaSnapshot{name: replica.name, views: replica.views})
		}
	}
	set.mu.RUnlock()
	sort.Slice(replicas, func(left, right int) bool { return replicas[left].name < replicas[right].name })
	return replicas
}

func (set *MaterializedViewComputeReplicaSet) orderedHealthyReplicaSnapshots() []materializedViewComputeReplicaSnapshot {
	replicas := set.healthyReplicaSnapshots()
	if len(replicas) < 2 {
		return replicas
	}
	set.mu.Lock()
	start := int(set.next % uint64(len(replicas)))
	set.next++
	set.mu.Unlock()
	ordered := make([]materializedViewComputeReplicaSnapshot, 0, len(replicas))
	ordered = append(ordered, replicas[start:]...)
	ordered = append(ordered, replicas[:start]...)
	return ordered
}

func (set *MaterializedViewComputeReplicaSet) recordRefresh(name string, refreshed []MaterializedViewStatus, err error) {
	set.mu.Lock()
	defer set.mu.Unlock()
	index, exists := set.byName[name]
	if !exists {
		return
	}
	replica := &set.replicas[index]
	if err != nil {
		replica.failures++
		replica.lastError = err.Error()
		return
	}
	replica.refreshes++
	replica.lastRefreshAt = time.Now().UTC()
	if len(refreshed) > 0 {
		replica.lastError = ""
	}
}

func normalizeMaterializedViewComputeReplicaName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxMaterializedViewComputeReplicaNameBytes || !utf8.ValidString(name) {
		return "", ErrMaterializedViewComputeReplicaSetInvalid
	}
	return name, nil
}
