package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrMaterializedViewPointLookupIndexRetirementInProgress = errors.New("materialized view point lookup index retirement is in progress")
	ErrMaterializedViewPointLookupReaderClosed              = errors.New("materialized view point lookup reader is closed")
)

// MaterializedViewPointLookupRetirementState describes an asynchronous index
// retirement. Retiring rejects new readers while existing readers drain;
// retired means the index and its reader state have been released.
type MaterializedViewPointLookupRetirementState string

const (
	MaterializedViewPointLookupRetirementStateRetiring MaterializedViewPointLookupRetirementState = "retiring"
	MaterializedViewPointLookupRetirementStateRetired  MaterializedViewPointLookupRetirementState = "retired"
)

// MaterializedViewPointLookupRetirementStatus is a consistent retirement
// progress snapshot.
type MaterializedViewPointLookupRetirementStatus struct {
	IndexName     string
	State         MaterializedViewPointLookupRetirementState
	ActiveReaders int
	StartedAt     time.Time
	FinishedAt    time.Time
}

// MaterializedViewPointLookupRetirement is a handle for one asynchronous
// point lookup index retirement. Its methods are safe for concurrent use.
type MaterializedViewPointLookupRetirement struct {
	state *materializedViewPointLookupRetirementState
}

type materializedViewPointLookupRetirementState struct {
	mu     sync.RWMutex
	status MaterializedViewPointLookupRetirementStatus
	done   chan struct{}
}

type materializedViewPointLookupReaderState struct {
	indexName  string
	active     int
	retiring   bool
	retirement *MaterializedViewPointLookupRetirement
}

// MaterializedViewPointLookupReader retains a safe immutable view of one
// point lookup index until Close is called. Existing readers may finish after
// retirement starts, but new readers cannot be acquired.
type MaterializedViewPointLookupReader struct {
	views     *MaterializedViews
	indexName string
	state     *materializedViewPointLookupReaderState
	index     materializedViewPointLookup

	mu     sync.RWMutex
	closed bool
}

// Status returns the latest retirement progress snapshot.
func (retirement *MaterializedViewPointLookupRetirement) Status() MaterializedViewPointLookupRetirementStatus {
	if retirement == nil || retirement.state == nil {
		return MaterializedViewPointLookupRetirementStatus{}
	}
	return retirement.state.snapshot()
}

// Wait waits for all dependent readers to drain and the index to be retired.
// A caller context timeout only interrupts the wait; it does not cancel
// retirement.
func (retirement *MaterializedViewPointLookupRetirement) Wait(ctx context.Context) (MaterializedViewPointLookupRetirementStatus, error) {
	if retirement == nil || retirement.state == nil {
		return MaterializedViewPointLookupRetirementStatus{}, fmt.Errorf("materialized view point lookup retirement is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-retirement.state.done:
		return retirement.state.snapshot(), nil
	case <-ctx.Done():
		return retirement.state.snapshot(), ctx.Err()
	}
}

// LookupPoint returns rows from the reader's immutable index snapshot.
func (reader *MaterializedViewPointLookupReader) LookupPoint(key string) (QueryResult, bool, error) {
	if reader == nil {
		return QueryResult{}, false, fmt.Errorf("materialized view point lookup reader is nil")
	}
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	if reader.closed {
		return QueryResult{}, false, ErrMaterializedViewPointLookupReaderClosed
	}
	rows, found := reader.index.rows[key]
	result := QueryResult{Columns: append([]string(nil), reader.index.columns...)}
	if found {
		result.Rows = CloneRows(rows)
	}
	return result, found, nil
}

// Close releases the reader lease. It is idempotent.
func (reader *MaterializedViewPointLookupReader) Close() {
	if reader == nil {
		return
	}
	reader.mu.Lock()
	if reader.closed {
		reader.mu.Unlock()
		return
	}
	reader.closed = true
	views := reader.views
	state := reader.state
	reader.mu.Unlock()
	if views != nil && state != nil {
		views.releasePointLookupReader(state)
	}
}

// AcquirePointLookupReader acquires a reader lease for one maintained point
// lookup index. The caller must Close the returned reader.
func (views *MaterializedViews) AcquirePointLookupReader(indexName string) (*MaterializedViewPointLookupReader, error) {
	if views == nil {
		return nil, fmt.Errorf("materialized views are nil")
	}
	indexName = strings.TrimSpace(indexName)
	if indexName == "" {
		return nil, fmt.Errorf("materialized view point lookup index name is required")
	}
	views.mu.Lock()
	index, exists := views.pointLookups[indexName]
	if !exists {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexMissing, indexName)
	}
	view, exists := views.views[index.definition.ViewName]
	if !exists {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupViewMissing, index.definition.ViewName)
	}
	if !materializedViewIsReady(view) {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewHydrationNotReady, index.definition.ViewName)
	}
	if views.pointLookupReaders == nil {
		views.pointLookupReaders = make(map[string]*materializedViewPointLookupReaderState)
	}
	state := views.pointLookupReaders[indexName]
	if state == nil {
		state = &materializedViewPointLookupReaderState{indexName: indexName}
		views.pointLookupReaders[indexName] = state
	}
	if state.retiring {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexRetirementInProgress, indexName)
	}
	state.active++
	reader := &MaterializedViewPointLookupReader{
		views:     views,
		indexName: indexName,
		state:     state,
		index:     index,
	}
	views.mu.Unlock()
	return reader, nil
}

// StartPointLookupIndexRetirement removes an index from new readers and
// returns immediately. Existing acquired readers keep their immutable
// snapshot until Close releases the retirement.
func (views *MaterializedViews) StartPointLookupIndexRetirement(indexName string) (*MaterializedViewPointLookupRetirement, error) {
	if views == nil {
		return nil, fmt.Errorf("materialized views are nil")
	}
	indexName = strings.TrimSpace(indexName)
	if indexName == "" {
		return nil, fmt.Errorf("materialized view point lookup index name is required")
	}
	views.mu.Lock()
	if retirement, exists := views.pointLookupRetirements[indexName]; exists && retirement != nil {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexRetirementInProgress, indexName)
	}
	if _, exists := views.pointLookupBuilds[indexName]; exists {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupBuildInProgress, indexName)
	}
	if _, exists := views.pointLookups[indexName]; !exists {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexMissing, indexName)
	}
	if views.pointLookupReaders == nil {
		views.pointLookupReaders = make(map[string]*materializedViewPointLookupReaderState)
	}
	state := views.pointLookupReaders[indexName]
	if state == nil {
		state = &materializedViewPointLookupReaderState{indexName: indexName}
		views.pointLookupReaders[indexName] = state
	}
	if state.retiring {
		views.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrMaterializedViewPointLookupIndexRetirementInProgress, indexName)
	}
	retirement := &MaterializedViewPointLookupRetirement{state: &materializedViewPointLookupRetirementState{
		status: MaterializedViewPointLookupRetirementStatus{
			IndexName:     indexName,
			State:         MaterializedViewPointLookupRetirementStateRetiring,
			ActiveReaders: state.active,
			StartedAt:     time.Now().UTC(),
		},
		done: make(chan struct{}),
	}}
	state.retiring = true
	state.retirement = retirement
	if views.pointLookupRetirements == nil {
		views.pointLookupRetirements = make(map[string]*MaterializedViewPointLookupRetirement)
	}
	views.pointLookupRetirements[indexName] = retirement
	delete(views.pointLookups, indexName)
	activeReaders := state.active
	if activeReaders == 0 {
		delete(views.pointLookupReaders, indexName)
		delete(views.pointLookupRetirements, indexName)
	}
	views.mu.Unlock()
	if activeReaders == 0 {
		retirement.state.finish()
	}
	return retirement, nil
}

func (state *materializedViewPointLookupRetirementState) snapshot() MaterializedViewPointLookupRetirementStatus {
	state.mu.RLock()
	status := state.status
	state.mu.RUnlock()
	return status
}

func (state *materializedViewPointLookupRetirementState) setActiveReaders(active int) {
	state.mu.Lock()
	state.status.ActiveReaders = active
	state.mu.Unlock()
}

func (state *materializedViewPointLookupRetirementState) finish() {
	state.mu.Lock()
	if state.status.State != MaterializedViewPointLookupRetirementStateRetiring {
		state.mu.Unlock()
		return
	}
	state.status.ActiveReaders = 0
	state.status.State = MaterializedViewPointLookupRetirementStateRetired
	state.status.FinishedAt = time.Now().UTC()
	close(state.done)
	state.mu.Unlock()
}

func (views *MaterializedViews) releasePointLookupReader(state *materializedViewPointLookupReaderState) {
	views.mu.Lock()
	if state.active > 0 {
		state.active--
	}
	activeReaders := state.active
	retirement := state.retirement
	finish := state.retiring && activeReaders == 0
	if finish {
		if views.pointLookupReaders[state.indexName] == state {
			delete(views.pointLookupReaders, state.indexName)
		}
		if retirement != nil && views.pointLookupRetirements[state.indexName] == retirement {
			delete(views.pointLookupRetirements, state.indexName)
		}
	}
	views.mu.Unlock()
	if retirement != nil {
		retirement.state.setActiveReaders(activeReaders)
		if finish {
			retirement.state.finish()
		}
	}
}
