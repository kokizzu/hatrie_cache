package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrMaterializedViewBudgetExceeded indicates that a create or refresh would
// exceed the configured aggregate materialized-view storage budget.
var ErrMaterializedViewBudgetExceeded = errors.New("materialized view storage budget exceeded")

// MaterializedViewDefinition declares a query and the source keys that can
// invalidate its materialized result. Dependencies are explicit so callers can
// refresh only views affected by a source update.
type MaterializedViewDefinition struct {
	Name              string
	Query             string
	Dependencies      []string
	PointLookupFields []string
}

// MaterializedViewStatus describes one immutable materialized-view snapshot.
type MaterializedViewStatus struct {
	Name            string
	Dependencies    []string
	Revision        uint64
	RefreshedAt     time.Time
	IdempotencyKeys []string `json:"idempotency_keys,omitempty"`
}

// MaterializedViewRefreshMetadata carries optional source-write metadata to
// dependent views. Empty metadata preserves the legacy refresh path.
type MaterializedViewRefreshMetadata struct {
	IdempotencyKeys []string
}

// MaterializedViewsOptions bounds the aggregate logical result retained by a
// MaterializedViews registry. A zero limit is unlimited for compatibility.
type MaterializedViewsOptions struct {
	MaxRows  int
	MaxBytes int64
}

// MaterializedViewPointLookupBuildRequest describes an asynchronous point
// posting build for one already-published snapshot. Fields may be omitted only
// when the view already declares PointLookupFields.
type MaterializedViewPointLookupBuildRequest struct {
	ID       string
	ViewName string
	Priority int
	Fields   []string
	// Progress receives the same monotonic completed/total callbacks used by
	// the rebuild queue. It is optional and does not affect publication.
	Progress SQLIndexRebuildProgressFunc
}

// MaterializedViewHydrationState describes the availability of an optional
// maintained point index without changing the view's ordinary snapshot path.
type MaterializedViewHydrationState string

const (
	MaterializedViewHydrationCold      MaterializedViewHydrationState = "cold"
	MaterializedViewHydrationHydrating MaterializedViewHydrationState = "hydrating"
	MaterializedViewHydrationReady     MaterializedViewHydrationState = "ready"
)

// MaterializedViewHydrationStatus reports the state of one view's optional
// point indexes. A queued or running rebuild is hydrating; failed and
// canceled rebuilds fall back to the current published index state.
type MaterializedViewHydrationStatus struct {
	Name                      string                         `json:"name"`
	State                     MaterializedViewHydrationState `json:"state"`
	Revision                  uint64                         `json:"revision"`
	Completed                 int                            `json:"completed"`
	Total                     int                            `json:"total"`
	Progress                  float64                        `json:"progress"`
	EstimatedRemainingSeconds float64                        `json:"estimated_remaining_seconds,omitempty"`
	Fields                    []string                       `json:"fields,omitempty"`
	TaskID                    string                         `json:"task_id,omitempty"`
}

// MaterializedViewStorageUsage reports logical rows and encoded row bytes
// retained by a registry. Bytes are accounted when MaxBytes is configured.
type MaterializedViewStorageUsage struct {
	Rows     int
	Bytes    int64
	MaxRows  int
	MaxBytes int64
}

// MaterializedView combines a snapshot's status with its query result.
type MaterializedView struct {
	Status MaterializedViewStatus
	Result QueryResult
}

// MaterializedViews stores named query-result snapshots. It is safe for
// concurrent reads and refreshes.
type MaterializedViews struct {
	mu         sync.RWMutex
	views      map[string]materializedView
	hydration  map[string]materializedViewHydration
	dependents map[string][]string
	maxRows    int
	maxBytes   int64
	rows       int
	bytes      int64
}

type materializedView struct {
	definition     MaterializedViewDefinition
	parsedQuery    *sqlQuery
	snapshot       MaterializedView
	sourceVersions map[string]string
	collation      SQLCollation
	pointLookups   map[string]map[string][]int
	readGate       *materializedViewReadGate
	storedRows     int
	storedBytes    int64
}

type materializedViewHydration struct {
	queue      *SQLIndexRebuildQueue
	replicaSet *SQLIndexRebuildReplicaSet
	taskID     string
	fields     []string
}

// materializedViewReadGate drains readers of one immutable view generation
// before a selected point index is removed. The pointer is shared by copied
// generations, while the maps and rows themselves are replaced atomically.
type materializedViewReadGate struct {
	mu sync.RWMutex
}

// NewMaterializedViews creates an empty materialized-view registry.
func NewMaterializedViews() *MaterializedViews {
	views, _ := NewMaterializedViewsWithOptions(MaterializedViewsOptions{})
	return views
}

// NewMaterializedViewsWithOptions creates a registry with optional aggregate
// row and logical-byte admission limits. Limits are checked before a new
// snapshot is published; a rejected refresh keeps the prior snapshot.
func NewMaterializedViewsWithOptions(options MaterializedViewsOptions) (*MaterializedViews, error) {
	if options.MaxRows < 0 || options.MaxBytes < 0 {
		return nil, fmt.Errorf("materialized view storage limits must not be negative")
	}
	return &MaterializedViews{
		views:      make(map[string]materializedView),
		hydration:  make(map[string]materializedViewHydration),
		dependents: make(map[string][]string),
		maxRows:    options.MaxRows,
		maxBytes:   options.MaxBytes,
	}, nil
}

// Usage returns the registry's current aggregate logical storage accounting.
func (views *MaterializedViews) Usage() MaterializedViewStorageUsage {
	if views == nil {
		return MaterializedViewStorageUsage{}
	}
	views.mu.RLock()
	usage := MaterializedViewStorageUsage{
		Rows: views.rows, Bytes: views.bytes, MaxRows: views.maxRows, MaxBytes: views.maxBytes,
	}
	views.mu.RUnlock()
	return usage
}

func (views *MaterializedViews) storageBytes(result QueryResult) int64 {
	if views == nil || views.maxBytes <= 0 {
		return 0
	}
	return int64(sqlRowsBytes(result.Rows))
}

func (views *MaterializedViews) checkStorageBudget(rows int, bytes int64) error {
	if views.maxRows > 0 && rows > views.maxRows {
		return fmt.Errorf("%w: rows %d exceeds maximum %d", ErrMaterializedViewBudgetExceeded, rows, views.maxRows)
	}
	if views.maxBytes > 0 && bytes > views.maxBytes {
		return fmt.Errorf("%w: bytes %d exceeds maximum %d", ErrMaterializedViewBudgetExceeded, bytes, views.maxBytes)
	}
	return nil
}

// Create evaluates and publishes a new materialized view. The view name must
// be unique; use RefreshChanged to recompute an existing view.
func (views *MaterializedViews) Create(ctx context.Context, definition MaterializedViewDefinition, resolver SourceResolver, options QueryOptions) (MaterializedViewStatus, error) {
	definition, err := normalizeMaterializedViewDefinition(definition)
	if err != nil {
		return MaterializedViewStatus{}, err
	}
	if views == nil {
		return MaterializedViewStatus{}, fmt.Errorf("materialized views are nil")
	}
	parsedQuery, err := parseSQLQuery(definition.Query)
	if err != nil {
		return MaterializedViewStatus{}, err
	}
	applySQLQueryCollation(parsedQuery, options.Collation)
	result, sourceVersions, err := executeMaterializedViewQuery(ctx, definition.Query, definition.Dependencies, resolver, options)
	if err != nil {
		return MaterializedViewStatus{}, err
	}
	pointLookups, err := buildMaterializedViewPointLookups(definition, result)
	if err != nil {
		return MaterializedViewStatus{}, err
	}

	views.mu.Lock()
	defer views.mu.Unlock()
	if views.views == nil {
		views.views = make(map[string]materializedView)
	}
	if views.dependents == nil {
		views.dependents = make(map[string][]string)
	}
	if _, exists := views.views[definition.Name]; exists {
		return MaterializedViewStatus{}, fmt.Errorf("materialized view %q already exists", definition.Name)
	}
	storedRows := len(result.Rows)
	storedBytes := views.storageBytes(result)
	if err := views.checkStorageBudget(views.rows+storedRows, views.bytes+storedBytes); err != nil {
		return MaterializedViewStatus{}, err
	}
	status := MaterializedViewStatus{
		Name:         definition.Name,
		Dependencies: append([]string(nil), definition.Dependencies...),
		Revision:     1,
		RefreshedAt:  time.Now().UTC(),
	}
	views.views[definition.Name] = materializedView{
		definition:     definition,
		parsedQuery:    parsedQuery,
		collation:      normalizedMaterializedViewCollation(options.Collation),
		sourceVersions: sourceVersions,
		snapshot: MaterializedView{
			Status: status,
			Result: cloneQueryResult(result),
		},
		pointLookups: pointLookups,
		readGate:     &materializedViewReadGate{},
		storedRows:   storedRows,
		storedBytes:  storedBytes,
	}
	views.rows += storedRows
	views.bytes += storedBytes
	for _, dependency := range definition.Dependencies {
		views.dependents[dependency] = append(views.dependents[dependency], definition.Name)
	}
	return cloneMaterializedViewStatus(status), nil
}

// EnqueuePointLookupBuild schedules an asynchronous point-posting build for a
// published snapshot. The snapshot remains readable while the task runs. The
// index is installed only when the captured snapshot revision is still current;
// callers observe row progress and the exclusive row frontier through the
// returned SQLIndexRebuildStatus and SQLIndexRebuildQueue.Status.
func (views *MaterializedViews) EnqueuePointLookupBuild(queue *SQLIndexRebuildQueue, request MaterializedViewPointLookupBuildRequest) (SQLIndexRebuildStatus, error) {
	if views == nil {
		return SQLIndexRebuildStatus{}, fmt.Errorf("materialized views are nil")
	}
	if queue == nil {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildQueueNil
	}
	request.ID = strings.TrimSpace(request.ID)
	request.ViewName = strings.TrimSpace(request.ViewName)
	if request.ID == "" || request.ViewName == "" {
		return SQLIndexRebuildStatus{}, ErrSQLIndexRebuildRequestInvalid
	}

	views.mu.RLock()
	view, exists := views.views[request.ViewName]
	if !exists {
		views.mu.RUnlock()
		return SQLIndexRebuildStatus{}, fmt.Errorf("materialized view %q does not exist", request.ViewName)
	}
	definition := view.definition
	originalFields := append([]string(nil), definition.PointLookupFields...)
	if len(request.Fields) > 0 {
		definition.PointLookupFields = append([]string(nil), request.Fields...)
	}
	definition, err := normalizeMaterializedViewDefinition(definition)
	if err != nil {
		views.mu.RUnlock()
		return SQLIndexRebuildStatus{}, err
	}
	if len(definition.PointLookupFields) == 0 {
		views.mu.RUnlock()
		return SQLIndexRebuildStatus{}, fmt.Errorf("materialized view %q point lookup fields are required", request.ViewName)
	}
	revision := view.snapshot.Status.Revision
	result := view.snapshot.Result
	views.mu.RUnlock()

	views.mu.Lock()
	current, currentExists := views.views[request.ViewName]
	if !currentExists || current.snapshot.Status.Revision != revision ||
		!sameMaterializedViewPointLookupFields(current.definition.PointLookupFields, originalFields) {
		views.mu.Unlock()
		return SQLIndexRebuildStatus{}, fmt.Errorf("materialized view %q changed during point lookup build", request.ViewName)
	}
	previousHydration, hadPreviousHydration := views.hydration[request.ViewName]
	views.hydration[request.ViewName] = materializedViewHydration{
		queue:  queue,
		taskID: request.ID,
		fields: append([]string(nil), definition.PointLookupFields...),
	}
	status, err := queue.Enqueue(SQLIndexRebuildRequest{
		ID:       request.ID,
		Name:     request.ViewName,
		Priority: request.Priority,
		Run: func(ctx context.Context, progress SQLIndexRebuildProgressFunc) error {
			return views.runMaterializedViewPointLookupBuild(ctx, progress, materializedPointLookupBuild{
				viewName:       request.ViewName,
				definition:     definition,
				originalFields: originalFields,
				revision:       revision,
				result:         result,
				progress:       request.Progress,
			}, request.ID)
		},
	})
	if err != nil {
		if hadPreviousHydration {
			views.hydration[request.ViewName] = previousHydration
		} else {
			delete(views.hydration, request.ViewName)
		}
	}
	views.mu.Unlock()
	if err != nil {
		return SQLIndexRebuildStatus{}, err
	}
	return status, nil
}

// EnqueueReplicatedPointLookupBuild fans one revision-fenced point-index
// build to every queue in replicas. Each worker may execute the idempotent
// publication, so the resulting index converges even when one worker fails.
func (views *MaterializedViews) EnqueueReplicatedPointLookupBuild(replicas *SQLIndexRebuildReplicaSet, request MaterializedViewPointLookupBuildRequest) (SQLIndexRebuildReplicaStatus, error) {
	if views == nil {
		return SQLIndexRebuildReplicaStatus{}, fmt.Errorf("materialized views are nil")
	}
	if replicas == nil {
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildReplicaSetInvalid
	}
	request.ID = strings.TrimSpace(request.ID)
	request.ViewName = strings.TrimSpace(request.ViewName)
	if request.ID == "" || request.ViewName == "" {
		return SQLIndexRebuildReplicaStatus{}, ErrSQLIndexRebuildRequestInvalid
	}

	views.mu.RLock()
	view, exists := views.views[request.ViewName]
	if !exists {
		views.mu.RUnlock()
		return SQLIndexRebuildReplicaStatus{}, fmt.Errorf("materialized view %q does not exist", request.ViewName)
	}
	definition := view.definition
	originalFields := append([]string(nil), definition.PointLookupFields...)
	if len(request.Fields) > 0 {
		definition.PointLookupFields = append([]string(nil), request.Fields...)
	}
	definition, err := normalizeMaterializedViewDefinition(definition)
	if err != nil {
		views.mu.RUnlock()
		return SQLIndexRebuildReplicaStatus{}, err
	}
	if len(definition.PointLookupFields) == 0 {
		views.mu.RUnlock()
		return SQLIndexRebuildReplicaStatus{}, fmt.Errorf("materialized view %q point lookup fields are required", request.ViewName)
	}
	build := materializedPointLookupBuild{
		viewName:       request.ViewName,
		definition:     definition,
		originalFields: originalFields,
		revision:       view.snapshot.Status.Revision,
		result:         view.snapshot.Result,
		progress:       request.Progress,
	}
	views.mu.RUnlock()

	views.mu.Lock()
	current, currentExists := views.views[request.ViewName]
	if !currentExists || current.snapshot.Status.Revision != build.revision ||
		!sameMaterializedViewPointLookupFields(current.definition.PointLookupFields, build.originalFields) {
		views.mu.Unlock()
		return SQLIndexRebuildReplicaStatus{}, fmt.Errorf("materialized view %q changed during point lookup build", request.ViewName)
	}
	previousHydration, hadPreviousHydration := views.hydration[request.ViewName]
	views.hydration[request.ViewName] = materializedViewHydration{
		replicaSet: replicas,
		taskID:     request.ID,
		fields:     append([]string(nil), build.definition.PointLookupFields...),
	}
	status, err := replicas.Enqueue(SQLIndexRebuildRequest{
		ID:       request.ID,
		Name:     request.ViewName,
		Priority: request.Priority,
		Run: func(ctx context.Context, progress SQLIndexRebuildProgressFunc) error {
			return views.runMaterializedViewPointLookupBuild(ctx, progress, build, request.ID)
		},
	})
	if err != nil {
		if hadPreviousHydration {
			views.hydration[request.ViewName] = previousHydration
		} else {
			delete(views.hydration, request.ViewName)
		}
	}
	views.mu.Unlock()
	if err != nil {
		return SQLIndexRebuildReplicaStatus{}, err
	}
	return status, nil
}

type materializedPointLookupBuild struct {
	viewName       string
	definition     MaterializedViewDefinition
	originalFields []string
	revision       uint64
	result         QueryResult
	progress       SQLIndexRebuildProgressFunc
}

func (views *MaterializedViews) runMaterializedViewPointLookupBuild(ctx context.Context, progress SQLIndexRebuildProgressFunc, build materializedPointLookupBuild, operationID string) error {
	if build.progress != nil {
		queueProgress := progress
		progress = func(completed, total int) {
			if queueProgress != nil {
				queueProgress(completed, total)
			}
			build.progress(completed, total)
		}
	}
	lookups, err := buildMaterializedViewPointLookupsWithProgress(ctx, build.definition, build.result, progress)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	views.mu.Lock()
	defer views.mu.Unlock()
	hydration, hydrationExists := views.hydration[build.viewName]
	if !hydrationExists || hydration.taskID != operationID {
		return fmt.Errorf("materialized view %q point lookup build is no longer current", build.viewName)
	}
	current, exists := views.views[build.viewName]
	if !exists || current.snapshot.Status.Revision != build.revision ||
		(!sameMaterializedViewPointLookupFields(current.definition.PointLookupFields, build.originalFields) &&
			!sameMaterializedViewPointLookupFields(current.definition.PointLookupFields, build.definition.PointLookupFields)) {
		return fmt.Errorf("materialized view %q changed during point lookup build", build.viewName)
	}
	current.definition.PointLookupFields = append([]string(nil), build.definition.PointLookupFields...)
	current.pointLookups = lookups
	hydration.fields = nil
	views.hydration[build.viewName] = hydration
	views.views[build.viewName] = current
	return nil
}

// Get returns an independent copy of the named materialized-view snapshot.
func (views *MaterializedViews) Get(name string) (MaterializedView, bool) {
	if views == nil {
		return MaterializedView{}, false
	}
	views.mu.RLock()
	view, exists := views.views[strings.TrimSpace(name)]
	views.mu.RUnlock()
	if !exists {
		return MaterializedView{}, false
	}
	return cloneMaterializedView(view.snapshot), true
}

// HydrationStatus returns the current optional point-index state for a view.
// It is deliberately separate from MaterializedViewStatus so existing
// snapshot consumers do not need to understand background index lifecycle.
func (views *MaterializedViews) HydrationStatus(name string) (MaterializedViewHydrationStatus, bool) {
	if views == nil {
		return MaterializedViewHydrationStatus{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return MaterializedViewHydrationStatus{}, false
	}
	views.mu.RLock()
	view, exists := views.views[name]
	if !exists {
		views.mu.RUnlock()
		return MaterializedViewHydrationStatus{}, false
	}
	state := materializedViewHydrationBaseState(view)
	status := MaterializedViewHydrationStatus{
		Name:     name,
		State:    state,
		Revision: view.snapshot.Status.Revision,
		Fields:   append([]string(nil), view.definition.PointLookupFields...),
	}
	hydration := views.hydration[name]
	views.mu.RUnlock()

	taskID := hydration.taskID
	if taskID == "" {
		return status, true
	}
	if hydration.replicaSet != nil {
		replicated, exists := hydration.replicaSet.Status(taskID)
		if !exists {
			return status, true
		}
		if task, ok := materializedViewHydrationReplicaTask(replicated); ok {
			applyMaterializedViewHydrationProgress(&status, task)
		}
		switch replicated.State {
		case SQLIndexRebuildQueued, SQLIndexRebuildRunning:
			status.State = MaterializedViewHydrationHydrating
			status.Fields = append([]string(nil), hydration.fields...)
			status.TaskID = taskID
		case SQLIndexRebuildSucceeded:
			if state == MaterializedViewHydrationReady {
				status.TaskID = taskID
			}
		}
		return status, true
	}
	queue := hydration.queue
	if queue == nil {
		return status, true
	}
	task, exists := queue.Status(taskID)
	if !exists {
		return status, true
	}
	applyMaterializedViewHydrationProgress(&status, task)
	switch task.State {
	case SQLIndexRebuildQueued, SQLIndexRebuildRunning:
		status.State = MaterializedViewHydrationHydrating
		status.Fields = append([]string(nil), hydration.fields...)
		status.TaskID = taskID
	case SQLIndexRebuildSucceeded:
		if state == MaterializedViewHydrationReady {
			status.TaskID = taskID
		}
	case SQLIndexRebuildFailed, SQLIndexRebuildCanceled:
		// The currently published index, if any, remains authoritative after
		// a failed or canceled replacement build.
	}
	return status, true
}

func applyMaterializedViewHydrationProgress(status *MaterializedViewHydrationStatus, task SQLIndexRebuildStatus) {
	completed := task.Completed
	if completed < 0 {
		completed = 0
	}
	total := task.Total
	if total < 0 {
		total = 0
	}
	if completed > total && total > 0 {
		completed = total
	}
	status.Completed = completed
	status.Total = total
	status.Progress = 0
	if total > 0 {
		status.Progress = float64(completed) / float64(total)
	} else if task.State == SQLIndexRebuildSucceeded {
		status.Progress = 1
	}
	status.EstimatedRemainingSeconds = 0
	if task.State == SQLIndexRebuildRunning && completed > 0 && total > completed && !task.StartedAt.IsZero() {
		elapsed := time.Since(task.StartedAt)
		if elapsed > 0 {
			status.EstimatedRemainingSeconds = elapsed.Seconds() * float64(total-completed) / float64(completed)
		}
	}
}

func materializedViewHydrationReplicaTask(status SQLIndexRebuildReplicaStatus) (SQLIndexRebuildStatus, bool) {
	best := SQLIndexRebuildStatus{}
	found := false
	for _, task := range status.ReplicaStatuses {
		if status.State == SQLIndexRebuildSucceeded && task.State != SQLIndexRebuildSucceeded {
			continue
		}
		if status.State != SQLIndexRebuildSucceeded && task.State != SQLIndexRebuildRunning && task.State != SQLIndexRebuildQueued {
			continue
		}
		if !found || materializedViewHydrationTaskProgress(task) > materializedViewHydrationTaskProgress(best) {
			best = task
			found = true
		}
	}
	return best, found
}

func materializedViewHydrationTaskProgress(task SQLIndexRebuildStatus) float64 {
	if task.State == SQLIndexRebuildSucceeded && task.Total == 0 {
		return 1
	}
	if task.Total <= 0 || task.Completed <= 0 {
		return 0
	}
	completed := task.Completed
	if completed > task.Total {
		completed = task.Total
	}
	return float64(completed) / float64(task.Total)
}

func materializedViewHydrationBaseState(view materializedView) MaterializedViewHydrationState {
	if len(view.definition.PointLookupFields) == 0 {
		return MaterializedViewHydrationCold
	}
	for _, field := range view.definition.PointLookupFields {
		if _, indexed := view.pointLookups[field]; !indexed {
			return MaterializedViewHydrationCold
		}
	}
	return MaterializedViewHydrationReady
}

func (views *MaterializedViews) clearMaterializedViewHydration(name string) {
	delete(views.hydration, name)
}

// PointLookup returns complete rows from an opt-in maintained point index.
// available is false when the view or field has no configured point index, so
// callers can safely fall back to a normal snapshot or source scan.
func (views *MaterializedViews) PointLookup(name, field string, value interface{}) ([]Row, bool, error) {
	if views == nil {
		return nil, false, fmt.Errorf("materialized views are nil")
	}
	name = strings.TrimSpace(name)
	field = strings.TrimSpace(field)
	if name == "" {
		return nil, false, fmt.Errorf("materialized view name is required")
	}
	if field == "" {
		return nil, false, fmt.Errorf("materialized view point lookup field is required")
	}
	key, supported := materializedViewPointLookupKey(value)
	if !supported {
		return nil, false, nil
	}

	views.mu.RLock()
	view, exists := views.views[name]
	if !exists {
		views.mu.RUnlock()
		return nil, false, nil
	}
	if view.readGate == nil {
		views.mu.RUnlock()
		return nil, false, fmt.Errorf("materialized view %q reader gate is missing", name)
	}
	view.readGate.mu.RLock()
	views.mu.RUnlock()
	defer view.readGate.mu.RUnlock()
	postings, available := view.pointLookups[field]
	if !available {
		return nil, false, nil
	}
	indexes := postings[key]
	rows := make([]Row, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(view.snapshot.Result.Rows) {
			return nil, false, fmt.Errorf("materialized view %q point lookup index is inconsistent", name)
		}
		rows = append(rows, cloneResultCacheRow(view.snapshot.Result.Rows[index]))
	}
	return rows, true, nil
}

// DropPointLookupFields removes selected point postings without dropping the
// materialized rows. Existing readers drain on the view's generation gate
// before the removal is published; future readers observe the remaining index
// fields and fall back to the snapshot arrangement when a field is absent.
// Passing no fields removes all point postings. Repeating a removal is safe.
func (views *MaterializedViews) DropPointLookupFields(name string, fields ...string) error {
	if views == nil {
		return fmt.Errorf("materialized views are nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("materialized view name is required")
	}
	remove := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return fmt.Errorf("materialized view point lookup field is required")
		}
		remove[field] = struct{}{}
	}

	views.mu.Lock()
	defer views.mu.Unlock()
	view, exists := views.views[name]
	if !exists {
		return fmt.Errorf("materialized view %q does not exist", name)
	}
	if view.readGate == nil {
		return fmt.Errorf("materialized view %q reader gate is missing", name)
	}
	view.readGate.mu.Lock()
	defer view.readGate.mu.Unlock()
	if len(remove) == 0 {
		view.definition.PointLookupFields = nil
		view.pointLookups = nil
		views.clearMaterializedViewHydration(name)
		views.views[name] = view
		return nil
	}

	remainingFields := make([]string, 0, len(view.definition.PointLookupFields))
	remainingLookups := make(map[string]map[string][]int, len(view.pointLookups))
	for _, field := range view.definition.PointLookupFields {
		if _, drop := remove[field]; drop {
			continue
		}
		remainingFields = append(remainingFields, field)
		if postings, indexed := view.pointLookups[field]; indexed {
			remainingLookups[field] = postings
		}
	}
	if len(remainingFields) == 0 {
		remainingFields = nil
		remainingLookups = nil
	}
	view.definition.PointLookupFields = remainingFields
	view.pointLookups = remainingLookups
	views.clearMaterializedViewHydration(name)
	views.views[name] = view
	return nil
}

// Drop removes one named materialized view and its retained storage accounting.
// The operation is idempotent only for nil registries; callers receive an error
// when the requested view does not exist.
func (views *MaterializedViews) Drop(name string) error {
	if views == nil {
		return fmt.Errorf("materialized views are nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("materialized view name is required")
	}
	views.mu.Lock()
	defer views.mu.Unlock()
	view, exists := views.views[name]
	if !exists {
		return fmt.Errorf("materialized view %q does not exist", name)
	}
	delete(views.views, name)
	views.clearMaterializedViewHydration(name)
	views.rows -= view.storedRows
	views.bytes -= view.storedBytes
	for dependency, dependents := range views.dependents {
		filtered := dependents[:0]
		for _, dependent := range dependents {
			if dependent != name {
				filtered = append(filtered, dependent)
			}
		}
		if len(filtered) == 0 {
			delete(views.dependents, dependency)
		} else {
			views.dependents[dependency] = filtered
		}
	}
	return nil
}

// RefreshChanged atomically publishes refreshed snapshots for views whose
// dependencies intersect changed. It leaves all prior snapshots untouched if
// any candidate query fails.
func (views *MaterializedViews) RefreshChanged(ctx context.Context, changed []string, resolver SourceResolver, options QueryOptions) ([]MaterializedViewStatus, error) {
	return views.RefreshChangedWithMetadata(ctx, changed, resolver, options, MaterializedViewRefreshMetadata{})
}

// RefreshChangedWithMetadata is RefreshChanged with optional metadata from
// the source mutation batch. The metadata is retained on each refreshed
// dependent-view status so consumers can correlate a published snapshot with
// its asynchronous source writes.
func (views *MaterializedViews) RefreshChangedWithMetadata(ctx context.Context, changed []string, resolver SourceResolver, options QueryOptions, metadata MaterializedViewRefreshMetadata) ([]MaterializedViewStatus, error) {
	if views == nil {
		return nil, fmt.Errorf("materialized views are nil")
	}
	metadataKeys := normalizeMaterializedViewIdempotencyKeys(metadata.IdempotencyKeys)
	changedSet := make(map[string]struct{}, len(changed))
	for _, dependency := range changed {
		if dependency = strings.TrimSpace(dependency); dependency != "" {
			changedSet[dependency] = struct{}{}
		}
	}
	if len(changedSet) == 0 {
		return nil, nil
	}

	views.mu.RLock()
	candidateNames := make(map[string]struct{})
	for dependency := range changedSet {
		for _, name := range views.dependents[dependency] {
			candidateNames[name] = struct{}{}
		}
	}
	candidates := make([]materializedView, 0, len(candidateNames))
	for name := range candidateNames {
		if view, exists := views.views[name]; exists {
			candidates = append(candidates, view)
		}
	}
	views.mu.RUnlock()
	sort.Slice(candidates, func(left, right int) bool {
		return candidates[left].definition.Name < candidates[right].definition.Name
	})
	if len(candidates) == 0 {
		return nil, nil
	}

	results := make(map[string]QueryResult, len(candidates))
	versions := make(map[string]map[string]string, len(candidates))
	resultRows := make(map[string]int, len(candidates))
	resultBytes := make(map[string]int64, len(candidates))
	pointLookups := make(map[string]map[string]map[string][]int, len(candidates))
	for _, candidate := range candidates {
		result, sourceVersions, err := executeMaterializedViewQuery(ctx, candidate.definition.Query, candidate.definition.Dependencies, resolver, options)
		if err != nil {
			return nil, fmt.Errorf("refresh materialized view %q: %w", candidate.definition.Name, err)
		}
		results[candidate.definition.Name] = cloneQueryResult(result)
		pointLookups[candidate.definition.Name], err = buildMaterializedViewPointLookups(candidate.definition, result)
		if err != nil {
			return nil, fmt.Errorf("refresh materialized view %q point lookup: %w", candidate.definition.Name, err)
		}
		versions[candidate.definition.Name] = sourceVersions
		resultRows[candidate.definition.Name] = len(result.Rows)
		resultBytes[candidate.definition.Name] = views.storageBytes(result)
	}

	refreshedAt := time.Now().UTC()
	statuses := make([]MaterializedViewStatus, 0, len(candidates))
	views.mu.Lock()
	defer views.mu.Unlock()
	nextRows, nextBytes := views.rows, views.bytes
	for _, candidate := range candidates {
		current, exists := views.views[candidate.definition.Name]
		if !exists || !sameMaterializedViewDefinition(current.definition, candidate.definition) {
			continue
		}
		nextRows += resultRows[candidate.definition.Name] - current.storedRows
		nextBytes += resultBytes[candidate.definition.Name] - current.storedBytes
	}
	if err := views.checkStorageBudget(nextRows, nextBytes); err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		current, exists := views.views[candidate.definition.Name]
		if !exists || !sameMaterializedViewDefinition(current.definition, candidate.definition) {
			continue
		}
		current.snapshot.Result = results[candidate.definition.Name]
		current.pointLookups = pointLookups[candidate.definition.Name]
		current.sourceVersions = versions[candidate.definition.Name]
		current.collation = normalizedMaterializedViewCollation(options.Collation)
		current.snapshot.Status.Revision++
		current.snapshot.Status.RefreshedAt = refreshedAt
		current.snapshot.Status.IdempotencyKeys = append([]string(nil), metadataKeys...)
		current.storedRows = resultRows[candidate.definition.Name]
		current.storedBytes = resultBytes[candidate.definition.Name]
		views.clearMaterializedViewHydration(candidate.definition.Name)
		views.views[candidate.definition.Name] = current
		statuses = append(statuses, cloneMaterializedViewStatus(current.snapshot.Status))
	}
	views.rows = nextRows
	views.bytes = nextBytes
	return statuses, nil
}

func normalizedMaterializedViewCollation(collation SQLCollation) SQLCollation {
	if collation == "" {
		return SQLCollationBinary
	}
	return collation
}

func materializedViewSourceVersions(resolver SourceResolver, dependencies []string) map[string]string {
	versions, ok := resolver.(SourceVersionResolver)
	if !ok || len(dependencies) == 0 {
		return nil
	}
	result := make(map[string]string, len(dependencies))
	for _, dependency := range dependencies {
		version, available, err := versions.SQLSourceVersion("CACHE", dependency)
		if err != nil || !available || version == "" {
			return nil
		}
		result[dependency] = version
	}
	return result
}

func sameMaterializedViewPointLookupFields(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func buildMaterializedViewPointLookups(definition MaterializedViewDefinition, result QueryResult) (map[string]map[string][]int, error) {
	if len(definition.PointLookupFields) == 0 {
		return nil, nil
	}
	columns := make(map[string]struct{}, len(result.Columns))
	for _, column := range result.Columns {
		columns[column] = struct{}{}
	}
	lookups := make(map[string]map[string][]int, len(definition.PointLookupFields))
	for _, field := range definition.PointLookupFields {
		if _, exists := columns[field]; !exists {
			return nil, fmt.Errorf("point lookup field %q is not a materialized view output column", field)
		}
		postings := make(map[string][]int)
		for rowIndex, row := range result.Rows {
			value, exists := row[field]
			if !exists {
				continue
			}
			key, supported := materializedViewPointLookupKey(value)
			if !supported {
				return nil, fmt.Errorf("point lookup field %q contains unsupported value type %T", field, value)
			}
			postings[key] = append(postings[key], rowIndex)
		}
		lookups[field] = postings
	}
	return lookups, nil
}

func buildMaterializedViewPointLookupsWithProgress(ctx context.Context, definition MaterializedViewDefinition, result QueryResult, progress SQLIndexRebuildProgressFunc) (map[string]map[string][]int, error) {
	if len(definition.PointLookupFields) == 0 {
		return nil, nil
	}
	columns := make(map[string]struct{}, len(result.Columns))
	for _, column := range result.Columns {
		columns[column] = struct{}{}
	}
	lookups := make(map[string]map[string][]int, len(definition.PointLookupFields))
	for _, field := range definition.PointLookupFields {
		if _, exists := columns[field]; !exists {
			return nil, fmt.Errorf("point lookup field %q is not a materialized view output column", field)
		}
		lookups[field] = make(map[string][]int)
	}
	total := len(result.Rows)
	if progress != nil {
		progress(0, total)
	}
	const batchSize = 256
	for start := 0; start < total; start += batchSize {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		end := start + batchSize
		if end > total {
			end = total
		}
		for rowIndex := start; rowIndex < end; rowIndex++ {
			row := result.Rows[rowIndex]
			for _, field := range definition.PointLookupFields {
				value, exists := row[field]
				if !exists {
					continue
				}
				key, supported := materializedViewPointLookupKey(value)
				if !supported {
					return nil, fmt.Errorf("point lookup field %q contains unsupported value type %T", field, value)
				}
				lookups[field][key] = append(lookups[field][key], rowIndex)
			}
		}
		if progress != nil {
			progress(end, total)
		}
	}
	return lookups, nil
}

func materializedViewPointLookupKey(value interface{}) (string, bool) {
	var buffer [32]byte
	key := buffer[:0]
	switch value := value.(type) {
	case nil:
		return "n", true
	case bool:
		if value {
			return "b:1", true
		}
		return "b:0", true
	case string:
		return "s:" + value, true
	case []byte:
		key = append(key, 'y', ':')
		key = append(key, value...)
		return string(key), true
	case int:
		key = append(key, 'i', ':')
		key = strconv.AppendInt(key, int64(value), 10)
		return string(key), true
	case int8:
		key = append(key, 'i', ':')
		key = strconv.AppendInt(key, int64(value), 10)
		return string(key), true
	case int16:
		key = append(key, 'i', ':')
		key = strconv.AppendInt(key, int64(value), 10)
		return string(key), true
	case int32:
		key = append(key, 'i', ':')
		key = strconv.AppendInt(key, int64(value), 10)
		return string(key), true
	case int64:
		key = append(key, 'i', ':')
		key = strconv.AppendInt(key, value, 10)
		return string(key), true
	case uint:
		key = append(key, 'u', ':')
		key = strconv.AppendUint(key, uint64(value), 10)
		return string(key), true
	case uint8:
		key = append(key, 'u', ':')
		key = strconv.AppendUint(key, uint64(value), 10)
		return string(key), true
	case uint16:
		key = append(key, 'u', ':')
		key = strconv.AppendUint(key, uint64(value), 10)
		return string(key), true
	case uint32:
		key = append(key, 'u', ':')
		key = strconv.AppendUint(key, uint64(value), 10)
		return string(key), true
	case uint64:
		key = append(key, 'u', ':')
		key = strconv.AppendUint(key, value, 10)
		return string(key), true
	case float32:
		key = append(key, 'f', ':')
		key = strconv.AppendFloat(key, float64(value), 'g', -1, 32)
		return string(key), true
	case float64:
		key = append(key, 'f', ':')
		key = strconv.AppendFloat(key, value, 'g', -1, 64)
		return string(key), true
	case time.Time:
		return "t:" + value.UTC().Format(time.RFC3339Nano), true
	default:
		return "", false
	}
}

func executeMaterializedViewQuery(ctx context.Context, query string, dependencies []string, resolver SourceResolver, options QueryOptions) (QueryResult, map[string]string, error) {
	before := materializedViewSourceVersions(resolver, dependencies)
	queryOptions := options
	queryOptions.ProjectionCatalog = nil
	result, err := ExecuteQueryParameters(ctx, query, resolver, nil, queryOptions)
	if err != nil {
		return QueryResult{}, nil, err
	}
	after := materializedViewSourceVersions(resolver, dependencies)
	if before != nil && after != nil && !sameMaterializedViewSourceVersions(before, after) {
		return QueryResult{}, nil, fmt.Errorf("materialized view source changed during refresh")
	}
	return result, after, nil
}

func sameMaterializedViewSourceVersions(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func (views *MaterializedViews) lookupExact(query string, resolver SourceResolver, options QueryOptions) (QueryResult, bool) {
	if views == nil || resolver == nil || strings.TrimSpace(query) == "" || options.IndexHint.Mode != "" {
		return QueryResult{}, false
	}
	query = strings.TrimSpace(query)
	requestedCollation := normalizedMaterializedViewCollation(options.Collation)
	versions, versioned := resolver.(SourceVersionResolver)
	if !versioned {
		return QueryResult{}, false
	}
	views.mu.RLock()
	names := make([]string, 0, len(views.views))
	for name, view := range views.views {
		if view.definition.Query == query && view.collation == requestedCollation && len(view.sourceVersions) == len(view.definition.Dependencies) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		view := views.views[name]
		fresh := true
		for _, dependency := range view.definition.Dependencies {
			version, available, err := versions.SQLSourceVersion("CACHE", dependency)
			if err != nil || !available || version == "" || version != view.sourceVersions[dependency] {
				fresh = false
				break
			}
		}
		if fresh {
			result := cloneQueryResult(view.snapshot.Result)
			result.Plan = []ExplainStep{{Node: "PROJECTION HIT", Detail: name}}
			views.mu.RUnlock()
			return result, true
		}
	}
	views.mu.RUnlock()
	return QueryResult{}, false
}

func (views *MaterializedViews) lookupPoint(query *sqlQuery, resolver SourceResolver, options QueryOptions) (QueryResult, bool, error) {
	if views == nil || query == nil || resolver == nil || options.IndexHint.Mode != "" {
		return QueryResult{}, false, nil
	}
	predicateField, predicateValue, ok := materializedPointLookupPredicate(query)
	if !ok {
		return QueryResult{}, false, nil
	}
	versions, versioned := resolver.(SourceVersionResolver)
	if !versioned {
		return QueryResult{}, false, nil
	}

	views.mu.RLock()
	names := make([]string, 0, len(views.views))
	for name := range views.views {
		names = append(names, name)
	}
	sort.Strings(names)
	views.mu.RUnlock()
	for _, name := range names {
		views.mu.RLock()
		view, exists := views.views[name]
		if !exists {
			views.mu.RUnlock()
			continue
		}
		if view.readGate == nil {
			views.mu.RUnlock()
			return QueryResult{}, false, fmt.Errorf("materialized view %q reader gate is missing", name)
		}
		view.readGate.mu.RLock()
		views.mu.RUnlock()
		outputField, compatible := materializedPointLookupOutputField(view.parsedQuery, query, predicateField)
		if !compatible || !materializedViewFresh(view, versions) {
			view.readGate.mu.RUnlock()
			continue
		}

		rows := view.snapshot.Result.Rows
		pointIndexes, indexed := view.pointLookups[outputField]
		usePoint := false
		var selected []Row
		if indexed && predicateValue != nil {
			key, supported := materializedViewPointLookupKey(predicateValue)
			if supported {
				indexes := pointIndexes[key]
				if materializedPointLookupPreferred(len(indexes), len(rows)) {
					var err error
					selected, err = materializedPointLookupRows(rows, indexes, name)
					if err != nil {
						view.readGate.mu.RUnlock()
						return QueryResult{}, false, err
					}
					usePoint = true
				}
			}
		}
		node := "MATERIALIZED ARRANGEMENT SCAN"
		if usePoint {
			node = "MATERIALIZED POINT LOOKUP"
		} else {
			var err error
			selected, err = materializedArrangementScanRows(rows, outputField, predicateValue, query.where.collation)
			if err != nil {
				view.readGate.mu.RUnlock()
				return QueryResult{}, false, err
			}
		}
		result := materializedLookupResult(view.snapshot.Result, selected, node, name+"."+outputField)
		view.readGate.mu.RUnlock()
		return result, true, nil
	}
	return QueryResult{}, false, nil
}

func materializedPointLookupPredicate(query *sqlQuery) (string, interface{}, bool) {
	if query == nil || query.from == nil || query.where.kind != "binary" || query.where.op != "=" || query.where.left == nil || query.where.right == nil || query.where.collation.normalized() != SQLCollationBinary {
		return "", nil, false
	}
	field, literal := query.where.left, query.where.right
	if field.kind != "field" || literal.kind != "literal" {
		field, literal = query.where.right, query.where.left
	}
	if field.kind != "field" || literal.kind != "literal" || field.qualifier != query.from.alias || field.name == "" {
		return "", nil, false
	}
	return field.name, literal.value, true
}

func materializedPointLookupOutputField(base, candidate *sqlQuery, predicateField string) (string, bool) {
	if base == nil || candidate == nil || base.from == nil || candidate.from == nil || base.where.kind != "" || !materializedPointLookupQueryShapeEqual(base, candidate) || !sameMaterializedPointLookupSource(base.from, candidate.from) {
		return "", false
	}
	columns := sqlColumns(base.selects)
	for index, item := range base.selects {
		if item.expr.kind == "field" && item.expr.name == predicateField && item.expr.qualifier == base.from.alias {
			return columns[index], true
		}
	}
	return "", false
}

func materializedPointLookupQueryShapeEqual(base, candidate *sqlQuery) bool {
	if base == nil || candidate == nil || base.from == nil || candidate.from == nil || base.from.kind != "CACHE" || candidate.from.kind != "CACHE" || len(base.ctes) != 0 || len(candidate.ctes) != 0 || len(base.joins) != 0 || len(candidate.joins) != 0 || len(base.groupBy) != 0 || len(candidate.groupBy) != 0 || len(base.groupingSets) != 0 || len(candidate.groupingSets) != 0 || len(base.groupingDimensions) != 0 || len(candidate.groupingDimensions) != 0 || base.having.kind != "" || candidate.having.kind != "" || base.qualify.kind != "" || candidate.qualify.kind != "" || len(base.orderBy) != 0 || len(candidate.orderBy) != 0 || base.sample != nil || candidate.sample != nil || base.limitBy != nil || candidate.limitBy != nil || len(base.unions) != 0 || len(candidate.unions) != 0 || base.distinct || candidate.distinct || base.offset != 0 || candidate.offset != 0 || base.limit >= 0 || candidate.limit >= 0 || base.explain || candidate.explain || base.pipeline || candidate.pipeline || base.analyze || candidate.analyze || base.prewhere.kind != "" || candidate.prewhere.kind != "" {
		return false
	}
	if len(base.selects) != len(candidate.selects) {
		return false
	}
	for index := range base.selects {
		if base.selects[index].alias != candidate.selects[index].alias || !sqlExpressionsStructurallyEqual(base.selects[index].expr, candidate.selects[index].expr) {
			return false
		}
	}
	return true
}

func sameMaterializedPointLookupSource(left, right *sqlSource) bool {
	if left == nil || right == nil || left.kind != right.kind || left.key != right.key || left.alias != right.alias || left.lateral != right.lateral || left.final != right.final || left.keyParameter != right.keyParameter || len(left.values) != 0 || len(right.values) != 0 || len(left.columns) != 0 || len(right.columns) != 0 || len(left.fieldTypes) != 0 || len(right.fieldTypes) != 0 || left.query != nil || right.query != nil {
		return false
	}
	return true
}

func materializedViewFresh(view materializedView, versions SourceVersionResolver) bool {
	if versions == nil || len(view.sourceVersions) != len(view.definition.Dependencies) {
		return false
	}
	for _, dependency := range view.definition.Dependencies {
		version, available, err := versions.SQLSourceVersion("CACHE", dependency)
		if err != nil || !available || version == "" || version != view.sourceVersions[dependency] {
			return false
		}
	}
	return true
}

func materializedPointLookupPreferred(matches, total int) bool {
	return total == 0 || matches <= total/2
}

func materializedPointLookupRows(rows []Row, indexes []int, name string) ([]Row, error) {
	selected := make([]Row, 0, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(rows) {
			return nil, fmt.Errorf("materialized view %q point lookup index is inconsistent", name)
		}
		selected = append(selected, cloneResultCacheRow(rows[index]))
	}
	return selected, nil
}

func materializedArrangementScanRows(rows []Row, field string, value interface{}, collation SQLCollation) ([]Row, error) {
	selected := make([]Row, 0)
	for _, row := range rows {
		matched := sqlBinaryValueWithCollation("=", row[field], value, collation)
		if err := sqlExpressionError(matched); err != nil {
			return nil, err
		}
		if sqlTruthy(matched) {
			selected = append(selected, cloneResultCacheRow(row))
		}
	}
	return selected, nil
}

func materializedLookupResult(snapshot QueryResult, rows []Row, node, detail string) QueryResult {
	return QueryResult{
		Columns: append([]string(nil), snapshot.Columns...),
		Rows:    rows,
		Plan:    []ExplainStep{{Node: node, Detail: detail}},
	}
}

func normalizeMaterializedViewDefinition(definition MaterializedViewDefinition) (MaterializedViewDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.Query = strings.TrimSpace(definition.Query)
	if definition.Name == "" {
		return MaterializedViewDefinition{}, fmt.Errorf("materialized view name is required")
	}
	if definition.Query == "" {
		return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q query is required", definition.Name)
	}
	if len(definition.Dependencies) == 0 {
		return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q dependencies are required", definition.Name)
	}
	dependencies := make([]string, 0, len(definition.Dependencies))
	seen := make(map[string]struct{}, len(definition.Dependencies))
	for _, dependency := range definition.Dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q has an empty dependency", definition.Name)
		}
		if _, exists := seen[dependency]; exists {
			return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q has duplicate dependency %q", definition.Name, dependency)
		}
		seen[dependency] = struct{}{}
		dependencies = append(dependencies, dependency)
	}
	definition.Dependencies = dependencies
	pointLookupFields := make([]string, 0, len(definition.PointLookupFields))
	seenPointLookupFields := make(map[string]struct{}, len(definition.PointLookupFields))
	for _, field := range definition.PointLookupFields {
		field = strings.TrimSpace(field)
		if field == "" {
			return MaterializedViewDefinition{}, fmt.Errorf("materialized view %q point lookup field is required", definition.Name)
		}
		if _, exists := seenPointLookupFields[field]; exists {
			continue
		}
		seenPointLookupFields[field] = struct{}{}
		pointLookupFields = append(pointLookupFields, field)
	}
	definition.PointLookupFields = pointLookupFields
	return definition, nil
}

func materializedViewDependsOn(definition MaterializedViewDefinition, changed map[string]struct{}) bool {
	for _, dependency := range definition.Dependencies {
		if _, exists := changed[dependency]; exists {
			return true
		}
	}
	return false
}

func sameMaterializedViewDefinition(left, right MaterializedViewDefinition) bool {
	if !sameMaterializedViewPointLookupFields(left.PointLookupFields, right.PointLookupFields) {
		return false
	}
	if left.Name != right.Name || left.Query != right.Query || len(left.Dependencies) != len(right.Dependencies) {
		return false
	}
	for index := range left.Dependencies {
		if left.Dependencies[index] != right.Dependencies[index] {
			return false
		}
	}
	return true
}

func cloneMaterializedView(view MaterializedView) MaterializedView {
	return MaterializedView{
		Status: cloneMaterializedViewStatus(view.Status),
		Result: cloneQueryResult(view.Result),
	}
}

func cloneMaterializedViewStatus(status MaterializedViewStatus) MaterializedViewStatus {
	status.Dependencies = append([]string(nil), status.Dependencies...)
	status.IdempotencyKeys = append([]string(nil), status.IdempotencyKeys...)
	return status
}

func normalizeMaterializedViewIdempotencyKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(keys))
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			normalized = append(normalized, key)
		}
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func cloneQueryResult(result QueryResult) QueryResult {
	result.Columns = append([]string(nil), result.Columns...)
	result.Rows = CloneRows(result.Rows)
	result.Plan = cloneMaterializedExplainSteps(result.Plan)
	result.PlanSnapshot = cloneSQLPlanSnapshot(result.PlanSnapshot)
	if result.Stats != nil {
		stats := *result.Stats
		result.Stats = &stats
	}
	return result
}

func cloneMaterializedExplainSteps(steps []ExplainStep) []ExplainStep {
	cloned := make([]ExplainStep, len(steps))
	for index, step := range steps {
		cloned[index] = step
		cloned[index].Arrangements = cloneSQLArrangementMetadata(step.Arrangements)
		cloned[index].EstimatedRows = cloneMaterializedInt(step.EstimatedRows)
		cloned[index].ActualInputRows = cloneMaterializedInt(step.ActualInputRows)
		cloned[index].ActualOutputRows = cloneMaterializedInt(step.ActualOutputRows)
		cloned[index].ActualInputBytes = cloneMaterializedInt(step.ActualInputBytes)
		cloned[index].ActualOutputBytes = cloneMaterializedInt(step.ActualOutputBytes)
		cloned[index].EstimateErrorRows = cloneMaterializedInt(step.EstimateErrorRows)
		cloned[index].EstimateErrorPercent = cloneMaterializedFloat64(step.EstimateErrorPercent)
		cloned[index].ElapsedNanos = cloneMaterializedInt64(step.ElapsedNanos)
		cloned[index].Projection = cloneExplainProjection(step.Projection)
		cloned[index].Pruning = cloneExplainPruning(step.Pruning)
	}
	return cloned
}

func cloneExplainProjection(value *ExplainProjection) *ExplainProjection {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Fields = append([]string(nil), value.Fields...)
	clone.PredicateFields = append([]string(nil), value.PredicateFields...)
	clone.OutputFields = append([]string(nil), value.OutputFields...)
	return &clone
}

func cloneExplainPruning(value *ExplainPruning) *ExplainPruning {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Decisions = append([]ExplainPruningDecision(nil), value.Decisions...)
	return &clone
}

func cloneMaterializedInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneMaterializedInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneMaterializedFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
