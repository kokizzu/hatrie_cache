package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrJobAlreadyRunning = errors.New("scheduled job is already running")

// JobSchedule controls the fixed interval at which a job becomes due.
type JobSchedule struct {
	Every time.Duration
}

// JobDestination names an in-memory result destination. Empty destinations
// are valid for jobs that only refresh or validate data.
type JobDestination struct {
	Name string
}

// JobCondition runs before the main query. RequireRows skips the main query
// unless the condition query finds at least one row.
type JobCondition struct {
	Query       string
	Parameters  []interface{}
	RequireRows bool
}

// JobDefinition is an immutable scheduled-query declaration after Create.
type JobDefinition struct {
	Name        string
	Query       string
	Parameters  []interface{}
	Destination JobDestination
	Schedule    JobSchedule
	Condition   *JobCondition
}

// JobRunStatus records the outcome of one run attempt.
type JobRunStatus string

const (
	JobRunSucceeded      JobRunStatus = "succeeded"
	JobRunSkipped        JobRunStatus = "skipped"
	JobRunFailed         JobRunStatus = "failed"
	JobRunAlreadyRunning JobRunStatus = "already_running"
)

// JobOutput summarizes a query result without retaining result rows in run
// history. Result rows, when requested, live only in the named destination.
type JobOutput struct {
	Rows    int
	Columns int
	Bytes   int
}

// JobRun is the bounded diagnostic history record for a job execution.
type JobRun struct {
	ID              uint64
	JobName         string
	StartedAt       time.Time
	FinishedAt      time.Time
	Duration        time.Duration
	Status          JobRunStatus
	SkipReason      string
	Output          JobOutput
	Plan            []ExplainStep
	ErrorDiagnostic string
}

// JobSchedulerOptions controls bounded scheduler state. Zero history capacity
// uses the conservative default; negative values are invalid.
type JobSchedulerOptions struct {
	Now                func() time.Time
	RunHistoryCapacity int
}

// JobScheduler stores scheduled SQL definitions, named result snapshots, and
// bounded execution diagnostics. It is safe for concurrent callers.
type JobScheduler struct {
	mu           sync.RWMutex
	resolver     SourceResolver
	queryOpts    QueryOptions
	now          func() time.Time
	historyCap   int
	jobs         map[string]scheduledJob
	destinations map[string]QueryResult
	history      []JobRun
	nextRunID    uint64
	runnerLive   bool
}

type scheduledJob struct {
	definition JobDefinition
	nextRun    time.Time
	lease      *jobExecutionLease
}

type jobExecutionLease struct {
	id         uint64
	acquiredAt time.Time
}

const defaultJobRunHistoryCapacity = 128

// NewJobScheduler creates an empty scheduler that executes definitions against
// resolver using the supplied bounded query options.
func NewJobScheduler(resolver SourceResolver, options QueryOptions, schedulerOptions JobSchedulerOptions) (*JobScheduler, error) {
	if schedulerOptions.RunHistoryCapacity < 0 {
		return nil, fmt.Errorf("job run history capacity cannot be negative")
	}
	if schedulerOptions.Now == nil {
		schedulerOptions.Now = func() time.Time { return time.Now().UTC() }
	}
	if schedulerOptions.RunHistoryCapacity == 0 {
		schedulerOptions.RunHistoryCapacity = defaultJobRunHistoryCapacity
	}
	return &JobScheduler{
		resolver:     resolver,
		queryOpts:    options,
		now:          schedulerOptions.Now,
		historyCap:   schedulerOptions.RunHistoryCapacity,
		jobs:         make(map[string]scheduledJob),
		destinations: make(map[string]QueryResult),
	}, nil
}

// Create validates and stores a scheduled query. A new job is immediately due
// so callers can validate it with RunDue without waiting for one full interval.
func (scheduler *JobScheduler) Create(definition JobDefinition) error {
	if scheduler == nil {
		return fmt.Errorf("job scheduler is nil")
	}
	definition, err := normalizeJobDefinition(definition)
	if err != nil {
		return err
	}
	now := scheduler.now()
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if _, exists := scheduler.jobs[definition.Name]; exists {
		return fmt.Errorf("job %q already exists", definition.Name)
	}
	scheduler.jobs[definition.Name] = scheduledJob{definition: definition, nextRun: now}
	return nil
}

// Destination returns an independent result snapshot published by a job.
func (scheduler *JobScheduler) Destination(name string) (QueryResult, bool) {
	if scheduler == nil {
		return QueryResult{}, false
	}
	scheduler.mu.RLock()
	result, exists := scheduler.destinations[strings.TrimSpace(name)]
	scheduler.mu.RUnlock()
	if !exists {
		return QueryResult{}, false
	}
	return cloneQueryResult(result), true
}

// History returns stable oldest-first run diagnostics.
func (scheduler *JobScheduler) History() []JobRun {
	if scheduler == nil {
		return nil
	}
	scheduler.mu.RLock()
	history := make([]JobRun, len(scheduler.history))
	for index := range scheduler.history {
		history[index] = cloneJobRun(scheduler.history[index])
	}
	scheduler.mu.RUnlock()
	return history
}

// RunDue executes all currently due jobs in deterministic name order. A failed
// job is included in the returned runs and returned as an error after its
// diagnostic has been recorded.
func (scheduler *JobScheduler) RunDue(ctx context.Context) ([]JobRun, error) {
	if scheduler == nil {
		return nil, fmt.Errorf("job scheduler is nil")
	}
	now := scheduler.now()
	scheduler.mu.RLock()
	names := make([]string, 0, len(scheduler.jobs))
	for name, job := range scheduler.jobs {
		if !job.nextRun.After(now) {
			names = append(names, name)
		}
	}
	scheduler.mu.RUnlock()
	sort.Strings(names)
	runs := make([]JobRun, 0, len(names))
	for _, name := range names {
		run, err := scheduler.Run(ctx, name)
		runs = append(runs, run)
		if err != nil && !errors.Is(err, ErrJobAlreadyRunning) {
			return runs, err
		}
	}
	return runs, nil
}

// Run executes one job immediately. Its execution lease covers condition
// evaluation, the main query, destination publication, and history recording.
func (scheduler *JobScheduler) Run(ctx context.Context, name string) (JobRun, error) {
	if scheduler == nil {
		return JobRun{}, fmt.Errorf("job scheduler is nil")
	}
	name = strings.TrimSpace(name)
	startedAt := scheduler.now()
	definition, lease, duplicate, err := scheduler.acquire(name, startedAt)
	if err != nil {
		return JobRun{}, err
	}
	if duplicate {
		run := JobRun{ID: lease.id, JobName: name, StartedAt: startedAt, FinishedAt: startedAt, Status: JobRunAlreadyRunning, ErrorDiagnostic: ErrJobAlreadyRunning.Error()}
		scheduler.record(run)
		return run, ErrJobAlreadyRunning
	}

	started := time.Now()
	run := JobRun{ID: lease.id, JobName: definition.Name, StartedAt: startedAt}
	if definition.Condition != nil {
		conditionResult, conditionPlan, conditionErr := scheduler.execute(ctx, definition.Condition.Query, definition.Condition.Parameters, lease.id, "condition")
		run.Plan = conditionPlan
		if conditionErr != nil {
			run.Status = JobRunFailed
			run.ErrorDiagnostic = conditionErr.Error()
			return scheduler.finish(run, definition, started, nil), conditionErr
		}
		if definition.Condition.RequireRows && len(conditionResult.Rows) == 0 {
			run.Status = JobRunSkipped
			run.SkipReason = "condition returned no rows"
			return scheduler.finish(run, definition, started, nil), nil
		}
	}

	result, queryPlan, queryErr := scheduler.execute(ctx, definition.Query, definition.Parameters, lease.id, "query")
	run.Plan = queryPlan
	if queryErr != nil {
		run.Status = JobRunFailed
		run.ErrorDiagnostic = queryErr.Error()
		return scheduler.finish(run, definition, started, nil), queryErr
	}
	run.Status = JobRunSucceeded
	run.Output = summarizeJobOutput(result)
	return scheduler.finish(run, definition, started, &result), nil
}

// Start polls due work until ctx is canceled. Only one polling loop may run
// per scheduler; callers still retain the explicit RunDue API for controlled
// maintenance environments and deterministic tests.
func (scheduler *JobScheduler) Start(ctx context.Context, interval time.Duration) error {
	if scheduler == nil {
		return fmt.Errorf("job scheduler is nil")
	}
	if interval <= 0 {
		return fmt.Errorf("job scheduler poll interval must be positive")
	}
	scheduler.mu.Lock()
	if scheduler.runnerLive {
		scheduler.mu.Unlock()
		return fmt.Errorf("job scheduler is already started")
	}
	scheduler.runnerLive = true
	scheduler.mu.Unlock()
	go func() {
		defer func() {
			scheduler.mu.Lock()
			scheduler.runnerLive = false
			scheduler.mu.Unlock()
		}()
		_, _ = scheduler.RunDue(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = scheduler.RunDue(ctx)
			}
		}
	}()
	return nil
}

func (scheduler *JobScheduler) acquire(name string, now time.Time) (JobDefinition, jobExecutionLease, bool, error) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	job, exists := scheduler.jobs[name]
	if !exists {
		return JobDefinition{}, jobExecutionLease{}, false, fmt.Errorf("job %q does not exist", name)
	}
	if job.lease != nil {
		return JobDefinition{}, *job.lease, true, nil
	}
	scheduler.nextRunID++
	lease := jobExecutionLease{id: scheduler.nextRunID, acquiredAt: now}
	job.lease = &lease
	job.nextRun = nextJobRun(job.nextRun, job.definition.Schedule.Every, now)
	scheduler.jobs[name] = job
	return cloneJobDefinition(job.definition), lease, false, nil
}

func (scheduler *JobScheduler) execute(ctx context.Context, source string, parameters []interface{}, runID uint64, stage string) (QueryResult, []ExplainStep, error) {
	options := scheduler.queryOpts
	options.QueryID = fmt.Sprintf("job:%d:%s", runID, stage)
	capture := &jobPlanCapture{next: options.Observer}
	options.Observer = capture
	result, err := ExecuteQueryParameters(ctx, source, scheduler.resolver, cloneJobParameters(parameters), options)
	plan := capture.plan()
	if len(plan) == 0 {
		plan = cloneMaterializedExplainSteps(result.Plan)
	}
	return result, plan, err
}

func (scheduler *JobScheduler) finish(run JobRun, definition JobDefinition, started time.Time, result *QueryResult) JobRun {
	run.Duration = time.Since(started)
	run.FinishedAt = scheduler.now()
	scheduler.mu.Lock()
	if result != nil && definition.Destination.Name != "" && run.Status == JobRunSucceeded {
		scheduler.destinations[definition.Destination.Name] = cloneQueryResult(*result)
	}
	job := scheduler.jobs[definition.Name]
	if job.lease != nil && job.lease.id == run.ID {
		job.lease = nil
		scheduler.jobs[definition.Name] = job
	}
	scheduler.appendHistoryLocked(run)
	scheduler.mu.Unlock()
	return cloneJobRun(run)
}

func (scheduler *JobScheduler) record(run JobRun) {
	scheduler.mu.Lock()
	scheduler.appendHistoryLocked(run)
	scheduler.mu.Unlock()
}

func (scheduler *JobScheduler) appendHistoryLocked(run JobRun) {
	if scheduler.historyCap <= 0 {
		return
	}
	run = cloneJobRun(run)
	if len(scheduler.history) == scheduler.historyCap {
		copy(scheduler.history, scheduler.history[1:])
		scheduler.history[len(scheduler.history)-1] = run
		return
	}
	scheduler.history = append(scheduler.history, run)
}

func normalizeJobDefinition(definition JobDefinition) (JobDefinition, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.Query = strings.TrimSpace(definition.Query)
	definition.Destination.Name = strings.TrimSpace(definition.Destination.Name)
	if definition.Name == "" {
		return JobDefinition{}, fmt.Errorf("job name is required")
	}
	if definition.Query == "" {
		return JobDefinition{}, fmt.Errorf("job %q query is required", definition.Name)
	}
	if definition.Schedule.Every <= 0 {
		return JobDefinition{}, fmt.Errorf("job %q schedule interval must be positive", definition.Name)
	}
	definition.Parameters = cloneJobParameters(definition.Parameters)
	if definition.Condition != nil {
		condition := *definition.Condition
		condition.Query = strings.TrimSpace(condition.Query)
		if condition.Query == "" {
			return JobDefinition{}, fmt.Errorf("job %q condition query is required", definition.Name)
		}
		condition.Parameters = cloneJobParameters(condition.Parameters)
		definition.Condition = &condition
	}
	return definition, nil
}

func nextJobRun(previous time.Time, every time.Duration, now time.Time) time.Time {
	if previous.IsZero() {
		previous = now
	}
	for !previous.After(now) {
		previous = previous.Add(every)
	}
	return previous
}

func summarizeJobOutput(result QueryResult) JobOutput {
	output := JobOutput{Rows: len(result.Rows), Columns: len(result.Columns)}
	if result.Stats != nil {
		output.Rows = result.Stats.OutputRows
		output.Columns = result.Stats.OutputColumns
		output.Bytes = result.Stats.ResultBytes
	}
	return output
}

func cloneJobDefinition(definition JobDefinition) JobDefinition {
	definition.Parameters = cloneJobParameters(definition.Parameters)
	if definition.Condition != nil {
		condition := *definition.Condition
		condition.Parameters = cloneJobParameters(condition.Parameters)
		definition.Condition = &condition
	}
	return definition
}

func cloneJobParameters(parameters []interface{}) []interface{} {
	if len(parameters) == 0 {
		return nil
	}
	cloned := make([]interface{}, len(parameters))
	for index, parameter := range parameters {
		cloned[index] = cloneJobParameter(parameter)
	}
	return cloned
}

func cloneJobParameter(parameter interface{}) interface{} {
	switch value := parameter.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case []interface{}:
		return cloneJobParameters(value)
	case map[string]interface{}:
		cloned := make(map[string]interface{}, len(value))
		for key, item := range value {
			cloned[key] = cloneJobParameter(item)
		}
		return cloned
	default:
		return value
	}
}

func cloneJobRun(run JobRun) JobRun {
	run.Plan = cloneMaterializedExplainSteps(run.Plan)
	return run
}

type jobPlanCapture struct {
	mu        sync.Mutex
	next      QueryObserver
	operators []QueryOperator
}

func (capture *jobPlanCapture) ObserveSQLQuery(event QueryEvent) {
	capture.mu.Lock()
	capture.operators = cloneSQLQueryOperators(event.Operators)
	capture.mu.Unlock()
	if capture.next != nil {
		capture.next.ObserveSQLQuery(event)
	}
}

func (capture *jobPlanCapture) plan() []ExplainStep {
	capture.mu.Lock()
	operators := cloneSQLQueryOperators(capture.operators)
	capture.mu.Unlock()
	plan := make([]ExplainStep, len(operators))
	for index, operator := range operators {
		plan[index] = ExplainStep{
			Node:                 operator.Node,
			EstimatedRows:        cloneJobInt(operator.EstimatedRows),
			ActualInputRows:      jobIntPointer(operator.InputRows),
			ActualOutputRows:     jobIntPointer(operator.OutputRows),
			ActualInputBytes:     cloneJobInt(operator.InputBytes),
			ActualOutputBytes:    cloneJobInt(operator.OutputBytes),
			EstimateErrorPercent: cloneJobFloat64(operator.EstimateErrorPercent),
			ElapsedNanos:         jobInt64Pointer(operator.ElapsedNanos),
		}
	}
	return plan
}

func cloneJobInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneJobFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func jobIntPointer(value int) *int { return &value }

func jobInt64Pointer(value int64) *int64 { return &value }
