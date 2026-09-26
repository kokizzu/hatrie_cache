package hatSql

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// SQLProjectionAdvisorOptions configures the bounded projection advisor.
// Workload forecasting is opt-in because it adds a timestamp and counter
// update to each eligible observed query.
type SQLProjectionAdvisorOptions struct {
	Capacity               int
	EnableWorkloadForecast bool
}

// SQLProjectionWorkloadForecast estimates future query volume for one
// caller-labeled projection candidate. The advisor retains counters and the
// first/last observation timestamps, never individual query events or SQL.
type SQLProjectionWorkloadForecast struct {
	SQLProjectionRecommendation
	ObservedQueries   uint64
	ObservationWindow time.Duration
	ForecastWindow    time.Duration
	ExpectedQueries   uint64
}

type sqlProjectionAdvisorWorkloadStats struct {
	queries           uint64
	firstObservedUnix int64
	lastObservedUnix  int64
	hasObservation    bool
}

// NewSQLProjectionAdvisorWithOptions creates a bounded advisor. A nonpositive
// capacity is inert. Workload forecasting remains disabled unless explicitly
// enabled.
func NewSQLProjectionAdvisorWithOptions(options SQLProjectionAdvisorOptions) *SQLProjectionAdvisor {
	advisor := &SQLProjectionAdvisor{
		capacity: options.Capacity,
		counts:   make(map[sqlProjectionAdvisorKey]sqlProjectionAdvisorStats),
	}
	if options.EnableWorkloadForecast && options.Capacity > 0 {
		advisor.forecastEnabled = true
		advisor.workloads = make(map[sqlProjectionAdvisorKey]sqlProjectionAdvisorWorkloadStats)
	}
	return advisor
}

// RecordWorkload records one successful caller-labeled query observation for
// the forecast. A zero timestamp uses the current time. Calls are ignored
// when forecasting is disabled, the advisor is inert, or the input is empty.
func (advisor *SQLProjectionAdvisor) RecordWorkload(queryID string, dependencies []string, observedAt time.Time) {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	advisor.recordWorkloadWithShape(queryID, dependencies, sqlProjectionAdvisorShape{}, observedAt)
}

// ForecastWorkloadAt projects each bounded workload counter over horizon.
// The timestamp is accepted for deterministic callers and is only used when
// it is zero; no wall-clock state is retained in the forecast.
func (advisor *SQLProjectionAdvisor) ForecastWorkloadAt(now time.Time, horizon time.Duration) ([]SQLProjectionWorkloadForecast, error) {
	if horizon <= 0 {
		return nil, fmt.Errorf("projection workload forecast horizon must be positive")
	}
	if advisor == nil || !advisor.forecastEnabled {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	nowUnix := now.UnixNano()
	advisor.mu.RLock()
	forecasts := make([]SQLProjectionWorkloadForecast, 0, len(advisor.workloads))
	for key, workload := range advisor.workloads {
		if !workload.hasObservation || workload.queries == 0 {
			continue
		}
		if workload.firstObservedUnix > nowUnix {
			continue
		}
		lastObservedUnix := workload.lastObservedUnix
		if lastObservedUnix > nowUnix {
			lastObservedUnix = nowUnix
		}
		observationWindow := sqlProjectionAdvisorObservationWindow(workload.firstObservedUnix, lastObservedUnix)
		expectedQueries := workload.queries
		if observationWindow > 0 {
			expectedQueries = sqlProjectionAdvisorForecastQueries(workload.queries, observationWindow, horizon)
		}
		forecasts = append(forecasts, SQLProjectionWorkloadForecast{
			SQLProjectionRecommendation: sqlProjectionAdvisorRecommendation(key, advisor.counts[key]),
			ObservedQueries:             workload.queries,
			ObservationWindow:           observationWindow,
			ForecastWindow:              horizon,
			ExpectedQueries:             expectedQueries,
		})
	}
	advisor.mu.RUnlock()
	sort.Slice(forecasts, func(left, right int) bool {
		if forecasts[left].ExpectedQueries != forecasts[right].ExpectedQueries {
			return forecasts[left].ExpectedQueries > forecasts[right].ExpectedQueries
		}
		if forecasts[left].QueryID != forecasts[right].QueryID {
			return forecasts[left].QueryID < forecasts[right].QueryID
		}
		leftDependencies := sqlProjectionAdvisorEncodeDependencies(forecasts[left].Dependencies)
		rightDependencies := sqlProjectionAdvisorEncodeDependencies(forecasts[right].Dependencies)
		if leftDependencies != rightDependencies {
			return leftDependencies < rightDependencies
		}
		return sqlProjectionAdvisorEncodeShape(sqlProjectionAdvisorShape{
			fields:        forecasts[left].Fields,
			filterFields:  forecasts[left].FilterFields,
			groupByFields: forecasts[left].GroupByFields,
			orderByFields: forecasts[left].OrderByFields,
		}) < sqlProjectionAdvisorEncodeShape(sqlProjectionAdvisorShape{
			fields:        forecasts[right].Fields,
			filterFields:  forecasts[right].FilterFields,
			groupByFields: forecasts[right].GroupByFields,
			orderByFields: forecasts[right].OrderByFields,
		})
	})
	return forecasts, nil
}

func (advisor *SQLProjectionAdvisor) recordWorkloadWithShape(queryID string, dependencies []string, shape sqlProjectionAdvisorShape, observedAt time.Time) {
	queryID = strings.TrimSpace(queryID)
	if advisor == nil || !advisor.forecastEnabled || advisor.capacity <= 0 || queryID == "" || len(dependencies) == 0 || observedAt.IsZero() {
		return
	}
	key := sqlProjectionAdvisorKey{
		queryID:      queryID,
		dependencies: sqlProjectionAdvisorEncodeDependencies(dependencies),
		shape:        sqlProjectionAdvisorEncodeShape(shape),
	}
	observedUnix := observedAt.UnixNano()
	advisor.mu.Lock()
	defer advisor.mu.Unlock()
	workload, exists := advisor.workloads[key]
	if !exists && len(advisor.workloads) >= advisor.capacity {
		return
	}
	if !workload.hasObservation {
		workload.firstObservedUnix = observedUnix
		workload.lastObservedUnix = observedUnix
		workload.hasObservation = true
	} else {
		if observedUnix < workload.firstObservedUnix {
			workload.firstObservedUnix = observedUnix
		}
		if observedUnix > workload.lastObservedUnix {
			workload.lastObservedUnix = observedUnix
		}
	}
	workload.queries = sqlProjectionAdvisorSaturatingAdd(workload.queries, 1)
	advisor.workloads[key] = workload
}

func sqlProjectionAdvisorObservationWindow(firstUnix, lastUnix int64) time.Duration {
	if lastUnix <= firstUnix {
		return 0
	}
	delta := uint64(lastUnix) - uint64(firstUnix)
	maxDuration := uint64(time.Duration(1<<63 - 1))
	if delta > maxDuration {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(delta)
}

func sqlProjectionAdvisorForecastQueries(observed uint64, observationWindow, forecastWindow time.Duration) uint64 {
	if observed == 0 || observationWindow <= 0 || forecastWindow <= 0 {
		return observed
	}
	window := uint64(observationWindow)
	horizon := uint64(forecastWindow)
	maxUint64 := ^uint64(0)
	if observed > maxUint64/horizon {
		return maxUint64
	}
	product := observed * horizon
	forecast := product / window
	if product%window != 0 && forecast < maxUint64 {
		forecast++
	}
	if forecast == 0 {
		return 1
	}
	return forecast
}
