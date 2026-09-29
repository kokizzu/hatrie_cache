package hatSql

import (
	"fmt"
	"time"
)

const (
	SQLDataflowProgressMetricInputTimestamp            = "input_timestamp"
	SQLDataflowProgressMetricOutputTimestamp           = "output_timestamp"
	SQLDataflowProgressMetricTimestampLag              = "input_output_timestamp_lag"
	SQLDataflowProgressMetricInputTimestampThroughput  = "input_timestamp_throughput"
	SQLDataflowProgressMetricOutputTimestampThroughput = "output_timestamp_throughput"
	SQLDataflowProgressMetricInputToOutputLatency      = "input_to_output_latency"
)

// SQLDataflowProgressObservation is a monotone pair of input and output
// frontier observations. Timestamps are logical dataflow timestamps. The
// wall-clock fields are optional and are used only for rate and latency
// calculations.
type SQLDataflowProgressObservation struct {
	InputTimestamp   uint64
	OutputTimestamp  uint64
	InputObservedAt  time.Time
	OutputObservedAt time.Time
}

// SQLDataflowProgressSnapshot is the exact frontier state and its derived
// monitoring values for one dataflow object.
type SQLDataflowProgressSnapshot struct {
	InputTimestamp            uint64
	OutputTimestamp           uint64
	TimestampLag              uint64
	InputTimestampThroughput  float64
	OutputTimestampThroughput float64
	InputToOutputLatency      time.Duration
	InputObservedAt           time.Time
	OutputObservedAt          time.Time
}

type sqlDataflowProgressState struct {
	snapshot SQLDataflowProgressSnapshot
}

type sqlDataflowProgressMetricDefinition struct {
	name string
	unit string
}

var sqlDataflowProgressMetricDefinitions = [...]sqlDataflowProgressMetricDefinition{
	{name: SQLDataflowProgressMetricInputTimestamp, unit: "timestamp"},
	{name: SQLDataflowProgressMetricOutputTimestamp, unit: "timestamp"},
	{name: SQLDataflowProgressMetricTimestampLag, unit: "timestamp"},
	{name: SQLDataflowProgressMetricInputTimestampThroughput, unit: "timestamp/s"},
	{name: SQLDataflowProgressMetricOutputTimestampThroughput, unit: "timestamp/s"},
	{name: SQLDataflowProgressMetricInputToOutputLatency, unit: "s"},
}

// ObserveProgress records source and result frontiers for a dataflow object.
// The first observation creates a bounded standard metric object. Subsequent
// observations update the existing metric slots in place and keep the exact
// uint64 values available through Progress.
func (c *SQLDataflowMetricsCatalog) ObserveProgress(kind SQLDataflowObjectKind, objectName string, observation SQLDataflowProgressObservation) error {
	if c == nil {
		return fmt.Errorf("%w: nil catalog", ErrSQLDataflowMetricsInvalid)
	}
	if err := validateSQLDataflowObjectKind(kind); err != nil {
		return err
	}
	if err := validateSQLDataflowText(objectName, c.options.maxObjectNameBytes, "object name"); err != nil {
		return err
	}

	key := sqlDataflowMetricsKey{kind: kind, name: objectName}
	c.mu.Lock()
	defer c.mu.Unlock()
	previous, hasPrevious := c.progress[key]
	if err := validateSQLDataflowProgressAdvance(previous.snapshot, observation); err != nil {
		return err
	}

	object, exists := c.objects[key]
	if !exists {
		if len(c.objects) >= c.options.maxObjects {
			return fmt.Errorf("%w: objects", ErrSQLDataflowMetricsLimit)
		}
		if len(sqlDataflowProgressMetricDefinitions) > c.options.maxMetricsPerObject || len(sqlDataflowProgressMetricDefinitions) > c.options.maxMetricPoints {
			return fmt.Errorf("%w: progress metric points", ErrSQLDataflowMetricsLimit)
		}
		object = SQLDataflowMetricsObject{
			Kind:    kind,
			Name:    objectName,
			Metrics: make([]SQLDataflowMetricPoint, len(sqlDataflowProgressMetricDefinitions)),
		}
		for index, definition := range sqlDataflowProgressMetricDefinitions {
			object.Metrics[index] = SQLDataflowMetricPoint{Name: definition.name, Unit: definition.unit}
		}
		c.metricPoints += len(object.Metrics)
	} else {
		var missing int
		for _, definition := range sqlDataflowProgressMetricDefinitions {
			if sqlDataflowMetricIndex(object.Metrics, definition.name) < 0 {
				missing++
			}
		}
		if len(object.Metrics)+missing > c.options.maxMetricsPerObject || c.metricPoints+missing > c.options.maxMetricPoints {
			return fmt.Errorf("%w: progress metric points", ErrSQLDataflowMetricsLimit)
		}
		if missing > 0 {
			metrics := make([]SQLDataflowMetricPoint, 0, len(object.Metrics)+missing)
			metrics = append(metrics, object.Metrics...)
			for _, definition := range sqlDataflowProgressMetricDefinitions {
				if sqlDataflowMetricIndex(metrics, definition.name) < 0 {
					metrics = append(metrics, SQLDataflowMetricPoint{Name: definition.name, Unit: definition.unit})
				}
			}
			object.Metrics = metrics
			c.metricPoints += missing
		}
	}

	snapshot := sqlDataflowProgressSnapshot(previous.snapshot, hasPrevious, observation)
	updatedAt := observation.OutputObservedAt
	if updatedAt.IsZero() {
		updatedAt = observation.InputObservedAt
	}
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowProgressMetricInputTimestamp, float64(observation.InputTimestamp), updatedAt)
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowProgressMetricOutputTimestamp, float64(observation.OutputTimestamp), updatedAt)
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowProgressMetricTimestampLag, float64(snapshot.TimestampLag), updatedAt)
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowProgressMetricInputTimestampThroughput, snapshot.InputTimestampThroughput, updatedAt)
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowProgressMetricOutputTimestampThroughput, snapshot.OutputTimestampThroughput, updatedAt)
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowProgressMetricInputToOutputLatency, snapshot.InputToOutputLatency.Seconds(), updatedAt)
	c.objects[key] = object
	if c.progress == nil {
		c.progress = make(map[sqlDataflowMetricsKey]sqlDataflowProgressState)
	}
	c.progress[key] = sqlDataflowProgressState{snapshot: snapshot}
	return nil
}

// Progress returns the latest exact progress snapshot for one object.
func (c *SQLDataflowMetricsCatalog) Progress(kind SQLDataflowObjectKind, objectName string) (SQLDataflowProgressSnapshot, error) {
	if c == nil {
		return SQLDataflowProgressSnapshot{}, fmt.Errorf("%w: nil catalog", ErrSQLDataflowMetricsInvalid)
	}
	if err := validateSQLDataflowObjectKind(kind); err != nil {
		return SQLDataflowProgressSnapshot{}, err
	}
	if err := validateSQLDataflowText(objectName, c.options.maxObjectNameBytes, "object name"); err != nil {
		return SQLDataflowProgressSnapshot{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	state, exists := c.progress[sqlDataflowMetricsKey{kind: kind, name: objectName}]
	if !exists {
		return SQLDataflowProgressSnapshot{}, fmt.Errorf("%w: %s/%s", ErrSQLDataflowMetricsNotFound, kind, objectName)
	}
	return state.snapshot, nil
}

func validateSQLDataflowProgressAdvance(previous SQLDataflowProgressSnapshot, observation SQLDataflowProgressObservation) error {
	if observation.InputTimestamp < previous.InputTimestamp || observation.OutputTimestamp < previous.OutputTimestamp {
		return fmt.Errorf("%w: progress timestamps must be monotone", ErrSQLDataflowMetricsInvalid)
	}
	if !previous.InputObservedAt.IsZero() && !observation.InputObservedAt.IsZero() && observation.InputObservedAt.Before(previous.InputObservedAt) {
		return fmt.Errorf("%w: input observation time moved backwards", ErrSQLDataflowMetricsInvalid)
	}
	if !previous.OutputObservedAt.IsZero() && !observation.OutputObservedAt.IsZero() && observation.OutputObservedAt.Before(previous.OutputObservedAt) {
		return fmt.Errorf("%w: output observation time moved backwards", ErrSQLDataflowMetricsInvalid)
	}
	if !observation.InputObservedAt.IsZero() && !observation.OutputObservedAt.IsZero() && observation.OutputObservedAt.Before(observation.InputObservedAt) {
		return fmt.Errorf("%w: output observed before input", ErrSQLDataflowMetricsInvalid)
	}
	return nil
}

func sqlDataflowProgressSnapshot(previous SQLDataflowProgressSnapshot, hasPrevious bool, observation SQLDataflowProgressObservation) SQLDataflowProgressSnapshot {
	snapshot := SQLDataflowProgressSnapshot{
		InputTimestamp:   observation.InputTimestamp,
		OutputTimestamp:  observation.OutputTimestamp,
		InputObservedAt:  observation.InputObservedAt,
		OutputObservedAt: observation.OutputObservedAt,
	}
	if snapshot.InputTimestamp >= snapshot.OutputTimestamp {
		snapshot.TimestampLag = snapshot.InputTimestamp - snapshot.OutputTimestamp
	}
	if hasPrevious {
		if elapsed := observation.InputObservedAt.Sub(previous.InputObservedAt); elapsed > 0 && !observation.InputObservedAt.IsZero() && !previous.InputObservedAt.IsZero() {
			snapshot.InputTimestampThroughput = float64(observation.InputTimestamp-previous.InputTimestamp) / elapsed.Seconds()
		}
		if elapsed := observation.OutputObservedAt.Sub(previous.OutputObservedAt); elapsed > 0 && !observation.OutputObservedAt.IsZero() && !previous.OutputObservedAt.IsZero() {
			snapshot.OutputTimestampThroughput = float64(observation.OutputTimestamp-previous.OutputTimestamp) / elapsed.Seconds()
		}
	}
	if !observation.InputObservedAt.IsZero() && !observation.OutputObservedAt.IsZero() {
		snapshot.InputToOutputLatency = observation.OutputObservedAt.Sub(observation.InputObservedAt)
	}
	return snapshot
}

func sqlDataflowMetricIndex(metrics []SQLDataflowMetricPoint, name string) int {
	for index := range metrics {
		if metrics[index].Name == name {
			return index
		}
	}
	return -1
}

func sqlDataflowUpdateProgressMetric(metrics []SQLDataflowMetricPoint, name string, value float64, updatedAt time.Time) {
	index := sqlDataflowMetricIndex(metrics, name)
	if index < 0 {
		return
	}
	metrics[index].Value = value
	metrics[index].UpdatedAt = updatedAt
}
