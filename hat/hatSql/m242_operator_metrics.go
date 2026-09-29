package hatSql

import (
	"fmt"
	"time"
)

const (
	SQLDataflowOperatorMetricUpdates  = "operator_updates_total"
	SQLDataflowOperatorMetricBatches  = "operator_batches_total"
	SQLDataflowOperatorMetricFrontier = "operator_frontier"
)

// SQLDataflowOperatorObservation is a monotone cumulative observation for one
// dataflow operator. ObservedAt is optional and is used for freshness only.
type SQLDataflowOperatorObservation struct {
	Updates    uint64
	Batches    uint64
	Frontier   uint64
	ObservedAt time.Time
}

// SQLDataflowOperatorSnapshot contains exact operator counters and frontier
// values independent of the catalog's float64 metric representation.
type SQLDataflowOperatorSnapshot struct {
	Updates   uint64
	Batches   uint64
	Frontier  uint64
	UpdatedAt time.Time
}

type sqlDataflowOperatorState struct {
	snapshot SQLDataflowOperatorSnapshot
}

var sqlDataflowOperatorMetricDefinitions = [...]sqlDataflowProgressMetricDefinition{
	{name: SQLDataflowOperatorMetricUpdates, unit: "updates"},
	{name: SQLDataflowOperatorMetricBatches, unit: "batches"},
	{name: SQLDataflowOperatorMetricFrontier, unit: "timestamp"},
}

// ObserveOperator records bounded per-operator update, batch, and frontier
// metrics. The first observation creates the standard metric points; later
// observations update them in place without allocating on the steady path.
func (c *SQLDataflowMetricsCatalog) ObserveOperator(kind SQLDataflowObjectKind, operatorName string, observation SQLDataflowOperatorObservation) error {
	if c == nil {
		return fmt.Errorf("%w: nil catalog", ErrSQLDataflowMetricsInvalid)
	}
	if err := validateSQLDataflowObjectKind(kind); err != nil {
		return err
	}
	if err := validateSQLDataflowText(operatorName, c.options.maxObjectNameBytes, "operator name"); err != nil {
		return err
	}

	key := sqlDataflowMetricsKey{kind: kind, name: operatorName}
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.operators[key]
	if err := validateSQLDataflowOperatorAdvance(previous.snapshot, observation); err != nil {
		return err
	}

	object, exists := c.objects[key]
	if !exists {
		if len(c.objects) >= c.options.maxObjects {
			return fmt.Errorf("%w: objects", ErrSQLDataflowMetricsLimit)
		}
		if len(sqlDataflowOperatorMetricDefinitions) > c.options.maxMetricsPerObject || len(sqlDataflowOperatorMetricDefinitions) > c.options.maxMetricPoints {
			return fmt.Errorf("%w: operator metric points", ErrSQLDataflowMetricsLimit)
		}
		object = SQLDataflowMetricsObject{
			Kind:    kind,
			Name:    operatorName,
			Metrics: make([]SQLDataflowMetricPoint, len(sqlDataflowOperatorMetricDefinitions)),
		}
		for index, definition := range sqlDataflowOperatorMetricDefinitions {
			object.Metrics[index] = SQLDataflowMetricPoint{Name: definition.name, Unit: definition.unit}
		}
		c.metricPoints += len(object.Metrics)
	} else {
		var missing int
		for _, definition := range sqlDataflowOperatorMetricDefinitions {
			if sqlDataflowMetricIndex(object.Metrics, definition.name) < 0 {
				missing++
			}
		}
		if len(object.Metrics)+missing > c.options.maxMetricsPerObject || c.metricPoints+missing > c.options.maxMetricPoints {
			return fmt.Errorf("%w: operator metric points", ErrSQLDataflowMetricsLimit)
		}
		if missing > 0 {
			metrics := make([]SQLDataflowMetricPoint, 0, len(object.Metrics)+missing)
			metrics = append(metrics, object.Metrics...)
			for _, definition := range sqlDataflowOperatorMetricDefinitions {
				if sqlDataflowMetricIndex(metrics, definition.name) < 0 {
					metrics = append(metrics, SQLDataflowMetricPoint{Name: definition.name, Unit: definition.unit})
				}
			}
			object.Metrics = metrics
			c.metricPoints += missing
		}
	}

	snapshot := SQLDataflowOperatorSnapshot{
		Updates:   observation.Updates,
		Batches:   observation.Batches,
		Frontier:  observation.Frontier,
		UpdatedAt: observation.ObservedAt,
	}
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowOperatorMetricUpdates, float64(snapshot.Updates), snapshot.UpdatedAt)
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowOperatorMetricBatches, float64(snapshot.Batches), snapshot.UpdatedAt)
	sqlDataflowUpdateProgressMetric(object.Metrics, SQLDataflowOperatorMetricFrontier, float64(snapshot.Frontier), snapshot.UpdatedAt)
	c.objects[key] = object
	if c.operators == nil {
		c.operators = make(map[sqlDataflowMetricsKey]sqlDataflowOperatorState)
	}
	c.operators[key] = sqlDataflowOperatorState{snapshot: snapshot}
	return nil
}

// Operator returns the latest exact observation for one operator.
func (c *SQLDataflowMetricsCatalog) Operator(kind SQLDataflowObjectKind, operatorName string) (SQLDataflowOperatorSnapshot, error) {
	if c == nil {
		return SQLDataflowOperatorSnapshot{}, fmt.Errorf("%w: nil catalog", ErrSQLDataflowMetricsInvalid)
	}
	if err := validateSQLDataflowObjectKind(kind); err != nil {
		return SQLDataflowOperatorSnapshot{}, err
	}
	if err := validateSQLDataflowText(operatorName, c.options.maxObjectNameBytes, "operator name"); err != nil {
		return SQLDataflowOperatorSnapshot{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	state, exists := c.operators[sqlDataflowMetricsKey{kind: kind, name: operatorName}]
	if !exists {
		return SQLDataflowOperatorSnapshot{}, fmt.Errorf("%w: %s/%s", ErrSQLDataflowMetricsNotFound, kind, operatorName)
	}
	return state.snapshot, nil
}

func validateSQLDataflowOperatorAdvance(previous SQLDataflowOperatorSnapshot, observation SQLDataflowOperatorObservation) error {
	if observation.Updates < previous.Updates || observation.Batches < previous.Batches || observation.Frontier < previous.Frontier {
		return fmt.Errorf("%w: operator counters must be monotone", ErrSQLDataflowMetricsInvalid)
	}
	if !previous.UpdatedAt.IsZero() && !observation.ObservedAt.IsZero() && observation.ObservedAt.Before(previous.UpdatedAt) {
		return fmt.Errorf("%w: operator observation time moved backwards", ErrSQLDataflowMetricsInvalid)
	}
	return nil
}
