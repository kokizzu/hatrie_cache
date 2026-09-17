package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// SQLDataflowObjectKind identifies the part of a SQL dataflow that owns a
// metric.
type SQLDataflowObjectKind string

const (
	SQLDataflowObjectSource  SQLDataflowObjectKind = "source"
	SQLDataflowObjectCompute SQLDataflowObjectKind = "compute"
	SQLDataflowObjectSink    SQLDataflowObjectKind = "sink"
)

const (
	// These defaults keep the catalog useful for a service while bounding
	// scrape and snapshot work when callers do not provide options.
	DefaultSQLDataflowMetricsMaxObjects          = 1024
	DefaultSQLDataflowMetricsMaxMetricsPerObject = 64
	DefaultSQLDataflowMetricsMaxMetricPoints     = 65536
	DefaultSQLDataflowMetricsMaxObjectNameBytes  = 256
	DefaultSQLDataflowMetricsMaxMetricNameBytes  = 128
	DefaultSQLDataflowMetricsMaxUnitBytes        = 32

	maxSQLDataflowMetricsObjects         = 1 << 20
	maxSQLDataflowMetricsPerObject       = 1 << 12
	maxSQLDataflowMetricsPoints          = 1 << 24
	maxSQLDataflowMetricsObjectNameBytes = 1 << 20
	maxSQLDataflowMetricsMetricNameBytes = 1 << 20
	maxSQLDataflowMetricsUnitBytes       = 1 << 20
)

var (
	// ErrSQLDataflowMetricsInvalid indicates malformed catalog input or
	// unsupported catalog options.
	ErrSQLDataflowMetricsInvalid = errors.New("hatSql: invalid dataflow metrics input")
	// ErrSQLDataflowMetricsLimit indicates that a configured catalog bound
	// would be exceeded.
	ErrSQLDataflowMetricsLimit = errors.New("hatSql: dataflow metrics limit exceeded")
	// ErrSQLDataflowMetricsNotFound indicates that a requested object is absent.
	ErrSQLDataflowMetricsNotFound = errors.New("hatSql: dataflow metrics object not found")
)

// SQLDataflowMetricPoint is one numeric measurement for a dataflow object.
// Metric names are deliberately ASCII-only so the rows have predictable SQL
// identifiers and bounded encoding cost.
type SQLDataflowMetricPoint struct {
	Name      string    `json:"name"`
	Unit      string    `json:"unit,omitempty"`
	Value     float64   `json:"value"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// SQLDataflowMetricsObject groups the measurements owned by one source,
// compute stage, or sink.
type SQLDataflowMetricsObject struct {
	Kind    SQLDataflowObjectKind    `json:"kind"`
	Name    string                   `json:"name"`
	Metrics []SQLDataflowMetricPoint `json:"metrics"`
}

// SQLDataflowMetricsCatalogOptions controls cardinality and encoded field
// sizes. Zero values select the documented defaults; negative values are
// rejected.
type SQLDataflowMetricsCatalogOptions struct {
	MaxObjects          int
	MaxMetricsPerObject int
	MaxMetricPoints     int
	MaxObjectNameBytes  int
	MaxMetricNameBytes  int
	MaxUnitBytes        int
}

// SQLDataflowMetricsCatalogStats summarizes the current catalog contents.
type SQLDataflowMetricsCatalogStats struct {
	Objects      int `json:"objects"`
	MetricPoints int `json:"metric_points"`
}

// SQLDataflowMetricsSnapshot is an independent, deterministic catalog view.
type SQLDataflowMetricsSnapshot struct {
	Objects []SQLDataflowMetricsObject `json:"objects"`
}

// SQLDataflowMetricsRow is the SQL-shaped representation of one metric.
type SQLDataflowMetricsRow map[string]any

type sqlDataflowMetricsKey struct {
	kind SQLDataflowObjectKind
	name string
}

type normalizedSQLDataflowMetricsOptions struct {
	maxObjects          int
	maxMetricsPerObject int
	maxMetricPoints     int
	maxObjectNameBytes  int
	maxMetricNameBytes  int
	maxUnitBytes        int
}

// SQLDataflowMetricsCatalog is a concurrency-safe bounded registry for
// source, compute, and sink measurements. Upsert validates and copies a full
// object before publishing it, so readers never observe a partial replacement.
type SQLDataflowMetricsCatalog struct {
	mu           sync.RWMutex
	options      normalizedSQLDataflowMetricsOptions
	objects      map[sqlDataflowMetricsKey]SQLDataflowMetricsObject
	metricPoints int
}

// NewSQLDataflowMetricsCatalog creates an empty bounded metrics catalog.
func NewSQLDataflowMetricsCatalog(options SQLDataflowMetricsCatalogOptions) (*SQLDataflowMetricsCatalog, error) {
	normalized, err := normalizeSQLDataflowMetricsOptions(options)
	if err != nil {
		return nil, err
	}
	return &SQLDataflowMetricsCatalog{
		options: normalized,
		objects: make(map[sqlDataflowMetricsKey]SQLDataflowMetricsObject),
	}, nil
}

// Upsert validates and atomically replaces the metrics for one object.
func (c *SQLDataflowMetricsCatalog) Upsert(object SQLDataflowMetricsObject) error {
	if c == nil {
		return fmt.Errorf("%w: nil catalog", ErrSQLDataflowMetricsInvalid)
	}

	copied, key, err := c.validateAndCopyObject(object)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	previous, exists := c.objects[key]
	if !exists && len(c.objects) >= c.options.maxObjects {
		return fmt.Errorf("%w: objects", ErrSQLDataflowMetricsLimit)
	}
	points := c.metricPoints - len(previous.Metrics) + len(copied.Metrics)
	if points > c.options.maxMetricPoints {
		return fmt.Errorf("%w: metric points", ErrSQLDataflowMetricsLimit)
	}
	c.objects[key] = copied
	c.metricPoints = points
	return nil
}

// UpdateMetric changes an already registered metric in place. Register the
// object shape once with Upsert, then use this allocation-free success path for
// high-frequency producer updates.
func (c *SQLDataflowMetricsCatalog) UpdateMetric(kind SQLDataflowObjectKind, objectName, metricName string, value float64, updatedAt time.Time) error {
	if c == nil {
		return fmt.Errorf("%w: nil catalog", ErrSQLDataflowMetricsInvalid)
	}
	if err := validateSQLDataflowObjectKind(kind); err != nil {
		return err
	}
	if err := validateSQLDataflowText(objectName, c.options.maxObjectNameBytes, "object name"); err != nil {
		return err
	}
	if err := validateSQLDataflowMetricName(metricName, c.options.maxMetricNameBytes); err != nil {
		return err
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("%w: metric %q value is not finite", ErrSQLDataflowMetricsInvalid, metricName)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	object, exists := c.objects[sqlDataflowMetricsKey{kind: kind, name: objectName}]
	if !exists {
		return fmt.Errorf("%w: %s/%s", ErrSQLDataflowMetricsNotFound, kind, objectName)
	}
	for index := range object.Metrics {
		if object.Metrics[index].Name == metricName {
			object.Metrics[index].Value = value
			object.Metrics[index].UpdatedAt = updatedAt
			return nil
		}
	}
	return fmt.Errorf("%w: %s/%s metric %s", ErrSQLDataflowMetricsNotFound, kind, objectName, metricName)
}

// Remove deletes one object from the catalog.
func (c *SQLDataflowMetricsCatalog) Remove(kind SQLDataflowObjectKind, name string) error {
	if c == nil {
		return fmt.Errorf("%w: nil catalog", ErrSQLDataflowMetricsInvalid)
	}
	if err := validateSQLDataflowObjectKind(kind); err != nil {
		return err
	}
	if err := validateSQLDataflowText(name, c.options.maxObjectNameBytes, "object name"); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	key := sqlDataflowMetricsKey{kind: kind, name: name}
	object, exists := c.objects[key]
	if !exists {
		return fmt.Errorf("%w: %s/%s", ErrSQLDataflowMetricsNotFound, kind, name)
	}
	delete(c.objects, key)
	c.metricPoints -= len(object.Metrics)
	return nil
}

// Snapshot returns a deep-copied, deterministically ordered catalog view.
func (c *SQLDataflowMetricsCatalog) Snapshot() SQLDataflowMetricsSnapshot {
	if c == nil {
		return SQLDataflowMetricsSnapshot{}
	}
	c.mu.RLock()
	objects := make([]SQLDataflowMetricsObject, 0, len(c.objects))
	for _, object := range c.objects {
		objects = append(objects, copySQLDataflowMetricsObject(object))
	}
	c.mu.RUnlock()

	sortSQLDataflowMetricsObjects(objects)
	return SQLDataflowMetricsSnapshot{Objects: objects}
}

// Rows returns one independent SQL-shaped row for every metric. The result is
// ordered by source, compute, sink, object name, and metric name.
func (c *SQLDataflowMetricsCatalog) Rows() []SQLDataflowMetricsRow {
	snapshot := c.Snapshot()
	rows := make([]SQLDataflowMetricsRow, 0)
	for _, object := range snapshot.Objects {
		for _, metric := range object.Metrics {
			rows = append(rows, SQLDataflowMetricsRow{
				"object_kind": string(object.Kind),
				"object_name": object.Name,
				"metric":      metric.Name,
				"value":       metric.Value,
				"unit":        metric.Unit,
				"updated_at":  metric.UpdatedAt,
			})
		}
	}
	return rows
}

// Stats returns the current object and metric counts without exposing mutable
// catalog state.
func (c *SQLDataflowMetricsCatalog) Stats() SQLDataflowMetricsCatalogStats {
	if c == nil {
		return SQLDataflowMetricsCatalogStats{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return SQLDataflowMetricsCatalogStats{
		Objects:      len(c.objects),
		MetricPoints: c.metricPoints,
	}
}

func normalizeSQLDataflowMetricsOptions(options SQLDataflowMetricsCatalogOptions) (normalizedSQLDataflowMetricsOptions, error) {
	values := [...]int{
		options.MaxObjects,
		options.MaxMetricsPerObject,
		options.MaxMetricPoints,
		options.MaxObjectNameBytes,
		options.MaxMetricNameBytes,
		options.MaxUnitBytes,
	}
	for _, value := range values {
		if value < 0 {
			return normalizedSQLDataflowMetricsOptions{}, fmt.Errorf("%w: negative option", ErrSQLDataflowMetricsInvalid)
		}
	}
	normalized := normalizedSQLDataflowMetricsOptions{
		maxObjects:          defaultSQLDataflowMetricsOption(options.MaxObjects, DefaultSQLDataflowMetricsMaxObjects),
		maxMetricsPerObject: defaultSQLDataflowMetricsOption(options.MaxMetricsPerObject, DefaultSQLDataflowMetricsMaxMetricsPerObject),
		maxMetricPoints:     defaultSQLDataflowMetricsOption(options.MaxMetricPoints, DefaultSQLDataflowMetricsMaxMetricPoints),
		maxObjectNameBytes:  defaultSQLDataflowMetricsOption(options.MaxObjectNameBytes, DefaultSQLDataflowMetricsMaxObjectNameBytes),
		maxMetricNameBytes:  defaultSQLDataflowMetricsOption(options.MaxMetricNameBytes, DefaultSQLDataflowMetricsMaxMetricNameBytes),
		maxUnitBytes:        defaultSQLDataflowMetricsOption(options.MaxUnitBytes, DefaultSQLDataflowMetricsMaxUnitBytes),
	}
	limits := [...]int{
		normalized.maxObjects,
		normalized.maxMetricsPerObject,
		normalized.maxMetricPoints,
		normalized.maxObjectNameBytes,
		normalized.maxMetricNameBytes,
		normalized.maxUnitBytes,
	}
	maximums := [...]int{
		maxSQLDataflowMetricsObjects,
		maxSQLDataflowMetricsPerObject,
		maxSQLDataflowMetricsPoints,
		maxSQLDataflowMetricsObjectNameBytes,
		maxSQLDataflowMetricsMetricNameBytes,
		maxSQLDataflowMetricsUnitBytes,
	}
	for index, value := range limits {
		if value > maximums[index] {
			return normalizedSQLDataflowMetricsOptions{}, fmt.Errorf("%w: option %d is too large", ErrSQLDataflowMetricsInvalid, index)
		}
	}
	return normalized, nil
}

func defaultSQLDataflowMetricsOption(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func (c *SQLDataflowMetricsCatalog) validateAndCopyObject(object SQLDataflowMetricsObject) (SQLDataflowMetricsObject, sqlDataflowMetricsKey, error) {
	if err := validateSQLDataflowObjectKind(object.Kind); err != nil {
		return SQLDataflowMetricsObject{}, sqlDataflowMetricsKey{}, err
	}
	if err := validateSQLDataflowText(object.Name, c.options.maxObjectNameBytes, "object name"); err != nil {
		return SQLDataflowMetricsObject{}, sqlDataflowMetricsKey{}, err
	}
	if len(object.Metrics) > c.options.maxMetricsPerObject {
		return SQLDataflowMetricsObject{}, sqlDataflowMetricsKey{}, fmt.Errorf("%w: metrics per object", ErrSQLDataflowMetricsLimit)
	}

	metrics := make([]SQLDataflowMetricPoint, len(object.Metrics))
	seen := make(map[string]struct{}, len(object.Metrics))
	for index, metric := range object.Metrics {
		if err := validateSQLDataflowMetricName(metric.Name, c.options.maxMetricNameBytes); err != nil {
			return SQLDataflowMetricsObject{}, sqlDataflowMetricsKey{}, err
		}
		if metric.Unit != "" {
			if err := validateSQLDataflowText(metric.Unit, c.options.maxUnitBytes, "metric unit"); err != nil {
				return SQLDataflowMetricsObject{}, sqlDataflowMetricsKey{}, err
			}
		}
		if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
			return SQLDataflowMetricsObject{}, sqlDataflowMetricsKey{}, fmt.Errorf("%w: metric %q value is not finite", ErrSQLDataflowMetricsInvalid, metric.Name)
		}
		if _, exists := seen[metric.Name]; exists {
			return SQLDataflowMetricsObject{}, sqlDataflowMetricsKey{}, fmt.Errorf("%w: duplicate metric %q", ErrSQLDataflowMetricsInvalid, metric.Name)
		}
		seen[metric.Name] = struct{}{}
		metrics[index] = metric
	}
	return SQLDataflowMetricsObject{
		Kind:    object.Kind,
		Name:    object.Name,
		Metrics: metrics,
	}, sqlDataflowMetricsKey{kind: object.Kind, name: object.Name}, nil
}

func validateSQLDataflowObjectKind(kind SQLDataflowObjectKind) error {
	switch kind {
	case SQLDataflowObjectSource, SQLDataflowObjectCompute, SQLDataflowObjectSink:
		return nil
	default:
		return fmt.Errorf("%w: unknown object kind %q", ErrSQLDataflowMetricsInvalid, kind)
	}
}

func validateSQLDataflowMetricName(name string, maxBytes int) error {
	if err := validateSQLDataflowText(name, maxBytes, "metric name"); err != nil {
		return err
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '-' || character == '.' {
			continue
		}
		return fmt.Errorf("%w: metric name %q contains unsupported character", ErrSQLDataflowMetricsInvalid, name)
	}
	return nil
}

func validateSQLDataflowText(value string, maxBytes int, field string) error {
	if value == "" {
		return fmt.Errorf("%w: empty %s", ErrSQLDataflowMetricsInvalid, field)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%w: %s is too long", ErrSQLDataflowMetricsLimit, field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrSQLDataflowMetricsInvalid, field)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: %s contains a control character", ErrSQLDataflowMetricsInvalid, field)
		}
	}
	return nil
}

func copySQLDataflowMetricsObject(object SQLDataflowMetricsObject) SQLDataflowMetricsObject {
	object.Metrics = append([]SQLDataflowMetricPoint(nil), object.Metrics...)
	return object
}

func sortSQLDataflowMetricsObjects(objects []SQLDataflowMetricsObject) {
	sort.Slice(objects, func(left, right int) bool {
		leftRank := sqlDataflowObjectKindRank(objects[left].Kind)
		rightRank := sqlDataflowObjectKindRank(objects[right].Kind)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return objects[left].Name < objects[right].Name
	})
	for index := range objects {
		sort.Slice(objects[index].Metrics, func(left, right int) bool {
			return objects[index].Metrics[left].Name < objects[index].Metrics[right].Name
		})
	}
}

func sqlDataflowObjectKindRank(kind SQLDataflowObjectKind) int {
	switch kind {
	case SQLDataflowObjectSource:
		return 0
	case SQLDataflowObjectCompute:
		return 1
	case SQLDataflowObjectSink:
		return 2
	default:
		return 3
	}
}
