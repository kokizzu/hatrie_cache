package hatriecache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

type SQLTimeSeriesOptions struct {
	TimeField     string
	ValueField    string
	Start         time.Time
	End           time.Time
	Interval      time.Duration
	RollingWindow int
}

type SQLTimeSeriesResult struct {
	Buckets []hatDataStructure.TimeSeriesBucket `json:"buckets"`
	Rolling []hatDataStructure.TimeSeriesPoint  `json:"rolling,omitempty"`
}

// QuerySQLTimeSeries evaluates SQL once, converts selected rows to timestamped
// points, then returns contiguous gap-aware buckets and optional rolling means.
func QuerySQLTimeSeries(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, queryOptions SQLQueryOptions, options SQLTimeSeriesOptions) (SQLTimeSeriesResult, error) {
	options.TimeField = strings.TrimSpace(options.TimeField)
	options.ValueField = strings.TrimSpace(options.ValueField)
	if options.TimeField == "" || options.ValueField == "" {
		return SQLTimeSeriesResult{}, fmt.Errorf("hatriecache: SQL time-series time and value fields are required")
	}
	result, err := ExecuteSQLQueryParameters(ctx, source, resolver, parameters, queryOptions)
	if err != nil {
		return SQLTimeSeriesResult{}, err
	}
	points := make([]hatDataStructure.TimeSeriesPoint, len(result.Rows))
	for index, row := range result.Rows {
		timestamp, err := sqlTimeSeriesTimestamp(row[options.TimeField])
		if err != nil {
			return SQLTimeSeriesResult{}, fmt.Errorf("hatriecache: SQL time-series row %d: %w", index+1, err)
		}
		value, ok := sqlNumber(row[options.ValueField])
		if !ok {
			return SQLTimeSeriesResult{}, fmt.Errorf("hatriecache: SQL time-series row %d: value field %q must be numeric", index+1, options.ValueField)
		}
		points[index] = hatDataStructure.TimeSeriesPoint{Time: timestamp, Value: value}
	}
	buckets, err := hatDataStructure.BucketTimeSeries(points, options.Start, options.End, options.Interval)
	if err != nil {
		return SQLTimeSeriesResult{}, err
	}
	out := SQLTimeSeriesResult{Buckets: buckets}
	if options.RollingWindow > 0 {
		out.Rolling, err = hatDataStructure.RollingAverage(points, options.RollingWindow)
		if err != nil {
			return SQLTimeSeriesResult{}, err
		}
	}
	return out, nil
}

func sqlTimeSeriesTimestamp(value interface{}) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return time.Time{}, fmt.Errorf("time field must be RFC3339 timestamp")
		}
		return parsed.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("time field must be RFC3339 timestamp")
	}
}
