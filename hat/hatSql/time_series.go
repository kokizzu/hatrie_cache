package hatSql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

// TimeSeriesOptions describes timestamp and numeric columns selected by SQL.
type TimeSeriesOptions struct {
	TimeField     string
	ValueField    string
	Start         time.Time
	End           time.Time
	Interval      time.Duration
	RollingWindow int
}

// TimeSeriesResult contains contiguous buckets and optional rolling averages.
type TimeSeriesResult struct {
	Buckets []hatDataStructure.TimeSeriesBucket `json:"buckets"`
	Rolling []hatDataStructure.TimeSeriesPoint  `json:"rolling,omitempty"`
}

// QueryTimeSeries executes SQL, converts its selected rows to points, then
// returns gap-aware buckets and optional rolling means.
func QueryTimeSeries(ctx context.Context, source string, resolver SourceResolver, parameters []interface{}, queryOptions QueryOptions, options TimeSeriesOptions) (TimeSeriesResult, error) {
	options.TimeField = strings.TrimSpace(options.TimeField)
	options.ValueField = strings.TrimSpace(options.ValueField)
	if options.TimeField == "" || options.ValueField == "" {
		return TimeSeriesResult{}, fmt.Errorf("hatSql: SQL time-series time and value fields are required")
	}
	result, err := ExecuteQueryParameters(ctx, source, resolver, parameters, queryOptions)
	if err != nil {
		return TimeSeriesResult{}, err
	}
	points := make([]hatDataStructure.TimeSeriesPoint, len(result.Rows))
	for index, row := range result.Rows {
		timestamp, err := timeSeriesTimestamp(row[options.TimeField])
		if err != nil {
			return TimeSeriesResult{}, fmt.Errorf("hatSql: SQL time-series row %d: %w", index+1, err)
		}
		value, ok := sqlNumber(row[options.ValueField])
		if !ok {
			return TimeSeriesResult{}, fmt.Errorf("hatSql: SQL time-series row %d: value field %q must be numeric", index+1, options.ValueField)
		}
		points[index] = hatDataStructure.TimeSeriesPoint{Time: timestamp, Value: value}
	}
	buckets, err := hatDataStructure.BucketTimeSeries(points, options.Start, options.End, options.Interval)
	if err != nil {
		return TimeSeriesResult{}, err
	}
	out := TimeSeriesResult{Buckets: buckets}
	if options.RollingWindow > 0 {
		out.Rolling, err = hatDataStructure.RollingAverage(points, options.RollingWindow)
		if err != nil {
			return TimeSeriesResult{}, err
		}
	}
	return out, nil
}

func timeSeriesTimestamp(value interface{}) (time.Time, error) {
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
