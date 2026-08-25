package hatDataStructure

import (
	"errors"
	"math"
	"sort"
	"time"
)

// TimeSeriesPoint is one timestamped numeric observation.
type TimeSeriesPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

// TimeSeriesBucket summarizes all observations in one half-open UTC range.
// Count zero denotes a gap, with Sum and Average set to zero.
type TimeSeriesBucket struct {
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Count   int       `json:"count"`
	Sum     float64   `json:"sum"`
	Average float64   `json:"average"`
}

// BucketTimeSeries builds contiguous [start,end) buckets, including empty
// gaps. Points outside the requested range are ignored.
func BucketTimeSeries(points []TimeSeriesPoint, start, end time.Time, interval time.Duration) ([]TimeSeriesBucket, error) {
	if interval <= 0 {
		return nil, errors.New("hatriecache: time-series bucket interval must be positive")
	}
	start, end = start.UTC(), end.UTC()
	if !end.After(start) {
		return nil, errors.New("hatriecache: time-series end must follow start")
	}
	count := int((end.Sub(start) + interval - 1) / interval)
	buckets := make([]TimeSeriesBucket, count)
	for index := range buckets {
		bucketStart := start.Add(time.Duration(index) * interval)
		bucketEnd := bucketStart.Add(interval)
		if bucketEnd.After(end) {
			bucketEnd = end
		}
		buckets[index] = TimeSeriesBucket{Start: bucketStart, End: bucketEnd}
	}
	for _, point := range points {
		point.Time = point.Time.UTC()
		if point.Time.Before(start) || !point.Time.Before(end) {
			continue
		}
		if math.IsNaN(point.Value) || math.IsInf(point.Value, 0) {
			return nil, errors.New("hatriecache: time-series values must be finite")
		}
		index := int(point.Time.Sub(start) / interval)
		buckets[index].Count++
		buckets[index].Sum += point.Value
	}
	for index := range buckets {
		if buckets[index].Count > 0 {
			buckets[index].Average = buckets[index].Sum / float64(buckets[index].Count)
		}
	}
	return buckets, nil
}

// RollingAverage returns a sorted copy of points whose values are replaced by
// the trailing observation-window average, including the current point.
func RollingAverage(points []TimeSeriesPoint, window int) ([]TimeSeriesPoint, error) {
	if window <= 0 {
		return nil, errors.New("hatriecache: time-series rolling window must be positive")
	}
	raw := append([]TimeSeriesPoint(nil), points...)
	sort.SliceStable(raw, func(left, right int) bool { return raw[left].Time.Before(raw[right].Time) })
	out := append([]TimeSeriesPoint(nil), raw...)
	sum := 0.0
	for index := range raw {
		if math.IsNaN(raw[index].Value) || math.IsInf(raw[index].Value, 0) {
			return nil, errors.New("hatriecache: time-series values must be finite")
		}
		sum += raw[index].Value
		if index >= window {
			sum -= raw[index-window].Value
		}
		width := min(index+1, window)
		out[index].Time = out[index].Time.UTC()
		out[index].Value = sum / float64(width)
	}
	return out, nil
}
