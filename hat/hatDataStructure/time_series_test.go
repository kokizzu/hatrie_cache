package hatDataStructure_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTimeSeriesBucketsIncludeGapsAndRollingAverage(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	points := []hatDataStructure.TimeSeriesPoint{
		{Time: start.Add(10 * time.Minute), Value: 2},
		{Time: start.Add(2*time.Hour + 10*time.Minute), Value: 8},
	}
	buckets, err := hatDataStructure.BucketTimeSeries(points, start, start.Add(3*time.Hour), time.Hour)
	if err != nil || len(buckets) != 3 || buckets[1].Count != 0 || buckets[0].Average != 2 || buckets[2].Average != 8 {
		t.Fatalf("BucketTimeSeries() = %#v, %v", buckets, err)
	}
	rolling, err := hatDataStructure.RollingAverage(points, 2)
	if err != nil || len(rolling) != 2 || rolling[1].Value != 5 {
		t.Fatalf("RollingAverage() = %#v, %v", rolling, err)
	}
}
