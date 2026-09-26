package hatSql

import (
	"context"
	"testing"
	"time"
)

func TestCH012ProjectionAdvisorForecastsOptInWorkload(t *testing.T) {
	base := time.Unix(100, 0)
	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               4,
		EnableWorkloadForecast: true,
	})
	advisor.RecordWorkload("hot", []string{"events"}, base)
	advisor.RecordWorkload("hot", []string{"events"}, base.Add(time.Hour))
	advisor.RecordWorkload("single", []string{"orders"}, base)

	forecasts, err := advisor.ForecastWorkloadAt(base.Add(24*time.Hour), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(forecasts) != 2 {
		t.Fatalf("forecasts = %#v, want 2", forecasts)
	}
	if forecasts[0].QueryID != "hot" || forecasts[0].ObservedQueries != 2 || forecasts[0].ObservationWindow != time.Hour || forecasts[0].ForecastWindow != 24*time.Hour || forecasts[0].ExpectedQueries != 48 {
		t.Fatalf("hot forecast = %#v", forecasts[0])
	}
	if forecasts[1].QueryID != "single" || forecasts[1].ObservedQueries != 1 || forecasts[1].ObservationWindow != 0 || forecasts[1].ExpectedQueries != 1 {
		t.Fatalf("single forecast = %#v", forecasts[1])
	}
}

func TestCH012ProjectionAdvisorForecastIsOptInAndBounded(t *testing.T) {
	base := time.Unix(200, 0)
	defaultAdvisor := NewSQLProjectionAdvisor(1)
	defaultAdvisor.RecordWorkload("ignored", []string{"events"}, base)
	if forecasts, err := defaultAdvisor.ForecastWorkloadAt(base, time.Hour); err != nil || len(forecasts) != 0 {
		t.Fatalf("default forecast = %#v, err = %v; want empty", forecasts, err)
	}

	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               1,
		EnableWorkloadForecast: true,
	})
	advisor.RecordWorkload("first", []string{"events"}, base)
	advisor.RecordWorkload("second", []string{"orders"}, base)
	forecasts, err := advisor.ForecastWorkloadAt(base, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(forecasts) != 1 || forecasts[0].QueryID != "first" {
		t.Fatalf("bounded forecasts = %#v, want first only", forecasts)
	}
}

func TestCH012ProjectionAdvisorForecastRejectsInvalidHorizon(t *testing.T) {
	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               1,
		EnableWorkloadForecast: true,
	})
	if _, err := advisor.ForecastWorkloadAt(time.Unix(300, 0), 0); err == nil {
		t.Fatal("zero forecast horizon was accepted")
	}
	if _, err := advisor.ForecastWorkloadAt(time.Unix(300, 0), -time.Second); err == nil {
		t.Fatal("negative forecast horizon was accepted")
	}
}

func TestCH012ProjectionAdvisorForecastSaturates(t *testing.T) {
	base := time.Unix(400, 0)
	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               1,
		EnableWorkloadForecast: true,
	})
	for index := 0; index < 3; index++ {
		advisor.RecordWorkload("saturated", []string{"events"}, base.Add(time.Duration(index)*time.Nanosecond))
	}
	forecasts, err := advisor.ForecastWorkloadAt(base.Add(time.Nanosecond), time.Duration(1<<63-1))
	if err != nil {
		t.Fatal(err)
	}
	if len(forecasts) != 1 || forecasts[0].ExpectedQueries != ^uint64(0) {
		t.Fatalf("saturated forecast = %#v, want max uint64", forecasts)
	}
}

func TestCH012ProjectionAdvisorRecordsAutomaticWorkloadWhenEnabled(t *testing.T) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"team": "blue", "points": int64(5)}}, nil
	})
	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               2,
		EnableWorkloadForecast: true,
	})
	if _, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team WHERE points > 0", resolver, nil, QueryOptions{
		ProjectionAdvisor:  advisor,
		QueryID:            "team_totals",
		SlowQueryThreshold: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	forecasts, err := advisor.ForecastWorkloadAt(time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(forecasts) != 1 || forecasts[0].QueryID != "team_totals" || forecasts[0].ObservedQueries != 1 {
		t.Fatalf("automatic forecasts = %#v, want one team_totals observation", forecasts)
	}
}
