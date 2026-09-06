package hatSql_test

import (
	"errors"
	"testing"
	"time"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestClassifyLateDataHonorsAllowedLatenessAndDropPolicy(t *testing.T) {
	watermark := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	policy := hatSql.LateDataPolicy{AllowedLateness: 5 * time.Minute, DropTooLate: true}

	cases := []struct {
		name       string
		eventTime  time.Time
		late       bool
		tooLate    bool
		accepted   bool
		wantBehind time.Duration
	}{
		{
			name:       "on time",
			eventTime:  watermark.Add(time.Second),
			accepted:   true,
			wantBehind: 0,
		},
		{
			name:       "late within bound",
			eventTime:  watermark.Add(-5 * time.Minute),
			late:       true,
			accepted:   true,
			wantBehind: 5 * time.Minute,
		},
		{
			name:       "too late is dropped",
			eventTime:  watermark.Add(-5*time.Minute - time.Nanosecond),
			late:       true,
			tooLate:    true,
			accepted:   false,
			wantBehind: 5*time.Minute + time.Nanosecond,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			decision, err := hatSql.ClassifyLateData(test.eventTime, watermark, policy)
			if err != nil {
				t.Fatalf("ClassifyLateData() error = %v", err)
			}
			if decision.Late != test.late || decision.TooLate != test.tooLate || decision.Accepted != test.accepted {
				t.Fatalf("ClassifyLateData() = %#v, want late=%v too_late=%v accepted=%v", decision, test.late, test.tooLate, test.accepted)
			}
			if decision.Lateness != test.wantBehind {
				t.Fatalf("Lateness = %v, want %v", decision.Lateness, test.wantBehind)
			}
		})
	}
}

func TestClassifyLateDataCanRetainTooLateEvents(t *testing.T) {
	watermark := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	decision, err := hatSql.ClassifyLateData(
		watermark.Add(-time.Hour),
		watermark,
		hatSql.LateDataPolicy{AllowedLateness: 5 * time.Minute},
	)
	if err != nil {
		t.Fatalf("ClassifyLateData() error = %v", err)
	}
	if !decision.Late || !decision.TooLate || !decision.Accepted {
		t.Fatalf("ClassifyLateData() = %#v, want retained too-late event", decision)
	}
}

func TestClassifyLateDataRejectsNegativeAllowedLateness(t *testing.T) {
	_, err := hatSql.ClassifyLateData(time.Time{}, time.Time{}, hatSql.LateDataPolicy{AllowedLateness: -time.Second})
	if !errors.Is(err, hatSql.ErrLateDataPolicyInvalid) {
		t.Fatalf("ClassifyLateData() error = %v, want ErrLateDataPolicyInvalid", err)
	}
}

func BenchmarkClassifyLateData(b *testing.B) {
	watermark := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	policy := hatSql.LateDataPolicy{AllowedLateness: 5 * time.Minute}
	eventTime := watermark.Add(-time.Minute)
	b.ReportAllocs()
	for range b.N {
		if _, err := hatSql.ClassifyLateData(eventTime, watermark, policy); err != nil {
			b.Fatal(err)
		}
	}
}
