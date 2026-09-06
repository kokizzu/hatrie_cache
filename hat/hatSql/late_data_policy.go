package hatSql

import (
	"errors"
	"time"
)

var ErrLateDataPolicyInvalid = errors.New("hatriecache: late-data policy is invalid")

// LateDataPolicy defines how far an event may trail the current watermark.
// When DropTooLate is false, too-late events are still accepted for correction
// or audit workflows.
type LateDataPolicy struct {
	AllowedLateness time.Duration
	DropTooLate     bool
}

// LateDataDecision describes one event-time admission decision.
type LateDataDecision struct {
	EventTime time.Time
	Watermark time.Time
	Lateness  time.Duration
	Late      bool
	TooLate   bool
	Accepted  bool
}

// ClassifyLateData classifies an event against a watermark without mutating
// either input. Events exactly at the allowed-lateness boundary are accepted.
func ClassifyLateData(eventTime, watermark time.Time, policy LateDataPolicy) (LateDataDecision, error) {
	if policy.AllowedLateness < 0 {
		return LateDataDecision{}, ErrLateDataPolicyInvalid
	}
	decision := LateDataDecision{
		EventTime: eventTime,
		Watermark: watermark,
		Accepted:  true,
	}
	if !eventTime.Before(watermark) {
		return decision, nil
	}
	decision.Late = true
	decision.Lateness = watermark.Sub(eventTime)
	decision.TooLate = decision.Lateness > policy.AllowedLateness
	decision.Accepted = !decision.TooLate || !policy.DropTooLate
	return decision, nil
}
