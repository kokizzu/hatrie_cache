package hatPipeline

import "fmt"

// FrontierRetentionExpiredError identifies why an as-of resume request can no
// longer be served. It unwraps to ErrFrontierRetentionExpired for callers that
// only need the existing sentinel behavior.
type FrontierRetentionExpiredError struct {
	FrontierID    string
	RequestedAsOf uint64
	CurrentLower  uint64
	CurrentUpper  uint64
}

func (err *FrontierRetentionExpiredError) Error() string {
	if err == nil {
		return ErrFrontierRetentionExpired.Error()
	}
	return fmt.Sprintf(
		"hatPipeline: frontier retention timestamp expired: frontier=%q requested_as_of=%d current_lower=%d current_upper=%d",
		err.FrontierID,
		err.RequestedAsOf,
		err.CurrentLower,
		err.CurrentUpper,
	)
}

func (err *FrontierRetentionExpiredError) Unwrap() error {
	return ErrFrontierRetentionExpired
}
