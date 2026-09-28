package hatSql

import (
	"errors"
	"fmt"
	"testing"
)

func m247Checkpoint() QuerySubscriptionCheckpoint {
	return QuerySubscriptionCheckpoint{
		Version: 1,
		Definition: QuerySubscriptionDefinition{
			Query:        "FROM CACHE('people') SELECT name",
			Dependencies: []string{"people"},
			AsOf:         10,
		},
		Snapshot: QuerySubscriptionSnapshot{
			Revision: 1,
			Frontier: 20,
		},
	}
}

func TestM247ResumeWithValidatorReportsExpiredFrontier(t *testing.T) {
	checkpoint := m247Checkpoint()
	validator := QuerySubscriptionCheckpointValidatorFunc(func(got QuerySubscriptionCheckpoint) error {
		if got.Snapshot.Frontier != checkpoint.Snapshot.Frontier {
			return fmt.Errorf("unexpected frontier %d", got.Snapshot.Frontier)
		}
		return fmt.Errorf("frontier %d: %w", got.Snapshot.Frontier, ErrQuerySubscriptionCheckpointExpired)
	})

	_, err := NewQuerySubscriptions(1).ResumeWithValidator(checkpoint, validator)
	if !errors.Is(err, ErrQuerySubscriptionCheckpointExpired) {
		t.Fatalf("ResumeWithValidator() error = %v, want ErrQuerySubscriptionCheckpointExpired", err)
	}
}

func TestM247ResumeWithValidatorPreservesLegacyAndDifferentialPaths(t *testing.T) {
	checkpoint := m247Checkpoint()
	validator := QuerySubscriptionCheckpointValidatorFunc(func(QuerySubscriptionCheckpoint) error { return nil })

	ordinary, err := NewQuerySubscriptions(1).ResumeWithValidator(checkpoint, validator)
	if err != nil {
		t.Fatalf("ordinary ResumeWithValidator() error = %v", err)
	}
	ordinary.Close()

	checkpoint.Differential = true
	differential, err := NewQuerySubscriptions(1).ResumeDifferentialWithValidator(checkpoint, validator)
	if err != nil {
		t.Fatalf("differential ResumeDifferentialWithValidator() error = %v", err)
	}
	differential.Close()

	legacy, err := NewQuerySubscriptions(1).Resume(m247Checkpoint())
	if err != nil {
		t.Fatalf("legacy Resume() error = %v", err)
	}
	legacy.Close()
}

func TestM247ResumeWithValidatorRejectsNilValidator(t *testing.T) {
	_, err := NewQuerySubscriptions(1).ResumeWithValidator(m247Checkpoint(), nil)
	if !errors.Is(err, ErrQuerySubscriptionCheckpointInvalid) {
		t.Fatalf("nil validator error = %v, want ErrQuerySubscriptionCheckpointInvalid", err)
	}
}

func TestM247ResumeWithValidatorWrapsNonExpiryErrorsAsInvalid(t *testing.T) {
	sourceErr := errors.New("retention metadata unavailable")
	validator := QuerySubscriptionCheckpointValidatorFunc(func(QuerySubscriptionCheckpoint) error {
		return sourceErr
	})

	_, err := NewQuerySubscriptions(1).ResumeWithValidator(m247Checkpoint(), validator)
	if !errors.Is(err, ErrQuerySubscriptionCheckpointInvalid) || !errors.Is(err, sourceErr) {
		t.Fatalf("validation error = %v, want invalid and source error", err)
	}
}

func TestM247ResumeWithValidatorRunsAfterStructuralValidation(t *testing.T) {
	called := false
	validator := QuerySubscriptionCheckpointValidatorFunc(func(QuerySubscriptionCheckpoint) error {
		called = true
		return nil
	})
	checkpoint := m247Checkpoint()
	checkpoint.Version++

	_, err := NewQuerySubscriptions(1).ResumeWithValidator(checkpoint, validator)
	if !errors.Is(err, ErrQuerySubscriptionCheckpointInvalid) {
		t.Fatalf("invalid checkpoint error = %v, want ErrQuerySubscriptionCheckpointInvalid", err)
	}
	if called {
		t.Fatal("validator ran for a structurally invalid checkpoint")
	}
}
