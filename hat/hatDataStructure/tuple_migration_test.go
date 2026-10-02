package hatDataStructure

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func migrationTestFormats(t *testing.T) (TupleFormat, TupleFormat, TupleFormat) {
	t.Helper()
	v1, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "region", Type: TupleFieldString},
	})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "region", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldString},
	})
	if err != nil {
		t.Fatal(err)
	}
	v3, err := NewTupleFormat(3, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "region", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldString},
		{Name: "score", Type: TupleFieldInt64},
	})
	if err != nil {
		t.Fatal(err)
	}
	return v1, v2, v3
}

func migrationTestTuple(t *testing.T, format TupleFormat) VersionedTuple {
	t.Helper()
	tuple, err := NewVersionedTuple(format, []TupleFieldValue{
		TupleInt64(42),
		TupleString("apac"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return tuple
}

func TestTupleMigrationPlanAppliesVersionChain(t *testing.T) {
	v1, v2, v3 := migrationTestFormats(t)
	source := migrationTestTuple(t, v1)
	var calls []string
	plan, err := NewTupleMigrationPlan(" accounts ", []TupleMigrationStep{
		{
			Name: "add-state",
			From: v1,
			To:   v2,
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				if tuple.Version() != v1.Version() {
					return VersionedTuple{}, fmt.Errorf("unexpected source version %d", tuple.Version())
				}
				calls = append(calls, "apply-1-2")
				return NewVersionedTuple(destination, []TupleFieldValue{
					TupleInt64(42),
					TupleString("apac"),
					TupleString("active"),
				})
			},
			Rollback: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				calls = append(calls, "rollback-2-1")
				return NewVersionedTuple(destination, []TupleFieldValue{
					TupleInt64(42),
					TupleString("apac"),
				})
			},
		},
		{
			Name: "add-score",
			From: v2,
			To:   v3,
			Precondition: func(tuple VersionedTuple) error {
				if tuple.Version() != v2.Version() {
					return fmt.Errorf("unexpected precondition version %d", tuple.Version())
				}
				calls = append(calls, "check-2-3")
				return nil
			},
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				calls = append(calls, "apply-2-3")
				return NewVersionedTuple(destination, []TupleFieldValue{
					TupleInt64(42),
					TupleString("apac"),
					TupleString("active"),
					TupleInt64(7),
				})
			},
			Rollback: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				calls = append(calls, "rollback-3-2")
				return NewVersionedTuple(destination, []TupleFieldValue{
					TupleInt64(42),
					TupleString("apac"),
					TupleString("active"),
				})
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Name(); got != "accounts" {
		t.Fatalf("plan name = %q, want accounts", got)
	}

	migrated, err := plan.Migrate(source, v3.Version())
	if err != nil {
		t.Fatal(err)
	}
	if err := migrated.Validate(v3); err != nil {
		t.Fatalf("migrated tuple validation: %v", err)
	}
	if got, want := calls, []string{"apply-1-2", "check-2-3", "apply-2-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("callbacks = %#v, want %#v", got, want)
	}

	unchanged, err := plan.Migrate(source, v1.Version())
	if err != nil {
		t.Fatal(err)
	}
	before, err := MarshalVersionedTuple(source)
	if err != nil {
		t.Fatal(err)
	}
	after, err := MarshalVersionedTuple(unchanged)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("same-version migration changed the tuple")
	}
}

func TestTupleMigrationPlanRollsBackAppliedSteps(t *testing.T) {
	v1, v2, v3 := migrationTestFormats(t)
	source := migrationTestTuple(t, v1)
	original, err := MarshalVersionedTuple(source)
	if err != nil {
		t.Fatal(err)
	}
	blocked := errors.New("migration is blocked")
	var calls []string
	plan, err := NewTupleMigrationPlan("accounts", []TupleMigrationStep{
		{
			From: v1,
			To:   v2,
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				calls = append(calls, "apply-1-2")
				return NewVersionedTuple(destination, []TupleFieldValue{
					TupleInt64(42), TupleString("apac"), TupleString("active"),
				})
			},
			Rollback: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				calls = append(calls, "rollback-2-1")
				return NewVersionedTuple(destination, []TupleFieldValue{
					TupleInt64(42), TupleString("apac"),
				})
			},
		},
		{
			From: v2,
			To:   v3,
			Precondition: func(tuple VersionedTuple) error {
				calls = append(calls, "check-2-3")
				return blocked
			},
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return VersionedTuple{}, errors.New("apply should not run")
			},
			Rollback: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return VersionedTuple{}, errors.New("rollback should not run")
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	rolledBack, err := plan.Migrate(source, v3.Version())
	if !errors.Is(err, blocked) {
		t.Fatalf("migration error = %v, want blocked error", err)
	}
	if got, want := calls, []string{"apply-1-2", "check-2-3", "rollback-2-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("callbacks = %#v, want %#v", got, want)
	}
	got, err := MarshalVersionedTuple(rolledBack)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, got) {
		t.Fatal("failed migration did not return the original tuple")
	}
}

func TestTupleMigrationPlanRejectsInvalidDefinitions(t *testing.T) {
	v1, v2, _ := migrationTestFormats(t)
	validStep := TupleMigrationStep{
		From: v1,
		To:   v2,
		Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
			return NewVersionedTuple(destination, []TupleFieldValue{
				TupleInt64(42), TupleString("apac"), TupleString("active"),
			})
		},
		Rollback: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
			return NewVersionedTuple(destination, []TupleFieldValue{
				TupleInt64(42), TupleString("apac"),
			})
		},
	}
	for name, steps := range map[string][]TupleMigrationStep{
		"missing name":     nil,
		"missing rollback": {{From: v1, To: v2, Apply: validStep.Apply}},
		"duplicate source": {validStep, validStep},
	} {
		t.Run(name, func(t *testing.T) {
			planName := "accounts"
			if name == "missing name" {
				planName = " "
			}
			_, err := NewTupleMigrationPlan(planName, steps)
			if !errors.Is(err, ErrTupleMigrationInvalid) {
				t.Fatalf("error = %v, want ErrTupleMigrationInvalid", err)
			}
		})
	}

	plan, err := NewTupleMigrationPlan("accounts", []TupleMigrationStep{validStep})
	if err != nil {
		t.Fatal(err)
	}
	source := migrationTestTuple(t, v1)
	if _, err := plan.Migrate(source, 99); !errors.Is(err, ErrTupleMigrationPath) {
		t.Fatalf("unreachable target error = %v, want ErrTupleMigrationPath", err)
	}
}

func TestTupleMigrationPlanRejectsConflictingSharedFormat(t *testing.T) {
	v1, v2, v3 := migrationTestFormats(t)
	conflictingV2, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "region", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldInt64},
	})
	if err != nil {
		t.Fatal(err)
	}
	step := func(from, to TupleFormat) TupleMigrationStep {
		return TupleMigrationStep{
			From: from,
			To:   to,
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return NewVersionedTuple(destination, nil)
			},
			Rollback: func(tuple VersionedTuple, source TupleFormat) (VersionedTuple, error) {
				return NewVersionedTuple(source, nil)
			},
		}
	}
	_, err = NewTupleMigrationPlan("accounts", []TupleMigrationStep{
		step(v1, v2),
		step(conflictingV2, v3),
	})
	if !errors.Is(err, ErrTupleMigrationInvalid) {
		t.Fatalf("error = %v, want ErrTupleMigrationInvalid", err)
	}
}

func TestTupleMigrationPlanReportsRollbackFailure(t *testing.T) {
	v1, v2, v3 := migrationTestFormats(t)
	source := migrationTestTuple(t, v1)
	blocked := errors.New("migration is blocked")
	rollbackFailed := errors.New("rollback storage unavailable")
	plan, err := NewTupleMigrationPlan("accounts", []TupleMigrationStep{
		{
			From: v1,
			To:   v2,
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return NewVersionedTuple(destination, []TupleFieldValue{
					TupleInt64(42), TupleString("apac"), TupleString("active"),
				})
			},
			Rollback: func(tuple VersionedTuple, source TupleFormat) (VersionedTuple, error) {
				return VersionedTuple{}, rollbackFailed
			},
		},
		{
			From: v2,
			To:   v3,
			Precondition: func(tuple VersionedTuple) error {
				return blocked
			},
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return VersionedTuple{}, errors.New("apply should not run")
			},
			Rollback: func(tuple VersionedTuple, source TupleFormat) (VersionedTuple, error) {
				return tuple, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := plan.Migrate(source, v3.Version())
	if !errors.Is(err, blocked) || !errors.Is(err, ErrTupleMigrationRollback) || !errors.Is(err, rollbackFailed) {
		t.Fatalf("error = %v, want blocked, rollback, and rollback cause", err)
	}
	if rolledBack.Version() != source.Version() {
		t.Fatalf("returned version = %d, want original version %d", rolledBack.Version(), source.Version())
	}
}
