package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// CatalogMigrationPlanVersion is the current serialized plan contract.
	CatalogMigrationPlanVersion uint64 = 1
	// MaxCatalogMigrationSteps bounds validation and rollback state.
	MaxCatalogMigrationSteps = 4096
)

var (
	// ErrCatalogMigrationPlanInvalid identifies malformed or cyclic plans.
	ErrCatalogMigrationPlanInvalid = errors.New("hatSql: catalog migration plan is invalid")
	// ErrCatalogMigrationContextRequired identifies a nil execution context.
	ErrCatalogMigrationContextRequired = errors.New("hatSql: catalog migration context is required")
	// ErrCatalogMigrationRunnerRequired identifies a missing apply/rollback hook.
	ErrCatalogMigrationRunnerRequired = errors.New("hatSql: catalog migration runner is incomplete")
	// ErrCatalogMigrationApplyFailed identifies a step that failed during apply.
	ErrCatalogMigrationApplyFailed = errors.New("hatSql: catalog migration apply failed")
)

// CatalogMigrationStep is one dependency-aware catalog operation. IDs and
// dependency IDs are stable plan-local names, not SQL text.
type CatalogMigrationStep struct {
	ID        string
	Object    string
	Action    string
	DependsOn []string
}

// CatalogMigrationPlan is a bounded, deterministic set of catalog operations.
type CatalogMigrationPlan struct {
	Version uint64
	Steps   []CatalogMigrationStep
}

// CatalogMigrationRunner supplies the side effects for a validated plan.
// Apply is all-or-rollback: every successfully applied step is offered to
// RollbackStep in reverse order after a later failure.
type CatalogMigrationRunner struct {
	ApplyStep    func(context.Context, CatalogMigrationStep) error
	RollbackStep func(context.Context, CatalogMigrationStep) error
}

// CatalogMigrationResult reports the committed prefix and rollback outcome.
type CatalogMigrationResult struct {
	Applied      []string
	RolledBack   []string
	FailedStep   string
	RollbackErrs []error
}

// CatalogMigrationError preserves the apply failure and every rollback error.
type CatalogMigrationError struct {
	FailedStep   string
	ApplyError   error
	RollbackErrs []error
}

func (err CatalogMigrationError) Error() string {
	if len(err.RollbackErrs) == 0 {
		return fmt.Sprintf("%s at %q: %v", ErrCatalogMigrationApplyFailed, err.FailedStep, err.ApplyError)
	}
	return fmt.Sprintf("%s at %q: %v (%d rollback error(s))", ErrCatalogMigrationApplyFailed, err.FailedStep, err.ApplyError, len(err.RollbackErrs))
}

func (err CatalogMigrationError) Is(target error) bool {
	return target == ErrCatalogMigrationApplyFailed || errors.Is(err.ApplyError, target)
}

func (err CatalogMigrationError) Unwrap() error {
	return err.ApplyError
}

// OrderedSteps validates the plan and returns a deterministic topological
// order. Independent steps are ordered by ID, making dry-run output stable.
func (plan CatalogMigrationPlan) OrderedSteps() ([]CatalogMigrationStep, error) {
	if plan.Version != CatalogMigrationPlanVersion || len(plan.Steps) == 0 || len(plan.Steps) > MaxCatalogMigrationSteps {
		return nil, ErrCatalogMigrationPlanInvalid
	}
	byID := make(map[string]CatalogMigrationStep, len(plan.Steps))
	indegree := make(map[string]int, len(plan.Steps))
	dependents := make(map[string][]string, len(plan.Steps))
	for _, input := range plan.Steps {
		step := cloneCatalogMigrationStep(input)
		if step.ID == "" || strings.TrimSpace(step.ID) != step.ID || step.Object == "" || strings.TrimSpace(step.Object) != step.Object || step.Action == "" || strings.TrimSpace(step.Action) != step.Action {
			return nil, ErrCatalogMigrationPlanInvalid
		}
		if _, exists := byID[step.ID]; exists {
			return nil, ErrCatalogMigrationPlanInvalid
		}
		byID[step.ID] = step
		indegree[step.ID] = len(step.DependsOn)
		seenDependencies := make(map[string]struct{}, len(step.DependsOn))
		for _, dependency := range step.DependsOn {
			if dependency == "" || strings.TrimSpace(dependency) != dependency || dependency == step.ID {
				return nil, ErrCatalogMigrationPlanInvalid
			}
			if _, duplicate := seenDependencies[dependency]; duplicate {
				return nil, ErrCatalogMigrationPlanInvalid
			}
			seenDependencies[dependency] = struct{}{}
			dependents[dependency] = append(dependents[dependency], step.ID)
		}
	}
	for dependency := range dependents {
		if _, exists := byID[dependency]; !exists {
			return nil, ErrCatalogMigrationPlanInvalid
		}
		sort.Strings(dependents[dependency])
	}

	ready := make([]string, 0, len(plan.Steps))
	for id, count := range indegree {
		if count == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	ordered := make([]CatalogMigrationStep, 0, len(plan.Steps))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		ordered = append(ordered, cloneCatalogMigrationStep(byID[id]))
		for _, dependent := range dependents[id] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				position := sort.SearchStrings(ready, dependent)
				ready = append(ready, "")
				copy(ready[position+1:], ready[position:])
				ready[position] = dependent
			}
		}
	}
	if len(ordered) != len(plan.Steps) {
		return nil, ErrCatalogMigrationPlanInvalid
	}
	return ordered, nil
}

// Apply validates and executes a plan. On failure it rolls back every
// successful step in reverse dependency order, even if the caller context was
// canceled after the failed apply.
func (runner CatalogMigrationRunner) Apply(ctx context.Context, plan CatalogMigrationPlan) (CatalogMigrationResult, error) {
	var result CatalogMigrationResult
	if ctx == nil {
		return result, ErrCatalogMigrationContextRequired
	}
	if runner.ApplyStep == nil || runner.RollbackStep == nil {
		return result, ErrCatalogMigrationRunnerRequired
	}
	ordered, err := plan.OrderedSteps()
	if err != nil {
		return result, err
	}
	appliedSteps := make([]CatalogMigrationStep, 0, len(ordered))
	for _, step := range ordered {
		stepErr := ctx.Err()
		if stepErr == nil {
			stepErr = runner.ApplyStep(ctx, step)
		}
		if stepErr != nil {
			result.FailedStep = step.ID
			rollbackContext := context.WithoutCancel(ctx)
			for index := len(appliedSteps) - 1; index >= 0; index-- {
				previous := appliedSteps[index]
				if rollbackErr := runner.RollbackStep(rollbackContext, previous); rollbackErr != nil {
					result.RollbackErrs = append(result.RollbackErrs, rollbackErr)
					continue
				}
				result.RolledBack = append(result.RolledBack, previous.ID)
			}
			return result, CatalogMigrationError{
				FailedStep:   step.ID,
				ApplyError:   stepErr,
				RollbackErrs: append([]error(nil), result.RollbackErrs...),
			}
		}
		result.Applied = append(result.Applied, step.ID)
		appliedSteps = append(appliedSteps, step)
	}
	return result, nil
}

func cloneCatalogMigrationStep(input CatalogMigrationStep) CatalogMigrationStep {
	input.DependsOn = append([]string(nil), input.DependsOn...)
	return input
}
