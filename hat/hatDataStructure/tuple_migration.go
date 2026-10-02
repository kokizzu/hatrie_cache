package hatDataStructure

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	// ErrTupleMigrationInvalid indicates an invalid migration plan or tuple
	// supplied to a plan.
	ErrTupleMigrationInvalid = errors.New("hatDataStructure: tuple migration is invalid")
	// ErrTupleMigrationPath indicates that a target version is not reachable
	// from the tuple's current version.
	ErrTupleMigrationPath = errors.New("hatDataStructure: tuple migration path is unavailable")
	// ErrTupleMigrationPrecondition indicates that a migration step was not
	// allowed to start.
	ErrTupleMigrationPrecondition = errors.New("hatDataStructure: tuple migration precondition failed")
	// ErrTupleMigrationApply indicates that a migration step failed or emitted
	// a tuple that does not match its destination format.
	ErrTupleMigrationApply = errors.New("hatDataStructure: tuple migration apply failed")
	// ErrTupleMigrationRollback indicates that an applied migration could not
	// be fully undone.
	ErrTupleMigrationRollback = errors.New("hatDataStructure: tuple migration rollback failed")
)

// TupleMigrationPrecondition checks a tuple immediately before a migration
// step is applied. It should be deterministic and free of mutation.
type TupleMigrationPrecondition func(VersionedTuple) error

// TupleMigrationTransform converts a tuple to the format supplied as the
// destination argument. Apply receives the step's To format; Rollback
// receives the step's From format.
type TupleMigrationTransform func(VersionedTuple, TupleFormat) (VersionedTuple, error)

// TupleMigrationStep describes one forward schema transition. Both Apply and
// Rollback are required so a plan can restore already migrated tuples when a
// later step or its precondition fails.
type TupleMigrationStep struct {
	Name         string
	From         TupleFormat
	To           TupleFormat
	Precondition TupleMigrationPrecondition
	Apply        TupleMigrationTransform
	Rollback     TupleMigrationTransform
}

// TupleMigrationPlan is an immutable, named chain of forward tuple schema
// migrations. Construct one with NewTupleMigrationPlan before serving data.
type TupleMigrationPlan struct {
	name    string
	steps   map[uint64]TupleMigrationStep
	formats map[uint64]TupleFormat
}

// NewTupleMigrationPlan validates and copies a set of version transitions.
// Versions must increase, each source version may have only one outgoing step,
// and every step must provide an apply and rollback function.
func NewTupleMigrationPlan(name string, steps []TupleMigrationStep) (*TupleMigrationPlan, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: plan name is required", ErrTupleMigrationInvalid)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("%w: plan %q has no steps", ErrTupleMigrationInvalid, name)
	}
	plan := &TupleMigrationPlan{
		name:    name,
		steps:   make(map[uint64]TupleMigrationStep, len(steps)),
		formats: make(map[uint64]TupleFormat, len(steps)+1),
	}
	for index, original := range steps {
		step := original
		step.Name = strings.TrimSpace(step.Name)
		if step.Name == "" {
			step.Name = fmt.Sprintf("v%d-to-v%d", step.From.Version(), step.To.Version())
		}
		if err := validateTupleMigrationStep(step); err != nil {
			return nil, fmt.Errorf("%w: step %d (%s): %v", ErrTupleMigrationInvalid, index, step.Name, err)
		}
		from, err := cloneTupleMigrationFormat(step.From)
		if err != nil {
			return nil, fmt.Errorf("%w: step %d source: %v", ErrTupleMigrationInvalid, index, err)
		}
		to, err := cloneTupleMigrationFormat(step.To)
		if err != nil {
			return nil, fmt.Errorf("%w: step %d destination: %v", ErrTupleMigrationInvalid, index, err)
		}
		step.From = from
		step.To = to
		fromVersion := from.Version()
		if _, exists := plan.steps[fromVersion]; exists {
			return nil, fmt.Errorf("%w: source version %d has multiple steps", ErrTupleMigrationInvalid, fromVersion)
		}
		if err := rememberTupleMigrationFormat(plan.formats, from); err != nil {
			return nil, fmt.Errorf("%w: step %d source: %v", ErrTupleMigrationInvalid, index, err)
		}
		if err := rememberTupleMigrationFormat(plan.formats, to); err != nil {
			return nil, fmt.Errorf("%w: step %d destination: %v", ErrTupleMigrationInvalid, index, err)
		}
		plan.steps[fromVersion] = step
	}
	return plan, nil
}

// Name returns the plan's trimmed logical name.
func (plan *TupleMigrationPlan) Name() string {
	if plan == nil {
		return ""
	}
	return plan.name
}

// Migrate applies the unique forward path to targetVersion. On failure it
// invokes Rollback in reverse order for every step that completed, and
// returns the original tuple rather than a partially migrated tuple.
func (plan *TupleMigrationPlan) Migrate(tuple VersionedTuple, targetVersion uint64) (VersionedTuple, error) {
	if plan == nil || plan.name == "" || len(plan.steps) == 0 {
		return VersionedTuple{}, ErrTupleMigrationInvalid
	}
	if tuple.Version() == 0 || targetVersion == 0 {
		return VersionedTuple{}, fmt.Errorf("%w: tuple and target versions must be positive", ErrTupleMigrationInvalid)
	}
	startFormat, ok := plan.formats[tuple.Version()]
	if !ok {
		return VersionedTuple{}, fmt.Errorf("%w: source version %d is not in plan %q", ErrTupleMigrationPath, tuple.Version(), plan.name)
	}
	if err := tuple.Validate(startFormat); err != nil {
		return VersionedTuple{}, fmt.Errorf("%w: source tuple: %w", ErrTupleMigrationInvalid, err)
	}
	if targetVersion == tuple.Version() {
		return tuple, nil
	}
	if targetVersion < tuple.Version() {
		return tuple, fmt.Errorf("%w: plan %q only supports forward migrations", ErrTupleMigrationPath, plan.name)
	}

	original := tuple
	current := tuple
	applied := make([]TupleMigrationStep, 0, 4)
	for current.Version() != targetVersion {
		step, ok := plan.steps[current.Version()]
		if !ok || step.To.Version() > targetVersion {
			cause := fmt.Errorf("%w: cannot reach version %d from version %d in plan %q", ErrTupleMigrationPath, targetVersion, current.Version(), plan.name)
			return plan.failMigration(original, current, applied, cause)
		}
		if err := current.Validate(step.From); err != nil {
			cause := fmt.Errorf("%w: step %q source tuple: %w", ErrTupleMigrationInvalid, step.Name, err)
			return plan.failMigration(original, current, applied, cause)
		}
		if step.Precondition != nil {
			if err := step.Precondition(current); err != nil {
				cause := fmt.Errorf("%w: step %q: %w", ErrTupleMigrationPrecondition, step.Name, err)
				return plan.failMigration(original, current, applied, cause)
			}
		}
		next, err := step.Apply(current, step.To)
		if err != nil {
			cause := fmt.Errorf("%w: step %q: %w", ErrTupleMigrationApply, step.Name, err)
			return plan.failMigration(original, current, applied, cause)
		}
		if err := next.Validate(step.To); err != nil {
			cause := fmt.Errorf("%w: step %q destination tuple: %w", ErrTupleMigrationApply, step.Name, err)
			return plan.failMigration(original, current, applied, cause)
		}
		current = next
		applied = append(applied, step)
	}
	return current, nil
}

func (plan *TupleMigrationPlan) failMigration(original, current VersionedTuple, applied []TupleMigrationStep, cause error) (VersionedTuple, error) {
	if len(applied) == 0 {
		return original, cause
	}
	if _, err := plan.rollbackMigration(current, applied); err != nil {
		return original, errors.Join(cause, err)
	}
	return original, cause
}

func (plan *TupleMigrationPlan) rollbackMigration(current VersionedTuple, applied []TupleMigrationStep) (VersionedTuple, error) {
	for index := len(applied) - 1; index >= 0; index-- {
		step := applied[index]
		previous, err := step.Rollback(current, step.From)
		if err != nil {
			return current, fmt.Errorf("%w: step %q: %w", ErrTupleMigrationRollback, step.Name, err)
		}
		if err := previous.Validate(step.From); err != nil {
			return current, fmt.Errorf("%w: step %q returned invalid source tuple: %w", ErrTupleMigrationRollback, step.Name, err)
		}
		current = previous
	}
	return current, nil
}

func validateTupleMigrationStep(step TupleMigrationStep) error {
	if err := step.From.validateDefinition(); err != nil {
		return fmt.Errorf("source format: %w", err)
	}
	if err := step.To.validateDefinition(); err != nil {
		return fmt.Errorf("destination format: %w", err)
	}
	if step.From.Version() == step.To.Version() {
		return errors.New("source and destination versions must differ")
	}
	if step.To.Version() < step.From.Version() {
		return errors.New("destination version must be greater than source version")
	}
	if step.Apply == nil {
		return errors.New("apply function is required")
	}
	if step.Rollback == nil {
		return errors.New("rollback function is required")
	}
	return nil
}

func cloneTupleMigrationFormat(format TupleFormat) (TupleFormat, error) {
	return NewTupleFormat(format.Version(), format.Fields())
}

func rememberTupleMigrationFormat(formats map[uint64]TupleFormat, format TupleFormat) error {
	version := format.Version()
	existing, ok := formats[version]
	if !ok {
		formats[version] = format
		return nil
	}
	if !tupleMigrationFormatsCompatible(existing, format) {
		return fmt.Errorf("version %d has conflicting format definitions", version)
	}
	return nil
}

func tupleMigrationFormatsCompatible(left, right TupleFormat) bool {
	if left.Version() != right.Version() || left.FieldCount() != right.FieldCount() {
		return false
	}
	for index, leftField := range left.fields {
		rightField := right.fields[index]
		if leftField.Name != rightField.Name || leftField.Type != rightField.Type || leftField.Nullable != rightField.Nullable {
			return false
		}
		if !reflect.DeepEqual(leftField.Default, rightField.Default) {
			return false
		}
		if (leftField.Generated == nil) != (rightField.Generated == nil) {
			return false
		}
		if leftField.Generated != nil && reflect.ValueOf(leftField.Generated).Pointer() != reflect.ValueOf(rightField.Generated).Pointer() {
			return false
		}
	}
	return true
}
