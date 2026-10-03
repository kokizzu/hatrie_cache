package hatSchema

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSchemaRegistryMaxSubjects bounds the number of named source
	// subjects retained by a registry when no explicit limit is supplied.
	DefaultSchemaRegistryMaxSubjects = 256
	// DefaultSchemaRegistryMaxVersionsPerSubject bounds retained versions for
	// one subject when no explicit limit is supplied.
	DefaultSchemaRegistryMaxVersionsPerSubject = 16
	// MaxSchemaRegistrySubjects prevents accidental unbounded configuration.
	MaxSchemaRegistrySubjects = 100_000
	// MaxSchemaRegistryVersionsPerSubject prevents accidental unbounded history.
	MaxSchemaRegistryVersionsPerSubject = 1_024
	maxSchemaRegistrySubjectBytes       = 256
)

var (
	// ErrSchemaRegistryNil indicates a method was called on a nil registry.
	ErrSchemaRegistryNil = errors.New("hatSchema: schema registry is nil")
	// ErrSchemaRegistrySubjectEmpty indicates that a subject name is missing.
	ErrSchemaRegistrySubjectEmpty = errors.New("hatSchema: schema registry subject is empty")
	// ErrSchemaRegistrySubjectTooLong indicates that a subject exceeds the
	// bounded registry name size.
	ErrSchemaRegistrySubjectTooLong = errors.New("hatSchema: schema registry subject is too long")
	// ErrSchemaRegistryOptionsInvalid indicates an invalid registry bound or
	// unknown compatibility policy.
	ErrSchemaRegistryOptionsInvalid = errors.New("hatSchema: schema registry options are invalid")
	// ErrSchemaRegistryCapacity indicates that a new subject would exceed the
	// configured subject bound.
	ErrSchemaRegistryCapacity = errors.New("hatSchema: schema registry subject capacity exceeded")
	// ErrSchemaRegistryIncompatible indicates that a candidate schema does not
	// satisfy the registry policy against the current version.
	ErrSchemaRegistryIncompatible = errors.New("hatSchema: schema registry candidate is incompatible")
)

// SchemaRegistryPolicy controls how a candidate is checked against the latest
// registered version. Strict is the safe default and only permits an identical
// schema at a new registry version. Rolling permits the additive changes
// accepted by CheckRollingCompatibility.
type SchemaRegistryPolicy uint8

const (
	SchemaRegistryPolicyStrict SchemaRegistryPolicy = iota
	SchemaRegistryPolicyRolling
)

// SchemaRegistryOptions bounds and configures a SchemaRegistry.
type SchemaRegistryOptions struct {
	MaxSubjects           int
	MaxVersionsPerSubject int
	Policy                SchemaRegistryPolicy
}

// SchemaRegistry stores a bounded, clone-isolated history of schemas by
// connector/source subject. It is an opt-in validation boundary: it does not
// apply schema changes to a source or persist registry state by itself.
type SchemaRegistry struct {
	mu                    sync.RWMutex
	maxSubjects           int
	maxVersionsPerSubject int
	policy                SchemaRegistryPolicy
	subjects              map[string][]Schema
}

// NewSchemaRegistry creates a bounded source schema registry. Zero limits use
// the documented defaults. Strict compatibility is selected when Policy is
// left at its zero value.
func NewSchemaRegistry(options SchemaRegistryOptions) (*SchemaRegistry, error) {
	if options.MaxSubjects == 0 {
		options.MaxSubjects = DefaultSchemaRegistryMaxSubjects
	}
	if options.MaxVersionsPerSubject == 0 {
		options.MaxVersionsPerSubject = DefaultSchemaRegistryMaxVersionsPerSubject
	}
	if options.MaxSubjects < 1 || options.MaxSubjects > MaxSchemaRegistrySubjects ||
		options.MaxVersionsPerSubject < 1 || options.MaxVersionsPerSubject > MaxSchemaRegistryVersionsPerSubject ||
		options.Policy > SchemaRegistryPolicyRolling {
		return nil, ErrSchemaRegistryOptionsInvalid
	}
	return &SchemaRegistry{
		maxSubjects:           options.MaxSubjects,
		maxVersionsPerSubject: options.MaxVersionsPerSubject,
		policy:                options.Policy,
		subjects:              make(map[string][]Schema, options.MaxSubjects),
	}, nil
}

// Check validates a candidate against the current version without publishing
// it. A new subject receives an initial compatible report.
func (r *SchemaRegistry) Check(subject string, candidate Schema) (SchemaCompatibilityReport, error) {
	if r == nil {
		return SchemaCompatibilityReport{}, ErrSchemaRegistryNil
	}
	subject, err := normalizeSchemaRegistrySubject(subject)
	if err != nil {
		return SchemaCompatibilityReport{}, err
	}
	if err := candidate.Validate(); err != nil {
		return SchemaCompatibilityReport{}, err
	}
	r.mu.RLock()
	history := r.subjects[subject]
	if len(history) == 0 {
		r.mu.RUnlock()
		return initialSchemaRegistryReport(candidate), nil
	}
	current := history[len(history)-1].Clone()
	policy := r.policy
	r.mu.RUnlock()
	return compareSchemaRegistryVersions(policy, current, candidate)
}

// Register validates and publishes a candidate. Incompatible candidates are
// returned with their report and are never stored. Re-registering the same
// version and fingerprint is idempotent. When history is full, the oldest
// retained version is evicted after a successful newer registration.
func (r *SchemaRegistry) Register(subject string, candidate Schema) (SchemaCompatibilityReport, error) {
	if r == nil {
		return SchemaCompatibilityReport{}, ErrSchemaRegistryNil
	}
	subject, err := normalizeSchemaRegistrySubject(subject)
	if err != nil {
		return SchemaCompatibilityReport{}, err
	}
	if err := candidate.Validate(); err != nil {
		return SchemaCompatibilityReport{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	history := r.subjects[subject]
	if len(history) == 0 {
		if len(r.subjects) >= r.maxSubjects {
			return SchemaCompatibilityReport{}, ErrSchemaRegistryCapacity
		}
		report := initialSchemaRegistryReport(candidate)
		r.subjects[subject] = []Schema{candidate.Clone()}
		return report, nil
	}
	current := history[len(history)-1]
	report, err := compareSchemaRegistryVersions(r.policy, current, candidate)
	if err != nil {
		return report, err
	}
	if !report.Compatible {
		return report, ErrSchemaRegistryIncompatible
	}
	if current.Version == candidate.Version && current.Fingerprint() == candidate.Fingerprint() {
		return report, nil
	}
	stored := candidate.Clone()
	if len(history) == r.maxVersionsPerSubject {
		history = append(history[1:], stored)
	} else {
		history = append(history, stored)
	}
	r.subjects[subject] = history
	return report, nil
}

// Current returns a deep copy of the latest schema for subject.
func (r *SchemaRegistry) Current(subject string) (Schema, bool) {
	if r == nil {
		return Schema{}, false
	}
	subject, err := normalizeSchemaRegistrySubject(subject)
	if err != nil {
		return Schema{}, false
	}
	r.mu.RLock()
	history := r.subjects[subject]
	if len(history) == 0 {
		r.mu.RUnlock()
		return Schema{}, false
	}
	current := history[len(history)-1].Clone()
	r.mu.RUnlock()
	return current, true
}

// History returns deep copies in oldest-to-newest order. An unknown or invalid
// subject returns nil.
func (r *SchemaRegistry) History(subject string) []Schema {
	if r == nil {
		return nil
	}
	subject, err := normalizeSchemaRegistrySubject(subject)
	if err != nil {
		return nil
	}
	r.mu.RLock()
	history := r.subjects[subject]
	if len(history) == 0 {
		r.mu.RUnlock()
		return nil
	}
	cloned := make([]Schema, len(history))
	for index := range history {
		cloned[index] = history[index].Clone()
	}
	r.mu.RUnlock()
	return cloned
}

// Subjects returns all registered subject names in deterministic order.
func (r *SchemaRegistry) Subjects() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	subjects := make([]string, 0, len(r.subjects))
	for subject := range r.subjects {
		subjects = append(subjects, subject)
	}
	r.mu.RUnlock()
	sort.Strings(subjects)
	return subjects
}

func compareSchemaRegistryVersions(policy SchemaRegistryPolicy, current, candidate Schema) (SchemaCompatibilityReport, error) {
	report, err := CheckRollingCompatibility(current, candidate)
	if err != nil {
		return report, err
	}
	if policy == SchemaRegistryPolicyStrict && current.Fingerprint() != candidate.Fingerprint() {
		report.Compatible = false
		report.Changes = append(report.Changes, SchemaCompatibilityChange{Kind: "strict_schema_changed"})
	}
	return report, nil
}

func initialSchemaRegistryReport(candidate Schema) SchemaCompatibilityReport {
	return SchemaCompatibilityReport{
		Compatible:      true,
		PreviousVersion: 0,
		NextVersion:     candidate.Version,
		Changes:         []SchemaCompatibilityChange{{Kind: "initial_schema"}},
	}
}

func normalizeSchemaRegistrySubject(subject string) (string, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return "", ErrSchemaRegistrySubjectEmpty
	}
	if len(subject) > maxSchemaRegistrySubjectBytes {
		return "", ErrSchemaRegistrySubjectTooLong
	}
	return subject, nil
}
