package hatSql

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	// MaxSQLPlanReproducibilityComponentBytes bounds caller-provided schema and
	// settings namespaces before they are copied into the hash input.
	MaxSQLPlanReproducibilityComponentBytes = 4096
	MaxSQLPlanReproducibilitySteps          = 4096
	MaxSQLPlanReproducibilityPlanBytes      = 1 << 20
)

var (
	// ErrSQLPlanReproducibilityInvalid indicates that a reproducibility input is
	// incomplete, oversized, or cannot be encoded deterministically.
	ErrSQLPlanReproducibilityInvalid = errors.New("hatSql: invalid SQL plan reproducibility input")
)

// SQLPlanReproducibilityInput identifies the stable inputs to one query plan.
// SchemaFingerprint and SettingsFingerprint should be stable caller-owned
// fingerprints, not raw schema or settings documents.
type SQLPlanReproducibilityInput struct {
	Query               string
	SchemaFingerprint   string
	SettingsFingerprint string
	Steps               []ExplainStep
}

// SQLPlanReproducibilityHash returns a stable SHA-256 hex digest for a query's
// logical plan namespace. Query literal values and formatting are normalized by
// SQLQueryFingerprint. Runtime-only EXPLAIN ANALYZE observations are excluded
// so the digest can identify a reproducible plan across executions and nodes.
func SQLPlanReproducibilityHash(input SQLPlanReproducibilityInput) (string, error) {
	if input.SchemaFingerprint == "" || input.SettingsFingerprint == "" {
		return "", fmt.Errorf("%w: schema and settings fingerprints are required", ErrSQLPlanReproducibilityInvalid)
	}
	if len(input.SchemaFingerprint) > MaxSQLPlanReproducibilityComponentBytes || len(input.SettingsFingerprint) > MaxSQLPlanReproducibilityComponentBytes {
		return "", fmt.Errorf("%w: schema or settings fingerprint exceeds %d bytes", ErrSQLPlanReproducibilityInvalid, MaxSQLPlanReproducibilityComponentBytes)
	}
	if len(input.Steps) > MaxSQLPlanReproducibilitySteps {
		return "", fmt.Errorf("%w: plan has %d steps, maximum is %d", ErrSQLPlanReproducibilityInvalid, len(input.Steps), MaxSQLPlanReproducibilitySteps)
	}
	queryFingerprint, err := SQLQueryFingerprint(input.Query)
	if err != nil {
		return "", fmt.Errorf("%w: query: %v", ErrSQLPlanReproducibilityInvalid, err)
	}

	steps := cloneSQLPlanSnapshotSteps(input.Steps)
	for index := range steps {
		stripSQLPlanRuntimeFields(&steps[index])
	}
	planBytes, err := json.Marshal(steps)
	if err != nil {
		return "", fmt.Errorf("%w: plan: %v", ErrSQLPlanReproducibilityInvalid, err)
	}
	if len(planBytes) > MaxSQLPlanReproducibilityPlanBytes {
		return "", fmt.Errorf("%w: encoded plan exceeds %d bytes", ErrSQLPlanReproducibilityInvalid, MaxSQLPlanReproducibilityPlanBytes)
	}

	hasher := sha256.New()
	_, _ = hasher.Write([]byte("hatrie-cache/sql-plan-reproducibility/v1"))
	writeHashComponent(hasher, queryFingerprint)
	writeHashComponent(hasher, input.SchemaFingerprint)
	writeHashComponent(hasher, input.SettingsFingerprint)
	writeHashComponent(hasher, string(planBytes))
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func stripSQLPlanRuntimeFields(step *ExplainStep) {
	step.Worker = 0
	step.Pruning = nil
	step.ActualInputRows = nil
	step.ActualOutputRows = nil
	step.ActualInputBytes = nil
	step.ActualOutputBytes = nil
	step.EstimateErrorRows = nil
	step.EstimateErrorPercent = nil
	step.ElapsedNanos = nil
}

func writeHashComponent(hasher interface{ Write([]byte) (int, error) }, value string) {
	var length [binary.MaxVarintLen64]byte
	encodedLength := binary.PutUvarint(length[:], uint64(len(value)))
	_, _ = hasher.Write(length[:encodedLength])
	_, _ = hasher.Write([]byte(value))
}
