package hatSql

import (
	"errors"
	"fmt"
)

var (
	ErrDifferentialWatermarkNil        = errors.New("hatSql: differential watermark is nil")
	ErrDifferentialWatermarkRegression = errors.New("hatSql: differential watermark moved backwards")
)

// DifferentialWatermark owns a monotonically advancing frontier and applies
// one explicit late-data policy to each batch. A frontier value marks all
// update times below it as late; equal times remain on time.
type DifferentialWatermark struct {
	frontier uint64
	policy   DifferentialLateDataPolicy
}

// NewDifferentialWatermark creates a watermark with the supplied late-data
// policy. The initial frontier is zero, so no uint64 timestamp is late.
func NewDifferentialWatermark(policy DifferentialLateDataPolicy) (*DifferentialWatermark, error) {
	if policy > DifferentialLateDataReject {
		return nil, fmt.Errorf("policy %d: %w", policy, ErrDifferentialLateDataPolicyInvalid)
	}
	return &DifferentialWatermark{policy: policy}, nil
}

// Advance moves the frontier forward. Repeating the current frontier is
// idempotent; moving it backwards is rejected and leaves the current frontier
// unchanged.
func (watermark *DifferentialWatermark) Advance(frontier uint64) error {
	if watermark == nil {
		return ErrDifferentialWatermarkNil
	}
	if frontier < watermark.frontier {
		return fmt.Errorf("frontier %d follows %d: %w", frontier, watermark.frontier, ErrDifferentialWatermarkRegression)
	}
	watermark.frontier = frontier
	return nil
}

// Frontier returns the current frontier. A nil watermark reports zero.
func (watermark *DifferentialWatermark) Frontier() uint64 {
	if watermark == nil {
		return 0
	}
	return watermark.frontier
}

// Apply applies the watermark's configured late-data policy without advancing
// the frontier. The input batch is not mutated.
func (watermark *DifferentialWatermark) Apply(rows []DifferentialRow) ([]DifferentialRow, error) {
	if watermark == nil {
		return nil, ErrDifferentialWatermarkNil
	}
	return ApplyDifferentialLateDataPolicy(rows, watermark.frontier, watermark.policy)
}
