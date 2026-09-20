package hatDataStructure

import (
	"errors"
	"time"
)

var (
	// ErrTTLRecompressionInvalid reports an invalid or overlapping policy.
	ErrTTLRecompressionInvalid = errors.New("hatDataStructure: invalid TTL recompression policy")
	// ErrTTLRecompressionTarget reports a missing destination compressor for a rewrite.
	ErrTTLRecompressionTarget = errors.New("hatDataStructure: TTL recompression target is required")
)

// TTLRecompressionDecision describes the independent action for one record.
type TTLRecompressionDecision uint8

const (
	// TTLRecompressionKeep leaves a record untouched.
	TTLRecompressionKeep TTLRecompressionDecision = iota
	// TTLRecompressionRewrite rewrites a record with a colder codec.
	TTLRecompressionRewrite
	// TTLRecompressionDelete removes a record at its deletion horizon.
	TTLRecompressionDelete
)

// String returns a stable decision name for metrics and logs.
func (decision TTLRecompressionDecision) String() string {
	switch decision {
	case TTLRecompressionKeep:
		return "keep"
	case TTLRecompressionRewrite:
		return "rewrite"
	case TTLRecompressionDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// TTLRecompressionPolicy separates a colder rewrite horizon from a deletion
// horizon. A zero horizon disables that action. Recompression must precede
// deletion when both horizons are configured.
type TTLRecompressionPolicy struct {
	RecompressAfter time.Duration
	DeleteAfter     time.Duration
}

// Validate checks the policy without inspecting a record timestamp.
func (policy TTLRecompressionPolicy) Validate() error {
	if policy.RecompressAfter < 0 || policy.DeleteAfter < 0 {
		return ErrTTLRecompressionInvalid
	}
	if policy.RecompressAfter > 0 && policy.DeleteAfter > 0 && policy.RecompressAfter >= policy.DeleteAfter {
		return ErrTTLRecompressionInvalid
	}
	return nil
}

// Decide selects an action without decoding the record. Invalid policies and
// unknown timestamps conservatively retain the record.
func (policy TTLRecompressionPolicy) Decide(createdAt, now time.Time) TTLRecompressionDecision {
	if policy.Validate() != nil || createdAt.IsZero() || now.Before(createdAt) {
		return TTLRecompressionKeep
	}
	age := now.Sub(createdAt)
	if policy.DeleteAfter > 0 && age >= policy.DeleteAfter {
		return TTLRecompressionDelete
	}
	if policy.RecompressAfter > 0 && age >= policy.RecompressAfter {
		return TTLRecompressionRewrite
	}
	return TTLRecompressionKeep
}

// RecompressIfDue applies a policy to one tuple frame. Keep returns the input
// frame without decoding, delete returns no frame without decoding, and
// rewrite decodes with the receiver and encodes with target. The caller owns
// the returned frame and may safely retain the original until the rewrite is
// durably committed.
func (compressor *TupleCompressor) RecompressIfDue(
	frame []byte,
	createdAt, now time.Time,
	policy TTLRecompressionPolicy,
	target *TupleCompressor,
) ([]byte, TTLRecompressionDecision, error) {
	if compressor == nil {
		return nil, TTLRecompressionKeep, ErrTupleCompressionInvalid
	}
	if err := policy.Validate(); err != nil {
		return nil, TTLRecompressionKeep, err
	}
	decision := policy.Decide(createdAt, now)
	switch decision {
	case TTLRecompressionKeep:
		return frame, decision, nil
	case TTLRecompressionDelete:
		return nil, decision, nil
	case TTLRecompressionRewrite:
		if target == nil {
			return nil, decision, ErrTTLRecompressionTarget
		}
		decoded, err := compressor.Decompress(frame)
		if err != nil {
			return nil, decision, err
		}
		rewritten, err := target.Compress(decoded)
		if err != nil {
			return nil, decision, err
		}
		return rewritten, decision, nil
	default:
		return nil, TTLRecompressionKeep, ErrTTLRecompressionInvalid
	}
}
