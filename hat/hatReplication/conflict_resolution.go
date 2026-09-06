package hatReplication

import "errors"

var ErrConflictVersionInvalid = errors.New("hatriecache: conflict version is invalid")

// ConflictVersion identifies one write for deterministic conflict resolution.
// Timestamp is a caller-supplied logical or physical timestamp. NodeID must
// identify the writer, and Sequence orders writes from the same writer at the
// same timestamp.
type ConflictVersion struct {
	Timestamp int64  `json:"timestamp"`
	NodeID    string `json:"node_id"`
	Sequence  uint64 `json:"sequence"`
}

// CompareConflictVersions orders two valid versions. The larger timestamp
// wins; NodeID is the stable tie-breaker for concurrent writers, followed by
// Sequence for writes from the same node.
func CompareConflictVersions(left, right ConflictVersion) (int, error) {
	if left.NodeID == "" || right.NodeID == "" {
		return 0, ErrConflictVersionInvalid
	}
	if left.Timestamp < right.Timestamp {
		return -1, nil
	}
	if left.Timestamp > right.Timestamp {
		return 1, nil
	}
	if left.NodeID < right.NodeID {
		return -1, nil
	}
	if left.NodeID > right.NodeID {
		return 1, nil
	}
	if left.Sequence < right.Sequence {
		return -1, nil
	}
	if left.Sequence > right.Sequence {
		return 1, nil
	}
	return 0, nil
}

// ResolveConflictVersion returns the deterministic winner of two writes.
// Equal versions preserve the left value, which is safe when callers assign a
// unique version to each distinct write.
func ResolveConflictVersion(left, right ConflictVersion) (ConflictVersion, error) {
	comparison, err := CompareConflictVersions(left, right)
	if err != nil {
		return ConflictVersion{}, err
	}
	if comparison < 0 {
		return right, nil
	}
	return left, nil
}
