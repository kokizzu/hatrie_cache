package hatReplication

import "strings"

// QuorumTarget describes one replication target. Anonymous targets still
// receive the operation, but their acknowledgement is excluded from quorum
// totals and read-value matching.
type QuorumTarget struct {
	Node      string
	Anonymous bool
}

func normalizeQuorumTargets(targets []QuorumTarget) ([]QuorumTarget, int, bool) {
	if len(targets) == 0 {
		return nil, 0, false
	}
	seen := make(map[string]struct{}, len(targets))
	eligible := 0
	var normalized []QuorumTarget
	for index, target := range targets {
		target.Node = strings.TrimSpace(target.Node)
		if target.Node == "" {
			return nil, 0, false
		}
		if _, found := seen[target.Node]; found {
			return nil, 0, false
		}
		seen[target.Node] = struct{}{}
		if !target.Anonymous {
			eligible++
		}
		if target.Node != targets[index].Node {
			if normalized == nil {
				normalized = make([]QuorumTarget, len(targets))
				copy(normalized, targets)
				for previous := 0; previous < index; previous++ {
					normalized[previous].Node = strings.TrimSpace(normalized[previous].Node)
				}
			}
		}
		if normalized != nil {
			normalized[index] = target
		}
	}
	if normalized == nil {
		normalized = targets
	}
	return normalized, eligible, true
}
