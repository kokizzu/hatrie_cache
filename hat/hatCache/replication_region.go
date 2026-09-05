package hatCache

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ReplicationRegionPolicy describes operator expectations for cross-region
// replication. A zero value leaves replication behavior unchanged.
type ReplicationRegionPolicy struct {
	LocalRegion           string
	RequiredRemoteRegions []string
	MaxRPOLagSequences    uint64
	MaxRTO                time.Duration
}

// ReplicationRegionStatus is a point-in-time view of regional replication
// coverage and sequence lag. Sequence lag is an RPO proxy, not elapsed time.
type ReplicationRegionStatus struct {
	Configured                bool                `json:"configured"`
	ConfigurationError        string              `json:"configuration_error,omitempty"`
	LocalRegion               string              `json:"local_region,omitempty"`
	RequiredRemoteRegions     []string            `json:"required_remote_regions,omitempty"`
	AvailableRemoteRegions    []string            `json:"available_remote_regions,omitempty"`
	MissingRemoteRegions      []string            `json:"missing_remote_regions,omitempty"`
	RemoteTargetsByRegion     map[string][]string `json:"remote_targets_by_region,omitempty"`
	ReplicationLagByTarget    map[string]uint64   `json:"replication_lag_by_target,omitempty"`
	CurrentMaxRPOLagSequences uint64              `json:"current_max_rpo_lag_sequences"`
	MaxRPOLagSequences        uint64              `json:"max_rpo_lag_sequences,omitempty"`
	RPOWithinBudget           bool                `json:"rpo_within_budget"`
	MaxRTOMillis              int64               `json:"max_rto_millis,omitempty"`
}

// Validate checks that a regional policy is internally consistent.
func (policy ReplicationRegionPolicy) Validate() error {
	_, err := normalizeReplicationRegionPolicy(policy)
	return err
}

// Configured reports whether this policy has any enabled setting.
func (policy ReplicationRegionPolicy) Configured() bool {
	return strings.TrimSpace(policy.LocalRegion) != "" ||
		len(policy.RequiredRemoteRegions) > 0 ||
		policy.MaxRPOLagSequences > 0 ||
		policy.MaxRTO > 0
}

// ValidateRecoveryDuration checks an observed recovery duration against the
// configured RTO budget. A zero MaxRTO disables the check.
func (policy ReplicationRegionPolicy) ValidateRecoveryDuration(duration time.Duration) error {
	if duration < 0 {
		return errors.New("hatriecache: recovery duration cannot be negative")
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	if policy.MaxRTO <= 0 {
		return nil
	}
	if duration > policy.MaxRTO {
		return fmt.Errorf("hatriecache: recovery duration %s exceeded RTO budget %s", duration, policy.MaxRTO)
	}
	return nil
}

func normalizeReplicationRegionPolicy(policy ReplicationRegionPolicy) (ReplicationRegionPolicy, error) {
	policy.LocalRegion = strings.TrimSpace(policy.LocalRegion)
	if policy.MaxRTO < 0 {
		return ReplicationRegionPolicy{}, errors.New("hatriecache: replication RTO cannot be negative")
	}
	if policy.Configured() && policy.LocalRegion == "" {
		return ReplicationRegionPolicy{}, errors.New("hatriecache: local region is required when regional replication policy is configured")
	}
	seen := make(map[string]struct{}, len(policy.RequiredRemoteRegions))
	regions := make([]string, 0, len(policy.RequiredRemoteRegions))
	for _, value := range policy.RequiredRemoteRegions {
		region := strings.TrimSpace(value)
		if region == "" {
			return ReplicationRegionPolicy{}, errors.New("hatriecache: required remote region cannot be empty")
		}
		if region == policy.LocalRegion {
			return ReplicationRegionPolicy{}, fmt.Errorf("hatriecache: required remote region %q is the local region", region)
		}
		if _, ok := seen[region]; ok {
			return ReplicationRegionPolicy{}, fmt.Errorf("hatriecache: duplicate required remote region %q", region)
		}
		seen[region] = struct{}{}
		regions = append(regions, region)
	}
	sort.Strings(regions)
	policy.RequiredRemoteRegions = regions
	return policy, nil
}

// RegionReplicationStatus returns regional coverage and current asynchronous
// replication sequence lag without changing routing or failover behavior.
func (replicator *HTTPReplicator) RegionReplicationStatus() ReplicationRegionStatus {
	status := ReplicationRegionStatus{RPOWithinBudget: true}
	if replicator == nil {
		return status
	}
	policy, err := normalizeReplicationRegionPolicy(replicator.regionPolicy)
	if err != nil {
		status.ConfigurationError = err.Error()
		status.RPOWithinBudget = false
		return status
	}
	if !policy.Configured() {
		return status
	}
	status.Configured = true
	status.LocalRegion = policy.LocalRegion
	status.RequiredRemoteRegions = append([]string(nil), policy.RequiredRemoteRegions...)
	status.MaxRPOLagSequences = policy.MaxRPOLagSequences
	if policy.MaxRTO > 0 {
		status.MaxRTOMillis = policy.MaxRTO.Milliseconds()
	}

	var topology ClusterTopology
	if replicator.topology != nil {
		topology, _ = replicator.topology.replicationSnapshot()
	}
	remoteTargetsByRegion := make(map[string][]string)
	self := strings.TrimSpace(replicator.self)
	for _, node := range topology.Nodes {
		nodeID := strings.TrimSpace(node.ID)
		if nodeID != "" && nodeID == self {
			continue
		}
		region := strings.TrimSpace(node.Region)
		if region == "" || region == policy.LocalRegion {
			continue
		}
		target := nodeID
		if target == "" {
			target = strings.TrimSpace(node.Address)
		}
		if target == "" {
			continue
		}
		remoteTargetsByRegion[region] = append(remoteTargetsByRegion[region], target)
	}
	for region := range remoteTargetsByRegion {
		sort.Strings(remoteTargetsByRegion[region])
	}
	status.RemoteTargetsByRegion = remoteTargetsByRegion
	status.AvailableRemoteRegions = make([]string, 0, len(remoteTargetsByRegion))
	for region := range remoteTargetsByRegion {
		status.AvailableRemoteRegions = append(status.AvailableRemoteRegions, region)
	}
	sort.Strings(status.AvailableRemoteRegions)

	available := make(map[string]struct{}, len(remoteTargetsByRegion))
	for region := range remoteTargetsByRegion {
		available[region] = struct{}{}
	}
	for _, region := range policy.RequiredRemoteRegions {
		if _, ok := available[region]; !ok {
			status.MissingRemoteRegions = append(status.MissingRemoteRegions, region)
		}
	}

	replicator.mu.RLock()
	queue := replicator.queueStatsLocked()
	replicator.mu.RUnlock()
	status.ReplicationLagByTarget = make(map[string]uint64)
	selectedRegions := available
	if len(policy.RequiredRemoteRegions) > 0 {
		selectedRegions = make(map[string]struct{}, len(policy.RequiredRemoteRegions))
		for _, region := range policy.RequiredRemoteRegions {
			selectedRegions[region] = struct{}{}
		}
	}
	for region := range selectedRegions {
		for _, target := range remoteTargetsByRegion[region] {
			acknowledged := queue.LastAcknowledgedSequenceByTarget[target]
			lag := uint64(0)
			if queue.SourceSequence > acknowledged {
				lag = queue.SourceSequence - acknowledged
			}
			status.ReplicationLagByTarget[target] = lag
			if lag > status.CurrentMaxRPOLagSequences {
				status.CurrentMaxRPOLagSequences = lag
			}
		}
	}
	status.RPOWithinBudget = len(status.MissingRemoteRegions) == 0 &&
		(policy.MaxRPOLagSequences == 0 || status.CurrentMaxRPOLagSequences <= policy.MaxRPOLagSequences)
	return status
}
