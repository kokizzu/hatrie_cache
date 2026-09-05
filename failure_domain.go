package hatriecache

import hatTopology "hatrie_cache/hat/hatTopology"

// ValidateFailureDomainPlacement checks that each shard has enough distinct,
// explicitly known failure domains. A non-positive minimum disables the check.
func ValidateFailureDomainPlacement(topology ClusterTopology, minimumDistinctDomains int) error {
	return hatTopology.ValidateFailureDomainPlacement(topology, minimumDistinctDomains)
}
