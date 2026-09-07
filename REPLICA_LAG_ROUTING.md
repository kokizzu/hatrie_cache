# Replica Lag Routing

`hatReplication.SelectReadReplica` filters read candidates by an explicit
`ReadReplicaPolicy` before choosing one. `RequiredFrontier` enforces a minimum
replication position, while `ObservedFrontier` and `MaxLag` reject replicas
that are too far behind the caller's observed state. A zero `MaxLag` requires
the candidate to be at least as fresh as the observed frontier.

Among eligible candidates, the policy prefers the highest frontier, then the
highest health score, then the lexical node name. Candidate slices are not
mutated, and no candidate produces an error merely because it is stale. If no
candidate meets the policy, the function returns `ErrNoEligibleReadReplica`.

The policy is deliberately separate from transport and endpoint discovery.
Callers can apply it to local, cross-region, or other read-routing choices
without silently adding network behavior.
