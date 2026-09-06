# Failure-Domain-Aware Replica Placement

Topology nodes may declare a normalized `failure_domain`, such as a zone,
rack, or host. `ValidateFailureDomainPlacement` verifies that every affected
shard has the requested number of distinct, non-empty domains.

The CLI exposes the policy on both `cluster add-replica` and `cluster join`:

```text
hatrie-cli cluster add-replica \
  -id node-c \
  -address http://node-c \
  -failure-domain zone-c \
  -min-failure-domains 3
```

`-min-failure-domains 0` is the default and disables placement validation, so
existing topology files and commands retain their behavior. A positive value
requires the joining or replacement node to provide a domain and rejects a
placement that would leave any affected shard with too few distinct domains.
Topology JSON and native gRPC preserve the field for peer consistency.

This is a placement safety check, not automatic rebalancing. Operators still
choose the topology and must account for the failure domains of all replicas.
