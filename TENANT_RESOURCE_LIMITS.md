# Tenant Resource Limits

`NamespaceQueryGovernor` provides per-namespace policies that can represent a
tenant, user, or source. Namespace overrides only tighten the defaults before
the query enters the executor. Supported limits include rows, join work and
bytes, result bytes, workers, sort/group/set/spill bytes, recursion depth,
concurrency, queued waiters, request quotas, and timeout.

The policy is immutable after construction, and the namespace map is copied so
later caller mutations cannot change enforcement. `make
test-tenant-resource-limits` verifies the tightening behavior; the full suite
and race target cover the execution path.
