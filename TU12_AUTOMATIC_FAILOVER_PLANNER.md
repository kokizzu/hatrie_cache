# T-U12 Quorum-Backed Failover Planner

`hatTopology.PlanFailover` is an importable, side-effect-free admission
planner for health-triggered shard promotion. It incorporates three ideas
from the existing ClickHouse, Materialize, and Tarantool-inspired backlog:
explicit control-plane decisions, fencing before promotion, and failure-domain
aware replica safety.

## Default

The zero-value `FailoverPolicy` is disabled. It returns a non-promoting
`disabled` decision and does not require operator approval, quorum, or a
fencing token. Existing `ElectionStore` routing remains unchanged. Callers
must explicitly choose `FailoverModeAutomatic` or
`FailoverModeOperatorApproved`.

```go
policy := hatTopology.FailoverPolicy{
	Mode:                hatTopology.FailoverModeAutomatic,
	RequireFencingToken: true,
}

decision, err := hatTopology.PlanFailover(policy, request)
if err != nil {
	return err
}
if !decision.Allowed {
	return nil
}

// Revalidate and commit through the caller's promotion barrier before
// changing topology or serving writes from decision.To.
```

## Safety Rules

- A healthy current leader produces `leader_healthy` and no promotion.
- The default healthy-owner threshold is a strict majority. A caller may set
  `RequiredHealthy` explicitly within the owner count.
- `FailoverModeOperatorApproved` requires `OperatorApproved` on the request.
- `RequireFencingToken` requires equal, non-zero expected and observed tokens.
- `MinFailureDomains` counts distinct non-empty domains among healthy owners.
- Candidate order is primary followed by replicas, with duplicate owner IDs
  removed. The first healthy candidate is selected deterministically.
- The selected candidate must have an applied sequence at least as high as the
  source sequence. A lagging preferred candidate is rejected rather than
  silently skipping to a later candidate.

The planner returns typed sentinel errors for invalid policy, missing approval,
unsatisfied quorum, missing candidates, lagging candidates, fencing mismatch,
and insufficient failure-domain diversity. It does not send network requests,
mutate topology, advance a fencing token, or write cache data. The caller must
pair the decision with its existing `hatReplication.ReplicaPromotionBarrier`
or equivalent commit protocol and revalidate the token at commit time.

## Measurement

Environment: Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor. Five samples
were collected with the repository `benchmark-chg13` target and `-benchmem`.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Pre-change `ElectShardLeader` control | 49.10 | 48 | 1 | 1.00x |
| Final existing election control | 50.63 | 48 | 1 | 1.03x vs pre-change |
| Disabled `PlanFailover` | 33.53 | 0 | 0 | 0.68x vs pre-change |
| Enabled automatic `PlanFailover` | 144.1 | 0 | 0 | 2.94x vs pre-change |

The enabled planner is intentionally not substituted into the per-request
route lookup path. It is a control-plane check whose additional quorum,
sequence, fencing, and failure-domain safety is the measured cost. The final
implementation removed all common-path heap allocation; owner sets larger
than eight use a correctness-preserving fallback allocation.

Raw final samples:

```text
BenchmarkFailoverExistingElectionControl: 50.63 ns/op 48 B/op 1 allocs/op
BenchmarkFailoverExistingElectionControl: 51.20 ns/op 48 B/op 1 allocs/op
BenchmarkFailoverExistingElectionControl: 50.47 ns/op 48 B/op 1 allocs/op
BenchmarkFailoverExistingElectionControl: 50.02 ns/op 48 B/op 1 allocs/op
BenchmarkFailoverExistingElectionControl: 52.37 ns/op 48 B/op 1 allocs/op
BenchmarkPlanFailoverAutomatic: 145.1 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverAutomatic: 129.3 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverAutomatic: 131.4 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverAutomatic: 147.0 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverAutomatic: 144.1 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverDisabled: 32.09 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverDisabled: 31.39 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverDisabled: 33.53 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverDisabled: 34.10 ns/op 0 B/op 0 allocs/op
BenchmarkPlanFailoverDisabled: 35.75 ns/op 0 B/op 0 allocs/op
```

The intermediate allocation-removal attempt using a nested domain scan reduced
memory but measured about `199 ns/op`; it was replaced by the bounded stack set,
which reduced both memory and CPU. The final result is retained because it adds
promotion safety without adding heap pressure to the normal election path.

## Verification

The feature is covered by disabled, automatic, operator-approved, quorum,
catch-up, fencing, failure-domain, deterministic-order, leader-healthy,
invalid-policy, duplicate-owner, and larger-owner tests.

```text
make test-chg13
make test-chg13-package
make race-chg13
make vet-chg13
make benchmark-chg13
```
