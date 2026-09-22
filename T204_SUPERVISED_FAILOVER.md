# T204 Supervised Failover

T204 adds an opt-in operator-gated lifecycle around an existing
`AutomaticFailoverDecision`. It separates decision, human approval, topology
commit, and post-failover recovery so a proposed promotion cannot silently
become serving state.

## Lifecycle

```text
idle -> proposed -> approved -> committed -> recovering -> recovered
             |          |                         |
             +----------+                         +-> recovery_failed
                         |
                         +-> cancelled
```

The coordinator is transport- and storage-neutral. It starts no timer,
performs no network I/O, and does not publish topology. The embedding control
plane owns those effects.

```go
supervisor, err := hatReplication.NewSupervisedFailoverCoordinator(
	hatReplication.SupervisedFailoverOptions{Enabled: true},
)
proposal, err := supervisor.Propose(automaticDecision)

// Either action is explicit and recorded with the operator identity.
_, err = supervisor.Approve(proposal.Generation, "operator-a")
// Or: supervisor.Override(proposal.Generation, "on-call")

committed, err := supervisor.Commit(proposal)
recovering, err := supervisor.BeginRecovery(committed.Generation, "operator-a")
_, err = supervisor.CompleteRecovery(recovering.Generation,
	hatReplication.SupervisedFailoverRecoveryReport{
		NodeID:          "node-b",
		Healthy:         true,
		AppliedSequence: 100,
		FencingToken:    2,
	},
)
```

## Safety rules

- The zero-value configuration is disabled. Existing automatic/manual
  failover behavior remains unchanged until explicitly enabled.
- `Commit` returns `ErrSupervisedFailoverApprovalRequired` until a named
  operator calls `Approve` or `Override`. Override is auditable but does not
  bypass proposal equality, generation checks, or fencing checks.
- Every proposal is bound to a lifecycle generation. Reject, commit, recovery
  completion/failure, and reset advance the generation, so copied old actions
  cannot be replayed.
- Recovery completion requires the exact proposed candidate ID, exact fencing
  token, `Healthy: true`, and an applied sequence at least as high as the
  failed source sequence. A not-ready report leaves recovery in progress.
- Recovery failure is terminal until a named operator calls `Reset`. Reset is
  also required after cancellation before another proposal can start.
- The coordinator records operator identities and failure reasons for status or
  audit output; callers remain responsible for durable audit storage.

## Measurement

Five `GOMAXPROCS=1` samples were collected with `make benchmark-t204` on Linux
amd64, AMD Ryzen 9 5950X. The existing automatic evaluator is a control for a
single decision; each supervised row performs the complete proposal,
approval/override, commit, recovery, and reset lifecycle. The comparison is
therefore a control-plane cost comparison, not a claim that supervision makes
candidate selection faster.

| Workload | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing automatic-failover evaluation | 78.60 | 0 | 0 | 1.00x |
| Complete supervised approval lifecycle | 237.6 | 0 | 0 | 3.02x control |
| Complete supervised override lifecycle | 239.5 | 0 | 0 | 3.05x control |

The additional CPU is the bounded cost of recording operator and recovery
state; there is no allocation or default-path cost. The explicit approval and
recovery gates are the feature value, so this tradeoff is intentional.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
Existing evaluator: 78.13 84.54 79.00 76.54 78.60; 0; 0
Approval lifecycle:  237.6 230.3 239.6 236.6 246.5; 0; 0
Override lifecycle:  239.5 239.6 245.4 237.8 237.9; 0; 0
```

Focused behavior, package, race, vet, and documentation checks are exposed by
the T204 Makefile targets. The red lifecycle test was run before the API was
implemented and failed on the missing coordinator/types.
