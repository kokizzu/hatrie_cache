# T204 Supervised Failover

`hatReplication.SupervisedFailoverCoordinator` adds an explicit operator
approval and recovery state machine around the existing automatic failover
policy. It is disabled by default and has no network, storage, or background
goroutine side effects.

## Lifecycle

1. `Propose` evaluates the authenticated observation with the existing quorum,
   lag, candidate-selection, and fencing rules.
2. `Approve` requires the exact proposal, a non-empty operator identity, and a
   configured operator token compared in constant time.
3. `StartRecovery` consumes the automatic coordinator's exact commit fence and
   enters `recovering`. The caller performs topology promotion and data
   recovery in this phase.
4. `CompleteRecovery` records `recovered` only when the candidate, fencing
   token, topology generation, and applied sequence still match the proposal.
5. `Abort` records an operator identity and bounded reason. It cancels an
   uncommitted proposal, or records a failed recovery after the commit fence.

Every transition is one-shot and protected by the coordinator mutex. A stale
proposal, changed decision, duplicate transition, or mismatched recovery
result is rejected.

## Configuration

```go
coordinator, err := hatReplication.NewSupervisedFailoverCoordinator(
    hatReplication.SupervisedFailoverOptions{
        Enabled: true,
        Automatic: hatReplication.AutomaticFailoverOptions{
            QuorumSize: 2,
            MaxLag:     1,
        },
        AllowOperatorOverride: true,
        OperatorOverrideToken: []byte("at-least-16-bytes"),
    },
)
```

The outer `Enabled` flag is the feature switch. The zero value leaves the
feature disabled. If operator approval is enabled, the token must be 16 to 128
bytes. Do not place the token in logs, URLs, or unencrypted configuration.
The embedding service remains responsible for authenticating the operator and
delivering the token securely.

The coordinator does not promote a replica, open a listener, copy data, or
declare a node healthy. The caller must use its consensus/topology and
`ReplicaPromotionBarrier` paths, then report the verified result through
`CompleteRecovery` or `Abort`.

## Benchmark

The lifecycle benchmark compares the existing automatic proposal/commit path
with the opt-in supervised proposal/approval/start/complete path. Approval is
intentionally more expensive because it adds a constant-time bearer-token
check, an operator audit identity, and recovery state fencing. The default
automatic coordinator and its evaluator are unchanged.

See [BENCHMARK.md](BENCHMARK.md#t204-supervised-failover) and run
`make benchmark-t204` for the reproducible command.
