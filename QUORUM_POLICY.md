# Write Quorum Policy

`hat/hatReplication` exposes `EvaluateWriteQuorum` as a small, explicit
replication-policy helper. It validates the replica counts and returns the
acknowledgement decision to the caller.

```go
decision, err := hatReplication.EvaluateWriteQuorum(
    replicaCount,
    acknowledgedCount,
    requiredAcknowledgements,
)
if err != nil {
    if errors.Is(err, hatReplication.ErrWriteQuorumUnsatisfied) {
        // The write did not reach the required durability level.
    }
    return err
}
fmt.Println(decision.Satisfied)
```

`total` must be positive. `acknowledged` must be between zero and `total`,
and `required` must be between one and `total`. `required` is deliberately
caller-defined so a deployment can count local and remote participants using
its own topology and failure policy.

An unsatisfied quorum returns both the decision and
`ErrWriteQuorumUnsatisfied`. Invalid inputs return the zero decision and
`ErrWriteQuorumInvalid`. The helper does not send network messages or mutate
replication state; it only makes the policy decision deterministic and easy
to test.
