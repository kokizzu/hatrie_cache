# Quorum Policy

`hat/hatReplication` exposes a small, reusable quorum-policy layer for callers
that already have replica acknowledgement counts. It makes read and write
thresholds explicit without changing the existing asynchronous replication
default.

```go
policy, err := hatReplication.NewDefaultQuorumPolicy(5)
if err != nil {
	return err
}

read, err := policy.EvaluateRead(3)
if err != nil || !read.Satisfied {
	return err
}

write, err := policy.EvaluateWrite(3)
if err != nil || !write.Satisfied {
	return err
}
```

`NewDefaultQuorumPolicy` uses a majority for both reads and writes. Use
`NewQuorumPolicy(total, readRequired, writeRequired)` when the deployment has
an explicit availability policy. All thresholds must be positive and no
greater than `total`; acknowledgement counts must be between zero and
`total`. Invalid configuration and unsatisfied quorum results are returned as
typed, `errors.Is`-compatible errors.

The policy is an evaluation primitive: it evaluates acknowledgements supplied
by the caller and does not send network requests, wait for replicas, or change
the current asynchronous replication path. A caller requiring read/write
intersection should choose thresholds where `readRequired + writeRequired >
total`.

The focused, full, and race verification is reproducible with:

```sh
make verify-t058
```
