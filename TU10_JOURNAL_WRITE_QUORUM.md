# T-U10: Journal-Wide Synchronous Write Quorum

T-U10 adds an opt-in `hatReplication.JournalWriteQuorum` coordinator for
writes that must be acknowledged by a configured replica quorum before the
caller treats the journal sequence as durable.

The coordinator is transport-neutral. The caller obtains the journal sequence
after its normal append/fsync step, supplies the current fencing token, and
uses `Execute` to collect acknowledgements over HTTP, gRPC, or another control
plane. The coordinator does not append journal records, retry indefinitely, or
publish topology state.

## Contract

```go
quorum, err := hatReplication.NewJournalWriteQuorum(
	hatReplication.JournalWriteQuorumOptions{
		Enabled: true,
		Voters:  []string{"node-a", "node-b", "node-c"},
		// Required: 0 means strict majority, so this is 2.
	},
)
if err != nil {
	return err
}

decision, err := quorum.Execute(ctx,
	hatReplication.JournalWriteQuorumProposal{
		Sequence:   journalSequence,
		FenceToken: currentFenceToken,
	}, func(ctx context.Context, node string, proposal hatReplication.JournalWriteQuorumProposal) (hatReplication.JournalWriteQuorumAcknowledgement, error) {
		return sendDurableJournalAck(ctx, node, proposal)
	})
if err != nil {
	return err
}
_ = decision
```

The import alias in the example is intentionally shortened in surrounding
application code as needed; the package path is `hatrie_cache/hat/hatReplication`.
Each successful acknowledgement must contain the exact proposal sequence and
fence token. Unknown or duplicate voters are rejected. Missing, failed, stale,
or explicitly rejected acknowledgements never count toward the quorum.

`Required: 0` selects a strict majority. `Enabled: false` is the default and
returns a reusable disabled coordinator; the existing asynchronous replication
path remains unchanged. A disabled coordinator does not validate voters or
start goroutines.

## Tradeoffs

| Concern | Behavior | Operational consequence |
| --- | --- | --- |
| Durability | The caller waits for the configured quorum | A write can fail when enough replicas are unavailable |
| Latency | Acknowledgements are collected concurrently | Completion is bounded by the slowest required response and transport timeout |
| Availability | Strict majority is the default when enabled | Network partitions reject writes instead of silently weakening durability |
| CPU/memory | Exact sequence/fence validation is allocation-free; execution allocates callback state | The cost is paid only by enabled synchronous writes |
| Recovery | The returned proposal is bound to the journal sequence and fence token | Callers can retry the same proposal or repair failed replicas without accepting stale acknowledgements |

The coordinator does not provide consensus by itself. The embedding service
must choose voters, persist or otherwise protect fence tokens, set deadlines,
and decide how to repair replicas after a quorum succeeds while another target
fails.

## Measurements

Machine: AMD Ryzen 9 5950X, Linux/amd64. Five `-benchmem` samples.

| Path | Median ns/op | B/op | Allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Existing counter-only `EvaluateWriteQuorum` baseline | 2.275 | 0 | 0 | reference only; it does not validate sequence/fence metadata |
| Exact journal proposal evaluation, 3 voters | 42.53 | 0 | 0 | 18.69x CPU cost versus the counter-only check |
| Disabled coordinator check | 0.4965 | 0 | 0 | 4.58x faster than the counter-only check; default path does no quorum work |
| Three parallel acknowledgements, 2 required | 1,715 | 755 | 10 | 1.04x the existing generic three-target executor, +211 B, same allocations |

The evaluation overhead is deliberate validation cost, not an optimization
claim. The 18.69x ratio compares different semantics: the old path compares
three integer counts, while T-U10 also checks voter identity, duplicate
responses, journal sequence, and fencing metadata. Network and fsync latency
will dominate the opt-in execution path in a real deployment.

Reproduce the measurements with:

```text
make baseline-tu10-write-quorum
make benchmark-tu10-write-quorum
make benchmark-generic-write-quorum
```

The focused correctness and race tests cover disabled defaults, configuration
validation, strict-majority selection, exact sequence/fence binding, stale
acknowledgements, duplicate and unknown voters, concurrent callback execution,
and cancellation.
