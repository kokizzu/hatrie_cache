# CH-U33: Quorum Remote-Part Publication

## Purpose

Remote-part replication can now collect acknowledgements for one exact part
before making that part visible. The proposal binds the publication to:

- a caller-supplied publication ID;
- the expected manifest fingerprint;
- the complete remote-part reference;
- a monotonically increasing fencing token; and
- a deterministic voter set and acknowledgement threshold.

This is an opt-in storage primitive. Existing asynchronous replication and
direct remote-part writes are unchanged.

## Contract

`NewRemotePartPublicationProposal` validates and normalizes the reference and
voter set. `Required: 0` means a strict majority of `Voters`.

Each voter response must echo the publication ID, manifest fingerprint,
candidate fingerprint, and fencing token. Unknown voters, duplicate votes, and
any mismatched identity are rejected rather than counted as negative votes.

`PublishRemotePartAfterQuorum` performs these checks in order:

1. validate the proposal and context;
2. reject a fencing token that is not newer than the caller's last accepted
   token;
3. evaluate the supplied votes;
4. call the injected `RemotePartPublicationStore` only after quorum is
   satisfied; and
5. return the store result.

The store adapter owns durable persistence and must not expose the part before
`PublishRemotePart` succeeds. Discovery, transport, retries, leases, and
rollback remain caller-owned. A failed store call does not claim publication
success and does not attempt rollback because the adapter controls its own
transaction boundary.

## Example

```go
proposal, err := hatStorage.NewRemotePartPublicationProposal(reference, hatStorage.RemotePartPublicationOptions{
    PublicationID:                "parts-2026-09-19-0007",
    ExpectedManifestFingerprint: "sha256:manifest-fingerprint",
    FencingToken:                 42,
    Voters:                       []string{"node-a", "node-b", "node-c"},
    Required:                     0, // strict majority: two acknowledgements
})
if err != nil {
    return err
}

result, err := hatStorage.PublishRemotePartAfterQuorum(
    ctx,
    store,
    proposal,
    votes,
    lastAcceptedFencingToken,
)
if err != nil {
    return err
}
if !result.Published {
    return errors.New("remote part was not published")
}
```

The candidate fingerprint is available through
`proposal.CandidateFingerprint()` and should be included in voter requests.

## Verification

Focused coverage is in
`hat/hatStorage/chu33_remote_part_publication_test.go`. It verifies quorum
gating, strict-majority defaults, deterministic voter ordering, mismatched and
duplicate vote rejection, fencing-token rejection, cancellation, and store
failures.

Run:

```text
make format-chu33
make test-chu33
make test-chu33-package
make race-chu33
make vet-chu33
make benchmark-chu33
```

## Benchmark

Machine: Linux/amd64, AMD Ryzen 9 5950X. Five benchmark samples were run by
`make benchmark-chu33`; the direct control is an inlined no-op store and does
not represent network or durable-storage latency.

| Path | Raw ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct no-op publication | 0.2637, 0.2468, 0.2433, 0.2414, 0.2380 | 0.2433 | 0 | 0 |
| Quorum publication, 3 voters / 2 required | 2151, 2198, 2169, 2124, 2161 | 2161 | 930 | 24 |

The quorum path is control-plane work performed before the real store call, so
the meaningful comparison is its fixed CPU/allocation budget rather than the
inlined control. Removing duplicate proposal validation improved the quorum
path from a 3,739 ns/op median, 1,715 B/op, and 45 allocs/op to 2,161 ns/op,
930 B/op, and 24 allocs/op: 1.73x faster, 1.85x less memory, and 1.88x fewer
allocations with the same tests and semantics.
