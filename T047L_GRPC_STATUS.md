# T047L: gRPC Participant Status Read

## Why

The gRPC cluster-write transport could prepare, commit, and abort a proposal,
but a recovery coordinator had no authenticated way to ask a participant what
phase it had durably recorded. That forced recovery code to guess or use a
separate transport.

## Design

The existing `ClusterWriteCommit` unary RPC now accepts a `STATUS` phase. It
does not add another RPC or change the prepare, commit, or abort request shape.
Status requests contain only a transaction ID. Successful responses return:

- `found=false` when no participant record exists;
- the proposal sequence, fence token, and 32-byte payload digest when found;
- a typed participant phase: prepared, committed, or aborted.

The same replication authorization metadata is required. The server rejects
proposal fields on a status request, trims and bounds transaction IDs, and does
not mutate participant state. The client rejects mismatched IDs, malformed
digests, and unknown participant phases.

## Wire Example

```text
request:
  phase: STATUS
  transaction_id: tx-42

response:
  ok: true
  phase: STATUS
  found: true
  transaction_id: tx-42
  sequence: 7
  fence_token: 11
  payload_digest: 32 bytes
  participant_phase: PREPARED
```

An absent record returns `ok=true`, `phase=STATUS`, and `found=false` without
exposing proposal metadata.

## Benchmark

Command: `make benchmark-t047-grpc-transport`

Five samples were collected on Linux/amd64, AMD Ryzen 9 5950X. The values below
use the sample medians from that run.

| Path | ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Direct participant status | 19.60 | 0 | 0 | baseline |
| Authenticated gRPC status | 32,664 | 12,492 | 175 | 1,667x direct latency; transport cost |
| Existing gRPC prepare+commit | 62,168 | 24,530 | 348 | status is 1.90x lower latency, 1.96x lower bytes, 1.99x fewer allocations |

The status path is an additional recovery capability, not a replacement for the
write phases. Its tradeoff is one authenticated unary round trip and response
metadata; in return, recovery gets an authoritative participant record without
replaying a write phase.

## Verification

Passed:

- `make generate-proto`
- `make format-t047-grpc-transport`
- `make test-t047-grpc-transport`
- `make race-t047-grpc-transport`
- `make benchmark-t047-grpc-transport`

The package-wide gRPC target also exercises unrelated SQL tests that are being
changed in parallel; those currently fail outside this feature. The focused
transport and race targets pass.
