# T-U06 Replica-Wide Read-Only Admission

`hatReplication.ReplicaWriteGate` is an opt-in, process-local admission primitive
for putting a replica into read-only mode. The zero-value options are writable,
so existing callers are unchanged until they explicitly create and call the
gate.

## Usage

Create one gate for the mutation surface that it protects and check it before
accepting a write:

```go
gate, err := hatReplication.NewReplicaWriteGate(
	hatReplication.ReplicaWriteGateOptions{},
)
if err != nil {
	 return err
}

if err := gate.Admit(hatReplication.ReplicaWriteExternal); err != nil {
	 return err
}
// Apply the mutation after admission succeeds.
```

Enable read-only mode with a reason. Every successful transition increments a
generation:

```go
state, err := gate.SetReadOnly("planned maintenance")
if err != nil {
	 return err
}

// Later, use the generation observed during the transition. A stale generation
// cannot accidentally make the replica writable after another transition.
_, err = gate.SetWritable(state.Generation)
```

`Admit(ReplicaWriteExternal)` returns `ErrReplicaWriteBlocked` while the gate
is read-only. `Snapshot` exposes the current state for status endpoints and
operator tooling.

## Replication and Operator Overrides

`Admit(ReplicaWriteInternalReplication)` is an explicit exception for a trusted
replication apply path. It is not authentication, authorization, or fencing.
The caller must authenticate the replication channel and validate its replication
epoch/lease before selecting this origin. Never derive the origin from an
untrusted request field.

An operator override can be enabled in `ReplicaWriteGateOptions` with a secret
token. `AdmitOperatorOverride` compares the supplied token in constant time and
allows the explicitly configured override when the gate is read-only. Tokens
must be 16 to 128 bytes. Keep them in a secret manager or protected process
configuration; do not log, expose, or put them in URLs.

The gate is process-local and has no automatic integration with HTTP, gRPC,
storage, or replication transports. Every mutation entry point that needs the
policy must call it, and distributed state must be coordinated by the caller.

## Configuration and Errors

`DefaultReadOnly` starts a new gate in read-only mode. `DefaultReason` is
required when that option is enabled and is limited to 256 bytes. The default
configuration is writable and has no operator override.

Invalid origins, malformed options, disabled overrides, bad tokens, stale
generations, and blocked external writes return distinct exported errors so
callers can expose the right operational status without string matching.

## Measurement

The focused benchmark runs five samples per case on an AMD Ryzen 9 5950X,
`linux/amd64`, with `ReportAllocs` enabled:

| Admission path | Median | Allocations |
| --- | ---: | ---: |
| Bare boolean baseline | 0.243 ns/op | 0 B/op, 0 allocs/op |
| Writable external admission | 3.795 ns/op | 0 B/op, 0 allocs/op |
| Blocked external admission | 3.734 ns/op | 0 B/op, 0 allocs/op |

The gate adds about 3.5 ns to this isolated read-lock check and no heap
allocation. The bare boolean loop is only a lower-bound reference; it does not
represent a real mutation path. Contention, state transitions, transport work,
and the protected storage operation are outside this microbenchmark.

Run the reproducible benchmark with:

```text
make benchmark-tu06-dev
```

The broader comparison and raw output are tracked in
[BENCHMARK.md](BENCHMARK.md#t-u06-replica-wide-read-only-admission).
