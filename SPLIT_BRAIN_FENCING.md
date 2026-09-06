# Split-Brain Fencing

The topology fencing token is an operator-controlled stale-writer guard for
replication. Set a non-zero token when promoting a new topology or fencing an
old writer. `TopologyStore` accepts only monotonic token updates, and the token
is included in the topology fingerprint and replication envelopes.

Receivers reject a replication command or batch when its non-zero token does
not match the local topology token, before applying the mutation. Missing or
mismatched tokens are therefore rejected whenever fencing is enabled. Token
`0` preserves the legacy unfenced behavior for backward compatibility.

This mechanism prevents an old writer from applying writes after an operator
has advanced the topology. It is not quorum consensus and does not elect a
leader by itself; pair it with the existing election, quorum, and operational
fencing procedures when stronger coordination is required.

The token is carried through HTTP command replication, batched envelopes,
native gRPC replication, and topology persistence. Tests cover monotonic
updates, fingerprint and JSON round trips, stale and missing writers, batch
preservation, and gRPC propagation.
