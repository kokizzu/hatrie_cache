# C153d Partition-Ownership Vote Authentication

Partition-ownership consensus already validates shard identity, primary,
replica order, topology fingerprint, fencing token, and vote acceptance. C153d
adds an opt-in caller-owned authentication layer so a transport cannot turn an
unsigned or tampered vote into quorum input.

## API

Create an authenticator with a stable key identifier and a secret of at least
32 bytes:

```go
authenticator, err := hatTopology.NewPartitionOwnershipConsensusAuthenticator(
    "region-a-control-v1",
    controlPlaneSecret,
)
if err != nil {
    return err
}

signedVote, err := authenticator.Sign(vote)
if err != nil {
    return err
}

decision, err := hatTopology.EvaluateAuthenticatedPartitionOwnershipConsensus(
    policy,
    expectedOwnership,
    []hatTopology.PartitionOwnershipConsensusVote{signedVote},
    authenticator,
)
```

`Sign` covers the domain marker, key ID, node ID, shard ID, fencing token,
primary, replica order, topology fingerprint, and accepted flag with HMAC-SHA256.
`Verify` requires the exact key ID and a 32-byte signature, and rejects
non-canonical whitespace in identity fields. The authenticator copies the key
at construction so later caller mutation cannot change verification behavior.

The vote adds optional JSON fields `key_id` and `signature`. Existing unsigned
votes remain compatible with the legacy `EvaluatePartitionOwnershipConsensus`
function. The authenticated evaluator is deliberately separate: callers must
choose authentication and provide key distribution, rotation, and transport
replay policy.

## Security boundary

Authentication happens before deterministic quorum evaluation. A malformed,
unsigned, wrong-key, or tampered vote returns
`ErrPartitionOwnershipConsensusAuthentication`; it is not silently counted as
a failed vote. HMAC proves possession of the shared control-plane secret, but
does not provide node-specific public-key identity or non-repudiation. A
deployment that needs those properties must authenticate the transport or use
a caller-owned public-key layer around this API.

The signature binds replica order and fencing metadata, preventing an attacker
with a valid old vote from changing ownership fields without detection. Fresh
fencing tokens and replay/expiry checks remain the responsibility of the
existing consensus policy and caller-owned transport.

## Measured tradeoff

Command:

```text
make benchmark-c153d-ownership-auth
```

Five samples were collected on Linux/amd64 with `-benchmem`. Median results:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Legacy unsigned quorum evaluation | 761.8 | 848 | 9 |
| Authenticated sign + verify + quorum | 2,830 | 2,912 | 30 |

Authentication is approximately 3.71x slower and uses 3.43x as many bytes per
vote in this small control-plane fixture. That is an intentional security
cost, not a performance optimization. It is paid only by callers opting into
authenticated vote handling; ordinary routing and legacy consensus remain
unchanged.

Raw output is recorded in [BENCHMARK.md](BENCHMARK.md).
