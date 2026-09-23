# T203 Strict Leader Fencing

Strict leader fencing prevents a write carrying an old topology generation
from being accepted after a replica failover. It is an opt-in safety feature;
the default remains unchanged and `EnforceLeaderFencing` is `false`.

## Enablement

Set the option on the server that accepts public commands:

```go
monitoring := hatCache.MonitoringOptions{
	EnforceLeaderFencing: true,
}
```

For the gRPC server, set the corresponding
`hatCache.CacheGRPCOptions.EnforceLeaderFencing` field. The option is passed
through to the command executor and applies to both HTTP/monitoring and gRPC
command paths.

Strict fencing also requires the normal topology and election dependencies.
Each write must be routed to the currently elected leader and must carry the
current topology fencing token. A missing, malformed, or stale token is
rejected before journaling or mutation.

## Client Token

The reserved pair key is:

```text
_hatrie_replication_fencing_token
```

Go clients should use the exported `hatCache.LeaderFencingTokenPair` constant
instead of repeating the string. The value is the decimal topology fencing
generation returned by the authoritative topology/control plane. Do not
invent or increment the value in the client.

An HTTP-like command request therefore carries the token in `pairs`:

```json
{
  "command": "SETSTR",
  "key": "account:42",
  "value": "active",
  "pairs": {
    "_hatrie_replication_fencing_token": "7"
  }
}
```

The fencing pair is metadata. It is removed before the command reaches the
trie and is not persisted as user data or forwarded as an ordinary user pair.

## Replication Behavior

Replication-generated internal writes already carry the same topology
generation. With strict fencing enabled, internal writes without the reserved
pair, or with a different generation, are rejected before mutation. This
protects the receiving node when a delayed replication request arrives after
topology advancement.

Scalar and structured gRPC batch protobuf messages do not have a pair map.
When strict fencing is enabled they are routed through the compatibility path
and cannot silently bypass validation; callers that need strict fencing must
use the generic command or batch request path that carries `pairs`.

## Safety Boundary

Fencing is a stale-writer guard, not a consensus protocol. The control plane
must still publish topology generations, route clients to the elected leader,
and ensure that nodes receive topology updates. A node with an old local view
cannot discover a newer generation by itself. The token must be obtained from
the authoritative topology source and refreshed after a rejected request.

## Verification

Focused checks:

```text
make test-t203
make race-t203
make vet-t203
make benchmark-t203
```

The tests cover stale, missing, and current public tokens, default-off
compatibility, mutation-before-validation ordering, and missing internal
replication metadata.
