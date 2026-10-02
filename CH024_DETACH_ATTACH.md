# CH-024 Detach/Attach Parts

`hatStorage.RemotePartAttachmentCatalog` is an opt-in control-plane registry
for immutable remote-part references. It makes operator repair state explicit
without moving or deleting bytes.

## Behavior

- `Attach` verifies and publishes an initial active reference.
- `Detach` atomically marks the current reference quarantined and records a
  bounded operator reason. It is idempotent for the same current generation.
- `AttachReplacement` requires the quarantine generation, verifies the
  replacement before publication, then atomically makes it active.
- A stale generation cannot revive a part after another operator completed a
  repair.
- Verification runs outside the catalog lock; the callback owns object-store
  or filesystem I/O, authentication, retries, and checksum policy.
- The catalog does not delete, move, persist, or replicate data. Callers that
  use `RemotePartCache` should invalidate the quarantined reference and route
  subsequent reads through the active catalog entry.
- Existing cache and storage APIs are unchanged; the catalog has no default
  runtime cost.

## Measurement

The benchmark uses immutable references and a no-op verifier after setup. All
measurements use five runs on the same AMD Ryzen 9 5950X Linux amd64 host.

| Operation | Median ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | ---: |
| Raw map lookup | 10.17 | 0 | 0 | 1.00x |
| Locked map lookup | 17.25 | 0 | 0 | 1.70x raw |
| Attachment catalog lookup | 40.60 | 0 | 0 | 2.35x locked |
| Detach plus verified replacement | 514.5 | 192 | 2 | Explicit operator action |

Raw baseline runs:

```text
raw lookup:    10.22, 10.15, 9.604, 10.17, 10.49 ns/op
locked lookup: 17.52, 16.83, 17.45, 16.73, 17.25 ns/op
```

Raw catalog runs:

```text
lookup:        40.41, 40.45, 40.79, 40.60, 40.62 ns/op; 0 B/op; 0 allocs/op
detach/attach: 482.6, 518.2, 486.4, 514.5, 523.6 ns/op; 192 B/op; 2 allocs/op
```

The lookup overhead is the deliberate validation and synchronized snapshot
boundary. It is not inserted into existing hot reads. The detach/attach cost
is an operator workflow and includes reference normalization plus generation
publication.

Focused commands:

```text
make format-round19-ch024-detach
make test-round19-ch024-detach
make race-round19-ch024-detach
make vet-round19-ch024-detach
make benchmark-round19-ch024-detach
```
