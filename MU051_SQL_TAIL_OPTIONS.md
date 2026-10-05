# M-U51 SQL `TAIL` Options

The SQL subscription envelope now accepts an optional trailing `WITH (...)`
clause. It exposes controls already supported by
`QuerySubscriptionDefinition` without adding a second subscription engine or
changing the existing plain `TAIL` syntax.

## Syntax

```sql
TAIL FROM CACHE('items')
SELECT id
WITH (
  SNAPSHOT = false,
  PROGRESS = true,
  AS OF = 4,
  UP TO = 9,
  DETERMINISTIC = true
)
```

Supported options are:

| Option | Value | Definition field | Meaning |
| --- | --- | --- | --- |
| `SNAPSHOT` | `true` or `false` | `StartLive = !SNAPSHOT` | Emit the initial snapshot when true; start from the next relevant refresh when false. |
| `PROGRESS` | `true` or `false` | `EmitProgress` | Request progress/heartbeat events from the existing subscription engine. |
| `AS OF` | unsigned integer | `AsOf` | Start at the requested revision/frontier. |
| `UP TO` | unsigned integer | `UpTo` | Stop at the requested revision/frontier. |
| `DETERMINISTIC` | `true` or `false` | `DeterministicOrder` | Request canonical ordering for differential removal/addition batches. |

The parser also accepts the existing forms unchanged:

```sql
TAIL FROM CACHE('items') SELECT id
SUBSCRIBE FROM CACHE('items') SELECT id
SUBSCRIBE SNAPSHOT FROM CACHE('items') SELECT id
SUBSCRIBE DIFFERENTIAL FROM CACHE('items') SELECT id
```

The options are parsed by `ParseSQLSubscriptionStatement` and can then be
passed to the existing `SubscribeSQL` or `SubscribeDifferentialSQL` path.
Query execution, dependency resolution, authentication, transport, and
durable publication remain caller-owned. `AS OF` must not be greater than a
non-zero `UP TO`; duplicate, unknown, malformed option envelopes, and
invalid-valued options are rejected.

## Cost

Measured on `linux/amd64`, AMD Ryzen 9 5950X, with five `go test -bench`
samples per benchmark. The pre-change baseline is the same parser before the
option envelope was added.

| Parser path | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Legacy `TAIL`, before M-U51 | 20.12 | 0 | 0 | 1.00x |
| Legacy `TAIL`, after M-U51 | 17.96 | 0 | 0 | 1.12x faster |
| `TAIL` with all five options | 1,156 | 704 | 15 | 64.4x slower than legacy |

The legacy result stays allocation-free because the parser returns before
option scanning when the statement does not end in an option envelope. The
option form is intended for registration/configuration, not a per-row data
path; its extra parse cost is paid once per subscription definition.
