# TR-47 Object-Scoped RBAC Grants

Hatrie Cache already supports bearer authentication and rules scoped to
commands, key namespaces, and SQL sources. This feature adds an optional
`objects` selector so an authenticated principal can be restricted to one
cache object or one SQL source instead of receiving access to every object
covered by a command rule.

## Compatibility And Defaults

Object filtering is opt-in. Omitting `objects` preserves the existing rule
behavior. An empty `Policy` remains an unrestricted policy for compatibility;
once roles are configured, unmatched requests are denied.

The public API is available from `hat/hatAuth`:

```go
allowed := policy.AuthorizeObject(
	"reader-token",
	"GET",
	"tenant-eu:people",
	"",
	"tenant-eu:people",
)
```

`AuthorizeRequest` is available when a caller has structured dimensions. The
legacy `Authorize` method remains available and deliberately passes an empty
object. A rule containing `objects` therefore cannot be bypassed by an older
call site.

## Rule Shape

The policy types are JSON-serializable and can be assembled by an embedding
application like this:

```json
{
  "principals": {
    "reader-token": ["reader"]
  },
  "roles": [
    {
      "name": "reader",
      "rules": [
        {
          "commands": ["GET"],
          "namespaces": ["tenant-eu:*"],
          "objects": ["tenant-eu:people"]
        }
      ]
    }
  ]
}
```

Selectors in one rule are ANDed. Multiple values in one selector are ORed,
and multiple rules are ORed. `*` matches everything and a trailing `*` is a
prefix match. Command selectors are case-insensitive; namespace, source, and
object selectors preserve the existing case-sensitive matching behavior.

## HTTP And gRPC Commands

For `/api/commands` and the native gRPC command methods, `objects` matches the
trimmed command key. The existing namespace dimension also receives that key,
so a rule can use either a broad namespace or an exact object selector.

```text
Rule: commands=[GET], objects=[tenant-eu:people]
GET tenant-eu:people  -> allowed
GET tenant-eu:users   -> forbidden
SET tenant-eu:people  -> forbidden
```

`BATCH` is checked recursively. Every nested command must match a rule before
any batch command is executed; a single unauthorized object rejects the whole
batch.

## SQL Queries

For SQL HTTP queries, the object dimension is the referenced `CACHE(...)`
source name. When a query references multiple sources, every source must be
authorized. A source-scoped rule can therefore grant a specific table-like
cache without granting access to other sources.

```text
Rule: commands=[SQL], objects=[tenant-eu:people]
SELECT ... FROM CACHE('tenant-eu:people') -> allowed
SELECT ... FROM CACHE('tenant-eu:users')  -> forbidden
```

SQL queries without a source have an empty object dimension. They remain
available to ordinary `SQL` rules, but fail closed for object-only rules.

SQL RowBinary imports receive their keys from the request stream, so there is
no single object to authorize before decoding and applying the body. An
object-only rule rejects such an import. Use a broader `SQL` rule when the
caller is trusted to import arbitrary keys, and retain body-size and write
protection limits.

## Security Properties

- Authentication still happens before RBAC and bearer tokens are not treated
  as object names.
- Blank objects cannot match a non-empty `objects` selector.
- Prefix matching is bounded to the selector value and does not perform
  substring matching.
- Mixed HTTP or gRPC batches cannot use one authorized object to authorize a
  different nested object.
- SQL source checks are performed for every referenced source.
- Policy administration is intentionally outside this feature; applications
  should load and validate policy through their existing configuration and
  deployment process.

## Measured Cost

The focused benchmark ran five samples on the local AMD Ryzen 9 5950X host
with `go test -count=5 ... -benchmem`:

| Check | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Legacy rule without objects | 81.25 | 0 | 0 | 1.00x |
| Command object rule | 97.96 | 0 | 0 | 1.21x |
| SQL source-object rule | 85.66 | 0 | 0 | 1.05x |

The absolute check cost remains below 0.1 microsecond and adds no heap
allocation. The command-object path is about 21% slower than the legacy
selector path in isolation; request parsing, command execution, and SQL
execution dominate real endpoint latency. See the raw samples in
[`BENCHMARK.md`](BENCHMARK.md#tr-047-object-scoped-rbac-grants).

## Verification

```text
make format-tr047-object-grants
make test-tr047-object-grants
make benchmark-tr047-object-grants
```

The tests cover legacy compatibility, exact object grants, SQL source grants,
mixed HTTP and gRPC batches, and fail-closed RowBinary imports.
