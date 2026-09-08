# Explicit Regional Partition Routing

`hatPartition.PrefixRouter` provides a small immutable routing primitive for
deployments that partition keys by an explicit region or tenant prefix. It is
an operator-provided rule table, not automatic sharding or a network router.

## Usage

```go
router, err := hatPartition.NewPrefixRouter([]hatPartition.PrefixRule{
	{Prefix: "sg:", Partition: "sg"},
	{Prefix: "sg:vip:", Partition: "sg-vip"},
	{Prefix: "us:", Partition: "us"},
})
if err != nil {
	return err
}

partition, ok := router.Route("sg:vip:tenant:42")
// partition == "sg-vip", ok == true
```

Rules are normalized once, sorted by descending prefix length, and matched in
that order. This makes the most specific rule win while allowing broad rules
such as `sg:` alongside narrower rules such as `sg:vip:`. Prefix matching is
case-sensitive. Duplicate normalized prefixes and empty prefixes or partition
names are rejected at construction.

The zero value is disabled. A key with no matching prefix returns `("", false)`;
there is no implicit default partition, which prevents an omitted routing rule
from silently sending data to the wrong region. `Rules` returns an independent
normalized snapshot for inspection.

## Compatibility And Scope

- Existing `hatPartition.Index` hash routing is unchanged.
- No configuration default changes and no automatic router starts.
- `Route` performs no allocation and returns immutable rule data.
- The router does not contact peers, move data, elect owners, or enforce
  fencing. Callers must apply the returned partition to their own command,
  SQL-source, backup, and replica policies.
- Overlapping regions, ownership consensus, failover, migration, and HTTP/gRPC
  topology integration remain separate operational concerns.

## Benchmark

Command:

```text
make benchmark-prefix-router-local-clean
```

Machine: AMD Ryzen 9 5950X, Linux/amd64. The benchmark routes four repeated
keys through 16 explicit rules, including a missing-prefix lookup.

Raw samples:

| Sample | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 1 | 31.78 | 0 | 0 |
| 2 | 35.27 | 0 | 0 |
| 3 | 29.26 | 0 | 0 |
| 4 | 31.35 | 0 | 0 |
| 5 | 29.49 | 0 | 0 |

Median: `31.35 ns/op`, `0 B/op`, `0 allocs/op`. The route table is intended
for a small number of explicit regional rules; a future very-large rule set
would need a separate benchmark before replacing the compact sorted slice.
