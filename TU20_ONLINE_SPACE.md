# T-U20 Online Space Upgrade

`hatSchema.OnlineSpace` is an importable, opt-in Tarantool-style online space
upgrade primitive. It keeps the current row representation serving while a
background converter prepares target rows, accepts writes during conversion,
and performs one atomic definition/row cutover when all rows are ready.

## Usage

```go
space, err := hatSchema.NewOnlineSpace(initial, func(row hatSchema.Row) (string, error) {
	return row["id"].(string), nil
})
if err != nil {
	return err
}

upgrade, err := space.BeginUpgrade(ctx, target, func(ctx context.Context, row hatSchema.Row) error {
	row["email"] = row["id"].(string) + "@example.test"
	return nil
})
if err != nil {
	return err
}
if err := upgrade.Wait(ctx); err != nil {
	return err
}
```

The converter receives an owned row copy and must mutate it in place without
retaining it. It should honor the context so cancellation can stop slow work.

## Guarantees

- Only strictly higher, schema-compatible versions are accepted.
- Reads continue returning the current schema until cutover.
- Writes continue during conversion; each write prepares both representations.
- A conversion error or cancellation discards all target rows and leaves the
  current definition and rows unchanged.
- Cutover changes the definition and row representations under one short lock.
- `Status` reports phase, total rows, converted rows, and terminal errors.
- Existing storage, replication, backup, and `MaterializedSource` paths are
  unchanged; callers own persistence and durable resume of an upgrade.

## Measured Tradeoff

Machine: AMD Ryzen 9 5950X, Linux amd64. Five-run medians over 4,096 rows;
fixture setup and final row collection were outside the timed interval.

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Synchronous conversion baseline | 1,217,753 | 1,564,867 | 16,385 |
| Background online upgrade | 2,271,555 | 1,616,888 | 16,689 |
| Background / baseline | 1.87x time | 1.03x bytes | 1.02x allocations |

The online path spends more CPU on coordination and lock-safe publication, but
keeps memory nearly flat and removes the write-blocking conversion pause. It is
therefore opt-in rather than a replacement for ordinary synchronous migration.

## Verification

- Focused online-upgrade tests passed.
- Full `hat/hatSchema` tests passed.
- `hat/hatSchema` race tests passed.
- `go vet ./hat/hatSchema` passed.
- Repository-wide `go test ./...` was attempted in an isolated cache and the
  new `hatSchema` package passed; unrelated pre-existing failures remain in
  root API declarations, SQL/cache/data-structure tests, compact protocol, and
  a peer TLS test timeout.
