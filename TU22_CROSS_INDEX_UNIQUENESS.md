# T-U22 Cross-Index Unique Constraints

`hatDataStructure.UniqueConstraintSet[T, K]` is an importable, opt-in
constraint registry for one row set with multiple unique indexes. It is useful
when a write must reserve several keys atomically, such as both an email and a
username. The registry tracks IDs and constraint keys; the caller retains the
actual row values.

## API

```go
type Account struct {
	Email    string
	Username string
}

constraints, err := hatDataStructure.NewUniqueConstraintSet[Account, string](
	[]hatDataStructure.UniqueConstraint[Account, string]{
		{Name: "email", Key: func(account Account) (string, bool) {
			return account.Email, account.Email != ""
		}},
		{Name: "username", Key: func(account Account) (string, bool) {
			return account.Username, account.Username != ""
		}},
	},
)
if err != nil {
	return err
}

if err := constraints.Insert(7, account); err != nil {
	return err
}
if err := constraints.Upsert(7, replacement); err != nil {
	// Keep the caller's row unchanged when a constraint conflicts.
	return err
}
exists := constraints.Contains(7)
removed := constraints.Delete(7)
```

`Insert` rejects duplicate IDs. `Upsert` derives every key and checks every
owner before releasing the lock or changing any index. A conflict returns
`UniqueConstraintViolationError`, which unwraps to
`ErrUniqueConstraintViolation` and names the failing constraint. A false key
extractor result means that the row does not participate in that constraint;
this is useful for nullable unique fields. `Reset` retains allocated maps for
reuse.

The registry is concurrency-safe. Key extractors run before the registry lock,
so they should be deterministic and side-effect free. Update the caller-owned
row only after a successful `Upsert`; a failed operation leaves the registry
unchanged.

## Scope And Tradeoff

This is a reusable primitive, not automatic schema or SQL integration. The
caller owns row storage, persistence, replication, and wiring the registry into
its write transaction. Multiple fields must use one comparable key type `K`;
applications with unrelated key types can use separate registries.

The registry is retained for atomicity and correctness, not as a faster map.
Existing map-backed storage is unchanged and pays no cost. The benchmark
below measures the extra opt-in structure against hand-written maps with the
same ID-to-key reverse state.

## Measurements

Measured on Linux/amd64, AMD Ryzen 9 5950X, five `-benchmem` samples, with two
unique string constraints and 256 rows.

| Workload | Hand-written control | Constraint set | Relative result | Memory |
| --- | ---: | ---: | --- | --- |
| ID containment lookup | 7.267 ns/op | 12.39 ns/op | 1.71x slower | 0 -> 0 B/op; 0 -> 0 allocs/op |
| Successful 256-row atomic build | 43,556 ns/op | 122,506 ns/op | 2.81x slower | 58,440 -> 113,416 B/op; 835 -> 1,130 allocs/op |

The cost is bounded to callers that opt into cross-index atomicity. It should
not replace a plain map when uniqueness is not required.

Raw samples:

```text
BenchmarkTU22BaselineIDLookup-32       7.240  7.406  7.203  7.267  7.524 ns/op  0 B/op  0 allocs/op
BenchmarkTU22BaselineAtomicInsert-32  40820 43443 44968 44456 43556 ns/op  58440 B/op 835 allocs/op
BenchmarkTU22UniqueConstraintSetLookup-32 12.39 11.70 11.77 13.21 12.85 ns/op  0 B/op 0 allocs/op
BenchmarkTU22UniqueConstraintSetInsert-32 122506 123435 124172 122349 122468 ns/op 113416 B/op 1130 allocs/op
```

Reproduce with `make benchmark-tu22`.
