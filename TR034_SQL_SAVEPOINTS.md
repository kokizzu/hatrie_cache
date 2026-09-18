# TR-34 SQL Savepoints

Status: implemented.

`hatCache` supports savepoints in two transaction surfaces:

- `CompileSQL` accepts `SAVEPOINT name`, `ROLLBACK TO name`, and `RELEASE SAVEPOINT name` inside `BEGIN ATOMIC ... COMMIT`.
- `SQLTransaction.Savepoint`, `RollbackTo`, and `ReleaseSavepoint` provide savepoints for snapshot-isolated SQL transactions.

Savepoint names are case-insensitive identifiers containing letters, digits, or
underscores. `RollbackTo` keeps the named savepoint and removes later
savepoints; `ReleaseSavepoint` removes only the named savepoint. Staged writes
remain private until `Commit`, so rolling back a savepoint cannot publish a
partial transaction.

The current implementation favors correctness and isolation over minimum
allocation cost: every runtime savepoint serializes the private snapshot with
the existing gzip-binary snapshot format, loads a private `HatTrie`, and
retains that snapshot until rollback, release, or transaction close. It is
therefore appropriate for bounded savepoint use, not for a savepoint per row.
An undo-log or copy-on-write checkpoint could reduce this cost, but would need
separate correctness and recovery validation before replacing the current
path.

The measured cost is recorded in [BENCHMARK.md](BENCHMARK.md#tr-034-sql-savepoints).
Focused checks are available through `make test-tr034-savepoint`,
`make race-tr034-savepoint`, `make vet-tr034-savepoint`, and
`make benchmark-tr034-savepoint`.
