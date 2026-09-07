# Schema Constraints

`hatSchema` defines typed schema constraints and provides explicit validation
for complete datasets or one source at a time. Foreign keys are enforced when
the caller invokes `ValidateDataset` or `ValidateRows`; declaring a constraint
does not silently add work to unrelated cache mutations.

```go
if err := schema.Validate(); err != nil {
	return err
}
if err := hatSchema.ValidateDataset(schema, map[string][]hatSchema.Row{
		"teams":   teams,
		"members": members,
}); err != nil {
	return err
}
```

## Foreign Keys

A `Constraint` with `Kind: ConstraintForeignKey` names local `Columns`, a
`ReferenceSource`, and equally sized `ReferenceColumns`. Validation checks
that the referenced source and columns exist, then requires every non-NULL
local key to have an equal referenced key. Composite keys are supported. A
single-source `ValidateRows` call receives a `SourceRowsResolver` when the
foreign key points at another source.

`ValidateDataset` validates against the supplied dataset view, so callers can
validate a transaction candidate before publishing it. Errors identify the
constraint, source, and row where possible. NULL foreign-key values follow the
documented SQL-compatible behavior and are not treated as missing references.

The validator does not provide cascading delete/update, locking, or automatic
integration with arbitrary external mutation code. The caller must validate
the complete candidate view at its transaction boundary.

## Other Constraints

The same validator supports `NOT NULL`, `UNIQUE`, and SQL `CHECK` constraints.
Schema and constraint names are normalized and included in the schema
fingerprint, so incompatible definitions remain distinguishable during
migrations and replication checks.

## Verification

The schema package tests cover valid and invalid foreign-key datasets,
cross-source resolution, composite-key validation, and invalid declarations in
both normal and race-enabled runs.
