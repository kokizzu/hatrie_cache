# Linear Indexed Joins

`hatSql.TypedTableJoin` maintains an exact inner equi-join over two
`TypedTable` instances. It keeps each source row by key, maps each non-NULL
join value to the source keys carrying it, and retains the currently matched
key pairs.

For an insert or update, the arrangement removes the old value bucket when
needed, adds the new value bucket, and touches only the opposite index bucket
for that value. Deletes perform the corresponding removal. Join maintenance is
therefore proportional to the affected equality bucket and emitted pairs,
instead of repeatedly scanning both tables. Typed values use their physical
kind in the index key, so equal textual representations of different types do
not collide.

`ApplyLeft` and `ApplyRight` require strictly ordered source sequences and
ignore already-applied changes. `Rows` returns independent, deterministic
copies of the maintained pairs. The arrangement is an opt-in reusable
component and does not alter ordinary SQL query planning.
