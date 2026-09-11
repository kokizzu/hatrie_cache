# SQL Multiset Set Operations

The SQL parser and executor support `INTERSECT ALL` and `EXCEPT ALL` in
addition to the existing distinct `INTERSECT`, `EXCEPT`, and `UNION` forms.
The `ALL` forms preserve duplicate multiplicity while retaining the left
query's row order.

```sql
FROM VALUES (2), (1), (1), (3), (1) AS lhs(id) SELECT lhs.id
INTERSECT ALL
FROM VALUES (1), (1), (4) AS rhs(id) SELECT rhs.id
```

The result is `1, 1`: each value appears at most the smaller count from the
two inputs. `EXCEPT ALL` consumes matching right-side rows one at a time:

```sql
FROM VALUES (2), (1), (1), (3), (1) AS lhs(id) SELECT lhs.id
EXCEPT ALL
FROM VALUES (1), (4) AS rhs(id) SELECT rhs.id
```

The result is `2, 1, 3, 1`. Both operations use the output row key generated
by the selected collation, so `SQLCollationUnicodeCI` treats `A` and `a` as
the same multiset value while preserving the first left-side payload. `NULL`
values participate in multiplicity matching according to the existing SQL row
key rules.

`INTERSECT` and `EXCEPT` without `ALL` retain their existing distinct
semantics. Set branches must project the same column names in the same order.
The set memory budget and external spill path remain applicable to distinct
operations; `ALL` operations use a right-side count map and do not silently
deduplicate the result.
