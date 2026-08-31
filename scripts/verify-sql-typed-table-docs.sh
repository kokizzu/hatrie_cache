#!/bin/sh
set -eu

rg -q 'TypedTable' TYPED_TABLES.md
rg -q 'ErrTypedTableChangesCompacted' TYPED_TABLES.md
rg -q 'benchmark-sql-typed-table' TYPED_TABLES.md
rg -q '\[Typed SQL tables\]\(TYPED_TABLES.md\)' README.md
