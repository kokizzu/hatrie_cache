#!/usr/bin/env bash
set -euo pipefail

for path in \
	M210_RETAINED_SQL_SNAPSHOTS.md \
	INSPIRATION_ROUND2.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	hat/hatSql/m210_retained_sql_snapshot.go \
	hat/hatSql/m210_retained_sql_snapshot_test.go; do
	test -s "$path"
done
rg -q 'M210.*Historical.*AS OF|M210.*retained' INSPIRATION_ROUND2.md
rg -q 'TypedTableSQLSnapshotRegistry' ADOPTED_QUERY_ENGINE_IDEAS.md M210_RETAINED_SQL_SNAPSHOTS.md
rg -q 'make benchmark-m210-retained-sql-snapshot' BENCHMARK.md M210_RETAINED_SQL_SNAPSHOTS.md
rg -q 'BeginSQLSnapshotAt' hat/hatSql/m210_retained_sql_snapshot.go
