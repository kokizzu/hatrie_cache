#!/bin/sh
set -eu

paths='ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_mvcc.go hat/hatSql/typed_table_mvcc_test.go hat/hatSql/typed_table_mvcc_benchmark_test.go scripts/audit-engine-surface.sh scripts/benchmark-sql-typed-table-mvcc.sh scripts/format-sql-typed-table-mvcc.sh scripts/test-race-sql-typed-table-mvcc.sh scripts/test-sql-typed-table-mvcc.sh scripts/deliver-sql-typed-table-mvcc.sh'

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	git diff --cached --name-only -- $paths Makefile
	;;
apply)
	git diff --check
	git add $paths
	if ! git grep --cached -q '^test-sql-typed-table-mvcc:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3342,0 +3343,35 @@
+audit-engine-surface:
+	bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-typed-table:
+	DETAIL=typed-table bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-storage:
+	DETAIL=storage bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-query:
+	DETAIL=query bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-typed-table-symbols:
+	DETAIL=typed-table-symbols bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-mvcc:
+	DETAIL=mvcc bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-mvcc-docs:
+	DETAIL=mvcc-docs bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-makefile:
+	DETAIL=makefile bash scripts/audit-engine-surface.sh
+
+audit-engine-surface-delivery:
+	DETAIL=delivery bash scripts/audit-engine-surface.sh
+
+test-sql-typed-table-mvcc:
+	bash scripts/test-sql-typed-table-mvcc.sh
+
+format-sql-typed-table-mvcc:
+	bash scripts/format-sql-typed-table-mvcc.sh
+
+benchmark-sql-typed-table-mvcc:
+	bash scripts/benchmark-sql-typed-table-mvcc.sh
+
+test-race-sql-typed-table-mvcc:
+	bash scripts/test-race-sql-typed-table-mvcc.sh
+
+deliver-sql-typed-table-mvcc:
+	bash scripts/deliver-sql-typed-table-mvcc.sh apply
+
+check-sql-typed-table-mvcc-stage:
+	bash scripts/deliver-sql-typed-table-mvcc.sh check
PATCH
	fi
	git diff --cached --check
	git diff --cached --name-only -- $paths Makefile
	git commit -m 'feat: add opt-in typed table MVCC snapshots'
	git push
	;;
*)
	printf '%s\n' 'usage: deliver-sql-typed-table-mvcc.sh [apply|check]' >&2
	exit 2
	;;
esac
