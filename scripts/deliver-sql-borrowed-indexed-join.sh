#!/bin/sh
set -eu

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/indexed_join_borrowed_test.go hat/hatCache/sql_borrowed_index.go hat/hatCache/sql_borrowed_index_test.go hat/hatCache/sql_borrowed_index_benchmark_test.go scripts/test-sql-borrowed-indexed-join.sh scripts/benchmark-sql-borrowed-indexed-join.sh scripts/verify-sql-borrowed-indexed-join.sh scripts/deliver-sql-borrowed-indexed-join-plan.sh scripts/deliver-sql-borrowed-indexed-join.sh
	;;
apply)
	git diff --check
	git add ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/indexed_join_borrowed_test.go hat/hatCache/sql_borrowed_index.go hat/hatCache/sql_borrowed_index_test.go hat/hatCache/sql_borrowed_index_benchmark_test.go scripts/test-sql-borrowed-indexed-join.sh scripts/benchmark-sql-borrowed-indexed-join.sh scripts/verify-sql-borrowed-indexed-join.sh scripts/deliver-sql-borrowed-indexed-join-plan.sh scripts/deliver-sql-borrowed-indexed-join.sh
	if ! git grep --cached -q '^deliver-sql-borrowed-indexed-join:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3314,0 +3315,14 @@
+test-sql-borrowed-indexed-join:
+	sh ./scripts/test-sql-borrowed-indexed-join.sh
+benchmark-sql-borrowed-indexed-join:
+	sh ./scripts/benchmark-sql-borrowed-indexed-join.sh
+verify-sql-borrowed-indexed-join:
+	sh ./scripts/verify-sql-borrowed-indexed-join.sh
+
+deliver-sql-borrowed-indexed-join-plan:
+	sh ./scripts/deliver-sql-borrowed-indexed-join-plan.sh
+
+deliver-sql-borrowed-indexed-join:
+	sh ./scripts/deliver-sql-borrowed-indexed-join.sh apply
+check-sql-borrowed-indexed-join-stage:
+	sh ./scripts/deliver-sql-borrowed-indexed-join.sh check
PATCH
	fi
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/indexed_join_borrowed_test.go hat/hatCache/sql_borrowed_index.go hat/hatCache/sql_borrowed_index_test.go hat/hatCache/sql_borrowed_index_benchmark_test.go scripts/test-sql-borrowed-indexed-join.sh scripts/benchmark-sql-borrowed-indexed-join.sh scripts/verify-sql-borrowed-indexed-join.sh scripts/deliver-sql-borrowed-indexed-join-plan.sh scripts/deliver-sql-borrowed-indexed-join.sh
	git commit -m 'perf: borrow indexed SQL join postings'
	git push
	;;
*)
	printf '%s\n' 'usage: deliver-sql-borrowed-indexed-join.sh [apply|check]' >&2
	exit 2
	;;
esac
