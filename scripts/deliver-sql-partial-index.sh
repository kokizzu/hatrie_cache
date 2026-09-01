#!/bin/sh
set -eu

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	;;
apply)
	git diff --check
	git add ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_partial_index.go hat/hatCache/sql_partial_index_benchmark_test.go hat/hatCache/sql_partial_index_test.go scripts/test-sql-partial-index.sh scripts/benchmark-sql-partial-index.sh scripts/verify-sql-partial-index.sh scripts/deliver-sql-partial-index-plan.sh scripts/deliver-sql-partial-index.sh
	if ! git grep --cached -q '^deliver-sql-partial-index:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3386,0 +3387,16 @@
+
+test-sql-partial-index:
+	sh ./scripts/test-sql-partial-index.sh
+
+benchmark-sql-partial-index:
+	sh ./scripts/benchmark-sql-partial-index.sh
+verify-sql-partial-index:
+	sh ./scripts/verify-sql-partial-index.sh
+
+deliver-sql-partial-index-plan:
+	sh ./scripts/deliver-sql-partial-index-plan.sh
+
+deliver-sql-partial-index:
+	sh ./scripts/deliver-sql-partial-index.sh apply
+check-sql-partial-index-stage:
+	sh ./scripts/deliver-sql-partial-index.sh check
PATCH
	fi
	git diff --cached --check
	git commit -m 'feat: add conditional JSON partial index'
	git push
	;;
*) exit 2 ;;
esac
