#!/bin/sh
set -eu

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatSql/temporal_analytics.go hat/hatSql/temporal_analytics_benchmark_test.go hat/hatSql/temporal_analytics_order_test.go scripts/test-sql-temporal-storage.sh scripts/benchmark-sql-temporal-storage.sh scripts/verify-sql-temporal-storage.sh scripts/deliver-sql-temporal-storage-plan.sh scripts/deliver-sql-temporal-storage.sh
	;;
apply)
	git diff --check
	git add ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatSql/temporal_analytics.go hat/hatSql/temporal_analytics_benchmark_test.go hat/hatSql/temporal_analytics_order_test.go scripts/test-sql-temporal-storage.sh scripts/benchmark-sql-temporal-storage.sh scripts/verify-sql-temporal-storage.sh scripts/deliver-sql-temporal-storage-plan.sh scripts/deliver-sql-temporal-storage.sh
	if ! git grep --cached -q '^deliver-sql-temporal-storage:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3342,0 +3343,15 @@
+test-sql-temporal-storage:
+	sh ./scripts/test-sql-temporal-storage.sh
+
+benchmark-sql-temporal-storage:
+	sh ./scripts/benchmark-sql-temporal-storage.sh
+verify-sql-temporal-storage:
+	sh ./scripts/verify-sql-temporal-storage.sh
+
+deliver-sql-temporal-storage-plan:
+	sh ./scripts/deliver-sql-temporal-storage-plan.sh
+
+deliver-sql-temporal-storage:
+	sh ./scripts/deliver-sql-temporal-storage.sh apply
+check-sql-temporal-storage-stage:
+	sh ./scripts/deliver-sql-temporal-storage.sh check
PATCH
	fi
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatSql/temporal_analytics.go hat/hatSql/temporal_analytics_benchmark_test.go hat/hatSql/temporal_analytics_order_test.go scripts/test-sql-temporal-storage.sh scripts/benchmark-sql-temporal-storage.sh scripts/verify-sql-temporal-storage.sh scripts/deliver-sql-temporal-storage-plan.sh scripts/deliver-sql-temporal-storage.sh
	git commit -m 'perf: optimize temporal version ordering'
	git push
	;;
*)
	printf '%s\n' 'usage: deliver-sql-temporal-storage.sh [apply|check]' >&2
	exit 2
	;;
esac
