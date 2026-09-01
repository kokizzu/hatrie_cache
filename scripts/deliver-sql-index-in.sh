#!/bin/sh
set -eu

mode=${1:-plan}
files='ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatCache/sql_index_in_test.go hat/hatCache/sql_index_in_benchmark_test.go hat/hatSql/query.go scripts/test-sql-index-in.sh scripts/test-race-sql-index-in.sh scripts/benchmark-sql-index-in.sh scripts/verify-sql-index-in.sh scripts/deliver-sql-index-in.sh'

case "$mode" in
plan)
	git status --short
	git diff --stat -- $files
	git diff -- Makefile
	;;
verify)
	sh ./scripts/verify-sql-index-in.sh
	;;
check-stage)
	git apply --cached --check <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3291,0 +3292,22 @@
+.PHONY: test-race-sql-index-in
+test-race-sql-index-in:
+	sh ./scripts/test-race-sql-index-in.sh
+
+.PHONY: verify-sql-index-in
+verify-sql-index-in:
+	sh ./scripts/verify-sql-index-in.sh
+.PHONY: test-sql-index-in
+test-sql-index-in:
+	sh ./scripts/test-sql-index-in.sh
+.PHONY: benchmark-sql-index-in
+benchmark-sql-index-in:
+	sh ./scripts/benchmark-sql-index-in.sh
+.PHONY: deliver-sql-index-in-plan verify-sql-index-in-delivery deliver-sql-index-in check-sql-index-in-stage
+deliver-sql-index-in-plan:
+	sh ./scripts/deliver-sql-index-in.sh plan
+verify-sql-index-in-delivery:
+	sh ./scripts/deliver-sql-index-in.sh verify
+deliver-sql-index-in:
+	sh ./scripts/deliver-sql-index-in.sh apply
+check-sql-index-in-stage:
+	sh ./scripts/deliver-sql-index-in.sh check-stage
PATCH
	;;
apply)
	git add -- ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatCache/sql_index_in_test.go hat/hatCache/sql_index_in_benchmark_test.go hat/hatSql/query.go scripts/test-sql-index-in.sh scripts/test-race-sql-index-in.sh scripts/benchmark-sql-index-in.sh scripts/verify-sql-index-in.sh scripts/deliver-sql-index-in.sh
	git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3291,0 +3292,22 @@
+.PHONY: test-race-sql-index-in
+test-race-sql-index-in:
+	sh ./scripts/test-race-sql-index-in.sh
+
+.PHONY: verify-sql-index-in
+verify-sql-index-in:
+	sh ./scripts/verify-sql-index-in.sh
+.PHONY: test-sql-index-in
+test-sql-index-in:
+	sh ./scripts/test-sql-index-in.sh
+.PHONY: benchmark-sql-index-in
+benchmark-sql-index-in:
+	sh ./scripts/benchmark-sql-index-in.sh
+.PHONY: deliver-sql-index-in-plan verify-sql-index-in-delivery deliver-sql-index-in check-sql-index-in-stage
+deliver-sql-index-in-plan:
+	sh ./scripts/deliver-sql-index-in.sh plan
+verify-sql-index-in-delivery:
+	sh ./scripts/deliver-sql-index-in.sh verify
+deliver-sql-index-in:
+	sh ./scripts/deliver-sql-index-in.sh apply
+check-sql-index-in-stage:
+	sh ./scripts/deliver-sql-index-in.sh check-stage
PATCH
	git diff --cached --check
	git diff --cached --name-only
	git commit -m 'perf: use indexes for SQL literal IN'
	git push
	;;
*)
	echo "usage: $0 plan|verify|check-stage|apply" >&2
	exit 2
	;;
esac
