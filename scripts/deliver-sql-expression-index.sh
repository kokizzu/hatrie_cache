#!/bin/sh
set -eu

mode=${1:-plan}
files='hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_lower_index.go hat/hatCache/sql_lower_index_test.go hat/hatCache/sql_lower_index_benchmark_test.go hat/hatSql/expression_index.go hat/hatSql/query.go ADOPTED_QUERY_ENGINE_IDEAS.md Makefile scripts/test-sql-expression-index.sh scripts/test-race-sql-expression-index.sh scripts/benchmark-sql-expression-index.sh scripts/verify-sql-expression-index.sh scripts/deliver-sql-expression-index.sh'

case "$mode" in
plan)
	git status --short
	git diff --stat -- $files
	git diff -- Makefile
	;;
verify)
	sh ./scripts/verify-sql-expression-index.sh
	;;
unstage)
	git restore --staged -- $files
	;;
check-stage)
	git apply --cached --check <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3268,0 +3269,23 @@
+.PHONY: test-sql-expression-index
+test-sql-expression-index:
+	sh ./scripts/test-sql-expression-index.sh
+.PHONY: benchmark-sql-expression-index
+benchmark-sql-expression-index:
+	sh ./scripts/benchmark-sql-expression-index.sh
+.PHONY: test-race-sql-expression-index
+test-race-sql-expression-index:
+	sh ./scripts/test-race-sql-expression-index.sh
+.PHONY: verify-sql-expression-index
+verify-sql-expression-index:
+	sh ./scripts/verify-sql-expression-index.sh
+.PHONY: deliver-sql-expression-index-plan verify-sql-expression-index-delivery deliver-sql-expression-index unstage-sql-expression-index check-sql-expression-index-stage
+deliver-sql-expression-index-plan:
+	sh ./scripts/deliver-sql-expression-index.sh plan
+verify-sql-expression-index-delivery:
+	sh ./scripts/deliver-sql-expression-index.sh verify
+deliver-sql-expression-index:
+	sh ./scripts/deliver-sql-expression-index.sh apply
+unstage-sql-expression-index:
+	sh ./scripts/deliver-sql-expression-index.sh unstage
+check-sql-expression-index-stage:
+	sh ./scripts/deliver-sql-expression-index.sh check-stage
PATCH
	;;
apply)
	git add -- ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_lower_index.go hat/hatCache/sql_lower_index_test.go hat/hatCache/sql_lower_index_benchmark_test.go hat/hatSql/expression_index.go hat/hatSql/query.go scripts/test-sql-expression-index.sh scripts/test-race-sql-expression-index.sh scripts/benchmark-sql-expression-index.sh scripts/verify-sql-expression-index.sh scripts/deliver-sql-expression-index.sh
	if git grep --cached -q '^deliver-sql-expression-index:$' -- Makefile; then
		:
	else
	git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3268,0 +3269,23 @@
+.PHONY: test-sql-expression-index
+test-sql-expression-index:
+	sh ./scripts/test-sql-expression-index.sh
+.PHONY: benchmark-sql-expression-index
+benchmark-sql-expression-index:
+	sh ./scripts/benchmark-sql-expression-index.sh
+.PHONY: test-race-sql-expression-index
+test-race-sql-expression-index:
+	sh ./scripts/test-race-sql-expression-index.sh
+.PHONY: verify-sql-expression-index
+verify-sql-expression-index:
+	sh ./scripts/verify-sql-expression-index.sh
+.PHONY: deliver-sql-expression-index-plan verify-sql-expression-index-delivery deliver-sql-expression-index unstage-sql-expression-index check-sql-expression-index-stage
+deliver-sql-expression-index-plan:
+	sh ./scripts/deliver-sql-expression-index.sh plan
+verify-sql-expression-index-delivery:
+	sh ./scripts/deliver-sql-expression-index.sh verify
+deliver-sql-expression-index:
+	sh ./scripts/deliver-sql-expression-index.sh apply
+unstage-sql-expression-index:
+	sh ./scripts/deliver-sql-expression-index.sh unstage
+check-sql-expression-index-stage:
+	sh ./scripts/deliver-sql-expression-index.sh check-stage
PATCH
	fi
	git diff --cached --check
	git diff --cached --name-only
	git commit -m 'feat: add SQL lower expression index'
	git push
	;;
*)
	echo "usage: $0 plan|verify|unstage|check-stage|apply" >&2
	exit 2
	;;
esac
