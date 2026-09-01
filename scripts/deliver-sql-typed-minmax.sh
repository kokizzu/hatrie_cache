#!/bin/sh
set -eu

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_benchmark_test.go hat/hatSql/typed_table_minmax_test.go scripts/test-sql-typed-minmax.sh scripts/benchmark-sql-typed-minmax.sh scripts/verify-sql-typed-minmax.sh scripts/deliver-sql-typed-minmax-plan.sh scripts/deliver-sql-typed-minmax.sh
	;;
apply)
	git diff --check
	git add ADOPTED_QUERY_ENGINE_IDEAS.md hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_benchmark_test.go hat/hatSql/typed_table_minmax_test.go scripts/test-sql-typed-minmax.sh scripts/benchmark-sql-typed-minmax.sh scripts/verify-sql-typed-minmax.sh scripts/deliver-sql-typed-minmax-plan.sh scripts/deliver-sql-typed-minmax.sh
	if ! git grep --cached -q '^deliver-sql-typed-minmax:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3329,0 +3330,14 @@
+test-sql-typed-minmax:
+	sh ./scripts/test-sql-typed-minmax.sh
+benchmark-sql-typed-minmax:
+	sh ./scripts/benchmark-sql-typed-minmax.sh
+verify-sql-typed-minmax:
+	sh ./scripts/verify-sql-typed-minmax.sh
+
+deliver-sql-typed-minmax-plan:
+	sh ./scripts/deliver-sql-typed-minmax-plan.sh
+
+deliver-sql-typed-minmax:
+	sh ./scripts/deliver-sql-typed-minmax.sh apply
+check-sql-typed-minmax-stage:
+	sh ./scripts/deliver-sql-typed-minmax.sh check
PATCH
	fi
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_benchmark_test.go hat/hatSql/typed_table_minmax_test.go scripts/test-sql-typed-minmax.sh scripts/benchmark-sql-typed-minmax.sh scripts/verify-sql-typed-minmax.sh scripts/deliver-sql-typed-minmax-plan.sh scripts/deliver-sql-typed-minmax.sh
	git commit -m 'feat: maintain typed table min and max'
	git push
	;;
*)
	printf '%s\n' 'usage: deliver-sql-typed-minmax.sh [apply|check]' >&2
	exit 2
	;;
esac
