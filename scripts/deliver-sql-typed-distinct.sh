#!/bin/sh
set -eu

case "${1:-apply}" in
check)
	git diff --check
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_distinct_benchmark_test.go hat/hatSql/typed_table_distinct_test.go scripts/test-sql-typed-distinct.sh scripts/benchmark-sql-typed-distinct.sh scripts/verify-sql-typed-distinct.sh scripts/deliver-sql-typed-distinct-plan.sh scripts/deliver-sql-typed-distinct.sh
	;;
apply)
	git diff --check
	git add ADOPTED_QUERY_ENGINE_IDEAS.md TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_distinct_benchmark_test.go hat/hatSql/typed_table_distinct_test.go scripts/test-sql-typed-distinct.sh scripts/benchmark-sql-typed-distinct.sh scripts/verify-sql-typed-distinct.sh scripts/deliver-sql-typed-distinct-plan.sh scripts/deliver-sql-typed-distinct.sh
	if ! git grep --cached -q '^deliver-sql-typed-distinct:$' -- Makefile; then
		git apply --cached --unidiff-zero --whitespace=nowarn <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3354,0 +3355,16 @@
+
+test-sql-typed-distinct:
+	sh ./scripts/test-sql-typed-distinct.sh
+
+benchmark-sql-typed-distinct:
+	sh ./scripts/benchmark-sql-typed-distinct.sh
+verify-sql-typed-distinct:
+	sh ./scripts/verify-sql-typed-distinct.sh
+
+deliver-sql-typed-distinct-plan:
+	sh ./scripts/deliver-sql-typed-distinct-plan.sh
+
+deliver-sql-typed-distinct:
+	sh ./scripts/deliver-sql-typed-distinct.sh apply
+check-sql-typed-distinct-stage:
+	sh ./scripts/deliver-sql-typed-distinct.sh check
PATCH
	fi
	git diff --cached --check
	git diff --cached --name-only -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile TYPED_TABLES.md hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_distinct_benchmark_test.go hat/hatSql/typed_table_distinct_test.go scripts/test-sql-typed-distinct.sh scripts/benchmark-sql-typed-distinct.sh scripts/verify-sql-typed-distinct.sh scripts/deliver-sql-typed-distinct-plan.sh scripts/deliver-sql-typed-distinct.sh
	git commit -m 'feat: maintain typed table count distinct'
	git push
	;;
*)
	printf '%s\n' 'usage: deliver-sql-typed-distinct.sh [apply|check]' >&2
	exit 2
	;;
esac
