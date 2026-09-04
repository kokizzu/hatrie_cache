#!/bin/sh
set -eu

feature_files="ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md README.md SQL_VECTORIZED_EXECUTION.md hat/hatSql/columnar_vector_group_aggregate.go hat/hatSql/columnar_vector_group_aggregate_test.go hat/hatSql/columnar_vector_group_aggregate_benchmark_test.go scripts/benchmark-sql-vectorized.sh scripts/format-sql-vectorized.sh scripts/test-race-sql-vectorized.sh scripts/test-sql-vectorized.sh scripts/vet-sql-vectorized.sh scripts/commit-sql-vectorized.sh"
check_files="ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md README.md SQL_VECTORIZED_EXECUTION.md hat/hatSql/columnar_vector_group_aggregate.go hat/hatSql/columnar_vector_group_aggregate_test.go hat/hatSql/columnar_vector_group_aggregate_benchmark_test.go scripts/benchmark-sql-vectorized.sh scripts/format-sql-vectorized.sh scripts/test-race-sql-vectorized.sh scripts/test-sql-vectorized.sh scripts/vet-sql-vectorized.sh"

stage_feature() {
	if ! git diff --cached --quiet; then
		printf '%s\n' 'Refusing to stage vectorized SQL: the index already contains changes.' >&2
		exit 1
	fi
	git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -697,4 +697,31 @@ test-sql-hash-aggregate:
 	sh ./scripts/test-sql-hash-aggregate.sh
 
+test-sql-vectorized:
+	sh ./scripts/test-sql-vectorized.sh
+
+test-race-sql-vectorized:
+	sh ./scripts/test-race-sql-vectorized.sh
+
+vet-sql-vectorized:
+	sh ./scripts/vet-sql-vectorized.sh
+
+format-sql-vectorized:
+	sh ./scripts/format-sql-vectorized.sh
+
+benchmark-sql-vectorized:
+	sh ./scripts/benchmark-sql-vectorized.sh
+
+benchmark-sql-vectorized-long:
+	BENCHTIME=1s sh ./scripts/benchmark-sql-vectorized.sh
+
+stage-sql-vectorized:
+	sh ./scripts/commit-sql-vectorized.sh stage
+
+commit-sql-vectorized:
+	sh ./scripts/commit-sql-vectorized.sh commit
+
+push-sql-vectorized:
+	sh ./scripts/commit-sql-vectorized.sh push
+
 format-sql-hash-aggregate:
 	sh ./scripts/format-sql-hash-aggregate.sh
PATCH
	git add $feature_files
	git diff --cached --check -- Makefile $check_files
}

case "${1:-inspect}" in
inspect)
	git status --short
	git diff --cached --name-status
	;;
stage)
	stage_feature
	git diff --cached --name-status
	;;
commit)
	if git diff --cached --quiet; then
		stage_feature
	fi
	git commit -m "sql: vectorize columnar grouped aggregates"
	;;
push)
	git push origin HEAD
	;;
*)
	printf 'usage: %s {inspect|stage|commit|push}\n' "$0" >&2
	exit 2
	;;
esac
