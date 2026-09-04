#!/bin/sh
set -eu

stage_makefile() {
  git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -694,6 +694,34 @@
 test-sql-limit-by:
 	sh ./scripts/test-sql-limit-by.sh
 
+test-sql-hash-aggregate:
+	sh ./scripts/test-sql-hash-aggregate.sh
+
+format-sql-hash-aggregate:
+	sh ./scripts/format-sql-hash-aggregate.sh
+
+benchmark-sql-hash-aggregate:
+	sh ./scripts/benchmark-sql-hash-aggregate.sh
+
+benchmark-sql-hash-aggregate-all:
+	HASH_AGGREGATE_BENCH_MODE=all sh ./scripts/benchmark-sql-hash-aggregate.sh
+
+.PHONY: verify-sql-hash-aggregate
+verify-sql-hash-aggregate:
+	sh ./scripts/verify-sql-hash-aggregate.sh
+
+.PHONY: stage-sql-hash-aggregate
+stage-sql-hash-aggregate:
+	sh ./scripts/commit-sql-hash-aggregate.sh stage
+
+.PHONY: commit-sql-hash-aggregate
+commit-sql-hash-aggregate:
+	sh ./scripts/commit-sql-hash-aggregate.sh commit
+
+.PHONY: push-sql-hash-aggregate
+push-sql-hash-aggregate:
+	sh ./scripts/commit-sql-hash-aggregate.sh push
+
 format-sql-limit-by:
 	sh ./scripts/format-sql-limit-by.sh
 
PATCH
}

check_feature_paths() {
  git diff --cached --check -- \
    Makefile \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    SQL_HASH_AGGREGATE.md \
    hat/hatSql/hash_group_aggregate.go \
    hat/hatSql/hash_group_aggregate_test.go \
    hat/hatSql/query.go \
    scripts/benchmark-sql-hash-aggregate.sh \
    scripts/format-sql-hash-aggregate.sh \
    scripts/test-sql-hash-aggregate.sh \
    scripts/verify-sql-hash-aggregate.sh
}

case "${1:-stage}" in
stage)
  if ! git diff --cached --quiet; then
    printf '%s\n' 'Refusing to stage: the index already contains changes.' >&2
    exit 1
  fi
  stage_makefile
  git add -- \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    SQL_HASH_AGGREGATE.md \
    hat/hatSql/hash_group_aggregate.go \
    hat/hatSql/hash_group_aggregate_test.go \
    hat/hatSql/query.go \
    scripts/benchmark-sql-hash-aggregate.sh \
    scripts/commit-sql-hash-aggregate.sh \
    scripts/format-sql-hash-aggregate.sh \
    scripts/test-sql-hash-aggregate.sh \
    scripts/verify-sql-hash-aggregate.sh
  check_feature_paths
  printf '%s\n' '--- staged feature paths ---'
  git diff --cached --name-status
  ;;
refresh-stage)
  git add -- scripts/commit-sql-hash-aggregate.sh
  git reset HEAD -- Makefile
  stage_makefile
  check_feature_paths
  ;;
commit)
  if git diff --cached --quiet; then
    printf '%s\n' 'No staged feature changes.' >&2
    exit 1
  fi
  git commit -m 'feat(sql): compact generic group aggregation'
  ;;
push)
  git push origin HEAD
  ;;
*)
  printf 'usage: %s stage|refresh-stage|commit|push\n' "$0" >&2
  exit 2
  ;;
esac
