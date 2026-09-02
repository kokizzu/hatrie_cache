#!/usr/bin/env bash
set -euo pipefail

mode="${1:-check}"
commit_message="feat: add bounded typed arrangement hydration"

run_check() {
  bash scripts/check-typed-table-arrangement-hydration.sh
}

stage_feature() {
  run_check
  git add \
    hat/hatSql/typed_table_arrangements.go \
    hat/hatSql/typed_table_join_arrangements.go \
    hat/hatSql/typed_table_arrangement_hydration_test.go \
    hat/hatSql/typed_table_arrangement_hydration_benchmark_test.go \
    scripts/test-typed-table-arrangement-hydration.sh \
    scripts/test-race-typed-table-arrangement-hydration.sh \
    scripts/format-typed-table-arrangement-hydration.sh \
    scripts/benchmark-typed-table-arrangement-hydration.sh \
    scripts/check-typed-table-arrangement-hydration.sh \
    scripts/deliver-typed-table-arrangement-hydration.sh \
    BENCHMARK.md \
    ADOPTED_QUERY_ENGINE_IDEAS.md
  staged_makefile="$(mktemp)"
  git show :Makefile >"$staged_makefile"
  if ! rg -q '^\.PHONY: test-typed-table-arrangement-hydration$' "$staged_makefile"; then
    git apply --cached --check <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3514,3 +3514,34 @@
 .PHONY: push-sql-prewhere
 push-sql-prewhere:
 	bash scripts/deliver-sql-prewhere.sh push
+
+.PHONY: test-typed-table-arrangement-hydration
+test-typed-table-arrangement-hydration:
+	bash scripts/test-typed-table-arrangement-hydration.sh
+
+.PHONY: format-typed-table-arrangement-hydration
+format-typed-table-arrangement-hydration:
+	bash scripts/format-typed-table-arrangement-hydration.sh
+
+.PHONY: benchmark-typed-table-arrangement-hydration
+benchmark-typed-table-arrangement-hydration:
+	bash scripts/benchmark-typed-table-arrangement-hydration.sh
+
+.PHONY: test-race-typed-table-arrangement-hydration
+test-race-typed-table-arrangement-hydration:
+	bash scripts/test-race-typed-table-arrangement-hydration.sh
+
+.PHONY: check-typed-table-arrangement-hydration
+check-typed-table-arrangement-hydration:
+	bash scripts/check-typed-table-arrangement-hydration.sh
+
+.PHONY: deliver-typed-table-arrangement-hydration
+deliver-typed-table-arrangement-hydration:
+	bash scripts/deliver-typed-table-arrangement-hydration.sh apply
+
+.PHONY: commit-typed-table-arrangement-hydration
+commit-typed-table-arrangement-hydration:
+	bash scripts/deliver-typed-table-arrangement-hydration.sh commit
+
+.PHONY: push-typed-table-arrangement-hydration
+push-typed-table-arrangement-hydration:
+	bash scripts/deliver-typed-table-arrangement-hydration.sh push
PATCH
    git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -3514,3 +3514,34 @@
 .PHONY: push-sql-prewhere
 push-sql-prewhere:
 	bash scripts/deliver-sql-prewhere.sh push
+
+.PHONY: test-typed-table-arrangement-hydration
+test-typed-table-arrangement-hydration:
+	bash scripts/test-typed-table-arrangement-hydration.sh
+
+.PHONY: format-typed-table-arrangement-hydration
+format-typed-table-arrangement-hydration:
+	bash scripts/format-typed-table-arrangement-hydration.sh
+
+.PHONY: benchmark-typed-table-arrangement-hydration
+benchmark-typed-table-arrangement-hydration:
+	bash scripts/benchmark-typed-table-arrangement-hydration.sh
+
+.PHONY: test-race-typed-table-arrangement-hydration
+test-race-typed-table-arrangement-hydration:
+	bash scripts/test-race-typed-table-arrangement-hydration.sh
+
+.PHONY: check-typed-table-arrangement-hydration
+check-typed-table-arrangement-hydration:
+	bash scripts/check-typed-table-arrangement-hydration.sh
+
+.PHONY: deliver-typed-table-arrangement-hydration
+deliver-typed-table-arrangement-hydration:
+	bash scripts/deliver-typed-table-arrangement-hydration.sh apply
+
+.PHONY: commit-typed-table-arrangement-hydration
+commit-typed-table-arrangement-hydration:
+	bash scripts/deliver-typed-table-arrangement-hydration.sh commit
+
+.PHONY: push-typed-table-arrangement-hydration
+push-typed-table-arrangement-hydration:
+	bash scripts/deliver-typed-table-arrangement-hydration.sh push
PATCH
  fi
  rm -f "$staged_makefile"
}

case "$mode" in
  check)
    run_check
    ;;
  apply)
    stage_feature
    ;;
  commit)
    stage_feature
    git commit -m "$commit_message"
    ;;
  push)
    stage_feature
    if ! git diff --cached --quiet; then
      git commit -m "$commit_message"
    fi
    git push
    ;;
  *)
    printf 'usage: %s {check|apply|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
