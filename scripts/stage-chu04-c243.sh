#!/usr/bin/env bash
set -euo pipefail

is_allowed() {
  case "$1" in
    ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|CHU04_EXTERNAL_DISTINCT_SPILL.md|Makefile|PRODUCT_IDEA_GAPS.md|README.md|hat/hatSql/query.go|hat/hatSql/chu04_external_distinct_spill_test.go|hat/hatSql/chu04_external_distinct_spill_benchmark_test.go|scripts/benchmark-chu04-c243.sh|scripts/commit-chu04-c243.sh|scripts/format-chu04-c243.sh|scripts/memory-chu04-c243.sh|scripts/push-chu04-c243.sh|scripts/race-chu04-c243.sh|scripts/stage-chu04-c243.sh|scripts/test-chu04-c243.sh|scripts/test-chu04-package-c243.sh|scripts/vet-chu04-c243.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

while IFS= read -r path; do
  if [[ -n "$path" ]] && ! is_allowed "$path"; then
    printf 'refusing to stage unrelated pre-staged path: %s\n' "$path" >&2
    exit 1
  fi
done < <(git diff --cached --name-only)

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU04_EXTERNAL_DISTINCT_SPILL.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/query.go \
  hat/hatSql/chu04_external_distinct_spill_test.go \
  hat/hatSql/chu04_external_distinct_spill_benchmark_test.go \
  scripts/benchmark-chu04-c243.sh \
  scripts/commit-chu04-c243.sh \
  scripts/format-chu04-c243.sh \
  scripts/memory-chu04-c243.sh \
  scripts/push-chu04-c243.sh \
  scripts/race-chu04-c243.sh \
  scripts/stage-chu04-c243.sh \
  scripts/test-chu04-c243.sh \
  scripts/test-chu04-package-c243.sh \
  scripts/vet-chu04-c243.sh

patch_file="$(mktemp)"
cleanup() {
  rm -f "$patch_file"
}
trap cleanup EXIT
cat > "$patch_file" <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -1,0 +2,39 @@
+.PHONY: test-chu04-c243
+test-chu04-c243:
+	@bash ./scripts/test-chu04-c243.sh
+
+.PHONY: benchmark-chu04-c243
+benchmark-chu04-c243:
+	@bash ./scripts/benchmark-chu04-c243.sh
+
+.PHONY: memory-chu04-c243
+memory-chu04-c243:
+	@bash ./scripts/memory-chu04-c243.sh
+
+.PHONY: format-chu04-c243
+format-chu04-c243:
+	@bash ./scripts/format-chu04-c243.sh
+
+.PHONY: test-chu04-package-c243
+test-chu04-package-c243:
+	@bash ./scripts/test-chu04-package-c243.sh
+
+.PHONY: race-chu04-c243
+race-chu04-c243:
+	@bash ./scripts/race-chu04-c243.sh
+
+.PHONY: vet-chu04-c243
+vet-chu04-c243:
+	@bash ./scripts/vet-chu04-c243.sh
+
+.PHONY: stage-chu04-c243
+stage-chu04-c243:
+	@bash ./scripts/stage-chu04-c243.sh
+
+.PHONY: commit-chu04-c243
+commit-chu04-c243:
+	@bash ./scripts/commit-chu04-c243.sh
+
+.PHONY: push-chu04-c243
+push-chu04-c243:
+	@bash ./scripts/push-chu04-c243.sh
PATCH
if ! git show :Makefile 2>/dev/null | rg -q '^stage-chu04-c243:'; then
  git apply --cached --unidiff-zero "$patch_file"
fi

while IFS= read -r path; do
  if [[ -n "$path" ]] && ! is_allowed "$path"; then
    printf 'refusing to leave unrelated staged path: %s\n' "$path" >&2
    exit 1
  fi
done < <(git diff --cached --name-only)
git diff --cached --check
