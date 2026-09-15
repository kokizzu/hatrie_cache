#!/usr/bin/env bash
set -euo pipefail

is_allowed() {
  case "$1" in
    ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|CHU02_EXTERNAL_ORDER_SPILL.md|Makefile|PRODUCT_IDEA_GAPS.md|README.md|hat/hatSql/contracts.go|hat/hatSql/external.go|hat/hatSql/query.go|hat/hatSql/chu02_external_order_spill_test.go|hat/hatSql/chu02_external_order_spill_benchmark_test.go|scripts/benchmark-chu02-c242.sh|scripts/commit-chu02-c242.sh|scripts/format-chu02-c242.sh|scripts/memory-chu02-c242.sh|scripts/push-chu02-c242.sh|scripts/race-chu02-c242.sh|scripts/test-chu02-c242.sh|scripts/test-chu02-package-c242.sh|scripts/vet-chu02-c242.sh|scripts/stage-chu02-c242.sh)
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
  CHU02_EXTERNAL_ORDER_SPILL.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/contracts.go \
  hat/hatSql/external.go \
  hat/hatSql/query.go \
  hat/hatSql/chu02_external_order_spill_test.go \
  hat/hatSql/chu02_external_order_spill_benchmark_test.go \
  scripts/benchmark-chu02-c242.sh \
  scripts/commit-chu02-c242.sh \
  scripts/format-chu02-c242.sh \
  scripts/memory-chu02-c242.sh \
  scripts/push-chu02-c242.sh \
  scripts/race-chu02-c242.sh \
  scripts/stage-chu02-c242.sh \
  scripts/test-chu02-c242.sh \
  scripts/test-chu02-package-c242.sh \
  scripts/vet-chu02-c242.sh

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
+.PHONY: format-chu02-c242
+format-chu02-c242:
+	@bash ./scripts/format-chu02-c242.sh
+
+.PHONY: test-chu02-c242
+test-chu02-c242:
+	@bash ./scripts/test-chu02-c242.sh
+
+.PHONY: benchmark-chu02-c242
+benchmark-chu02-c242:
+	@bash ./scripts/benchmark-chu02-c242.sh
+
+.PHONY: test-chu02-package-c242
+test-chu02-package-c242:
+	@bash ./scripts/test-chu02-package-c242.sh
+
+.PHONY: race-chu02-c242
+race-chu02-c242:
+	@bash ./scripts/race-chu02-c242.sh
+
+.PHONY: vet-chu02-c242
+vet-chu02-c242:
+	@bash ./scripts/vet-chu02-c242.sh
+
+.PHONY: memory-chu02-c242
+memory-chu02-c242:
+	@bash ./scripts/memory-chu02-c242.sh
+
+.PHONY: stage-chu02-c242
+stage-chu02-c242:
+	@bash ./scripts/stage-chu02-c242.sh
+
+.PHONY: commit-chu02-c242
+commit-chu02-c242:
+	@bash ./scripts/commit-chu02-c242.sh
+
+.PHONY: push-chu02-c242
+push-chu02-c242:
+	@bash ./scripts/push-chu02-c242.sh
PATCH
if ! git show :Makefile 2>/dev/null | rg -q '^stage-chu02-c242:'; then
  git apply --cached --unidiff-zero "$patch_file"
fi

while IFS= read -r path; do
  if [[ -n "$path" ]] && ! is_allowed "$path"; then
    printf 'refusing to leave unrelated staged path: %s\n' "$path" >&2
    exit 1
  fi
done < <(git diff --cached --name-only)
git diff --cached --check
