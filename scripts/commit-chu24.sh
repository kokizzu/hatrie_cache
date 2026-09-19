#!/bin/sh
set -eu

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
	exit 1
fi

git add -- \
  BENCHMARK.md \
  CHU24_TYPED_TABLE_MEMORY_BUDGET.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/chu24_typed_table_memory_budget_benchmark_test.go \
  hat/hatSql/chu24_typed_table_memory_budget_test.go \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_columnar_append.go \
  hat/hatSql/typed_table_patch_parts.go \
  scripts/benchmark-chu24.sh \
  scripts/commit-chu24.sh \
  scripts/format-chu24.sh \
  scripts/race-chu24.sh \
  scripts/test-chu24-package.sh \
  scripts/test-chu24.sh \
  scripts/vet-chu24.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
git show HEAD:Makefile >"$makefile_stage"
cat >>"$makefile_stage" <<'EOF'

.PHONY: test-chu24
test-chu24:
	bash ./scripts/test-chu24.sh

.PHONY: format-chu24
format-chu24:
	bash ./scripts/format-chu24.sh

.PHONY: benchmark-chu24
benchmark-chu24:
	bash ./scripts/benchmark-chu24.sh

.PHONY: test-chu24-package
test-chu24-package:
	bash ./scripts/test-chu24-package.sh

.PHONY: race-chu24
race-chu24:
	bash ./scripts/race-chu24.sh

.PHONY: vet-chu24
vet-chu24:
	bash ./scripts/vet-chu24.sh

.PHONY: commit-chu24
commit-chu24:
	bash ./scripts/commit-chu24.sh
EOF
makefile_hash=$(git hash-object -w "$makefile_stage")
git update-index --add --cacheinfo 100644 "$makefile_hash" Makefile
git diff --cached --check
git commit -m 'feat(sql): add typed-table memory budgets'
