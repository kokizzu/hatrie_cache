#!/bin/sh
set -eu

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
	exit 1
fi

git add -- \
  BENCHMARK.md \
  CH042_APPROX_DISTINCT_STATE.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/approx_aggregate.go \
  hat/hatSql/ch042_approx_distinct_state.go \
  hat/hatSql/ch042_approx_distinct_state_benchmark_test.go \
  hat/hatSql/ch042_approx_distinct_state_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-chu42.sh \
  scripts/commit-chu42.sh \
  scripts/format-chu42.sh \
  scripts/race-chu42.sh \
  scripts/test-chu42-package.sh \
  scripts/test-chu42.sh \
  scripts/vet-chu42.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
git show HEAD:Makefile >"$makefile_stage"
cat >>"$makefile_stage" <<'EOF'

.PHONY: test-chu42
test-chu42:
	sh scripts/test-chu42.sh

.PHONY: test-chu42-package
test-chu42-package:
	sh scripts/test-chu42-package.sh

.PHONY: benchmark-chu42
benchmark-chu42:
	sh scripts/benchmark-chu42.sh

.PHONY: format-chu42
format-chu42:
	sh scripts/format-chu42.sh

.PHONY: race-chu42
race-chu42:
	sh scripts/race-chu42.sh

.PHONY: vet-chu42
vet-chu42:
	sh scripts/vet-chu42.sh

.PHONY: commit-chu42
commit-chu42:
	sh scripts/commit-chu42.sh
EOF
makefile_hash=$(git hash-object -w "$makefile_stage")
git update-index --add --cacheinfo 100644 "$makefile_hash" Makefile
git commit -m 'feat(sql): add mergeable approximate distinct states'
