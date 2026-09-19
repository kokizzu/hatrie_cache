#!/bin/sh
set -eu

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
	exit 1
fi

git add -- \
  BENCHMARK.md \
  CHU43_TDIGEST_STATE.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/approx_aggregate.go \
  hat/hatSql/ch043_tdigest_state.go \
  hat/hatSql/chu43_tdigest_state_benchmark_test.go \
  hat/hatSql/chu43_tdigest_state_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-chu43.sh \
  scripts/commit-chu43.sh \
  scripts/format-chu43.sh \
  scripts/race-chu43.sh \
  scripts/test-chu43-package.sh \
  scripts/test-chu43.sh \
  scripts/vet-chu43.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
git show HEAD:Makefile >"$makefile_stage"
cat >>"$makefile_stage" <<'EOF'

.PHONY: test-chu43
test-chu43:
	sh scripts/test-chu43.sh

.PHONY: format-chu43
format-chu43:
	sh scripts/format-chu43.sh

.PHONY: benchmark-chu43
benchmark-chu43:
	sh scripts/benchmark-chu43.sh

.PHONY: test-chu43-package
test-chu43-package:
	sh scripts/test-chu43-package.sh

.PHONY: race-chu43
race-chu43:
	sh scripts/race-chu43.sh

.PHONY: vet-chu43
vet-chu43:
	sh scripts/vet-chu43.sh

.PHONY: commit-chu43
commit-chu43:
	sh scripts/commit-chu43.sh
EOF
makefile_hash=$(git hash-object -w "$makefile_stage")
git update-index --add --cacheinfo 100644 "$makefile_hash" Makefile
git commit -m 'feat(sql): add mergeable t-digest percentile states'
