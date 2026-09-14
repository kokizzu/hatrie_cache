#!/usr/bin/env bash
set -euo pipefail

staged_paths=$(git diff --cached --name-only)
if [ "$staged_paths" = $'Makefile\nscripts/push-ch030-map-c203.sh' ]; then
    git restore --staged -- Makefile
    git add -- scripts/push-ch030-map-c203.sh scripts/stage-ch030-map-c203.sh
    exit 0
fi

if ! git diff --cached --quiet; then
    printf '%s\n' 'refusing to stage CH030 while unrelated changes are already staged' >&2
    exit 1
fi

makefile_tmp=$(mktemp)
trap 'rm -f "$makefile_tmp"' EXIT
git show HEAD:Makefile >"$makefile_tmp"
if ! rg -q '^\.PHONY: test-ch030-map-c203$' "$makefile_tmp"; then
cat >>"$makefile_tmp" <<'EOF'

.PHONY: test-ch030-map-c203
test-ch030-map-c203:
	bash ./scripts/test-ch030-map-c203.sh

.PHONY: format-ch030-map-c203
format-ch030-map-c203:
	bash ./scripts/format-ch030-map-c203.sh

.PHONY: benchmark-ch030-map-c203
benchmark-ch030-map-c203:
	bash ./scripts/benchmark-ch030-map-c203.sh

.PHONY: verify-ch030-map-c203
verify-ch030-map-c203:
	bash ./scripts/verify-ch030-map-c203.sh

.PHONY: inspect-ch030-map-c203
inspect-ch030-map-c203:
	bash ./scripts/inspect-ch030-map-c203.sh

.PHONY: stage-ch030-map-c203
stage-ch030-map-c203:
	bash ./scripts/stage-ch030-map-c203.sh

.PHONY: inspect-staged-ch030-map-c203
inspect-staged-ch030-map-c203:
	bash ./scripts/inspect-staged-ch030-map-c203.sh

.PHONY: commit-ch030-map-c203
commit-ch030-map-c203:
	bash ./scripts/commit-ch030-map-c203.sh

.PHONY: push-ch030-map-c203
push-ch030-map-c203:
	bash ./scripts/push-ch030-map-c203.sh
EOF
fi
makefile_blob=$(git hash-object -w "$makefile_tmp")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CH030_MAP_SUBCOLUMNS.md INSPIRATION_BACKLOG.md README.md hat/hatSql/catalog.go hat/hatSql/ch030_map_subcolumn_benchmark_test.go hat/hatSql/ch030_map_subcolumn_test.go hat/hatSql/columnar_map.go hat/hatSql/columnar_map_scan.go hat/hatSql/columnar_vertical_merge.go hat/hatSql/contracts.go hat/hatSql/json_path.go hat/hatSql/query.go scripts/benchmark-ch030-map-c203.sh scripts/format-ch030-map-c203.sh scripts/inspect-ch030-map-c203.sh scripts/stage-ch030-map-c203.sh scripts/test-ch030-map-c203.sh scripts/verify-ch030-map-c203.sh scripts/inspect-staged-ch030-map-c203.sh scripts/commit-ch030-map-c203.sh scripts/push-ch030-map-c203.sh
