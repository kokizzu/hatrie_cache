#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION_BACKLOG.md
  README.md
  TR019_TUPLE_FIELD_OFFSETS.md
  hat/hatSql/columnar_field_offsets.go
  hat/hatSql/contracts.go
  hat/hatSql/typed_table.go
  hat/hatSql/tr019_tuple_field_offset_test.go
  hat/hatSql/tr019_tuple_field_offset_benchmark_test.go
  scripts/benchmark-tr019-c203.sh
  scripts/test-tr019-c203.sh
  scripts/format-tr019-c203.sh
  scripts/verify-tr019-c203.sh
  scripts/stage-tr019-c203.sh
  scripts/inspect-staged-tr019-c203.sh
  scripts/commit-tr019-c203.sh
  scripts/push-tr019-c203.sh
)

git add -- "${feature_paths[@]}"

head_makefile=$(mktemp)
candidate_makefile=$(mktemp)
makefile_patch=$(mktemp)
trap 'rm -f "$head_makefile" "$candidate_makefile" "$makefile_patch"' EXIT

git show HEAD:Makefile > "$head_makefile"
cp "$head_makefile" "$candidate_makefile"
cat >> "$candidate_makefile" <<'EOF'

.PHONY: benchmark-tr019-c203
benchmark-tr019-c203:
	bash ./scripts/benchmark-tr019-c203.sh

.PHONY: test-tr019-c203
test-tr019-c203:
	bash ./scripts/test-tr019-c203.sh

.PHONY: format-tr019-c203
format-tr019-c203:
	bash ./scripts/format-tr019-c203.sh

.PHONY: verify-tr019-c203
verify-tr019-c203:
	bash ./scripts/verify-tr019-c203.sh

.PHONY: stage-tr019-c203
stage-tr019-c203:
	bash ./scripts/stage-tr019-c203.sh

.PHONY: inspect-staged-tr019-c203
inspect-staged-tr019-c203:
	bash ./scripts/inspect-staged-tr019-c203.sh

.PHONY: commit-tr019-c203
commit-tr019-c203:
	bash ./scripts/commit-tr019-c203.sh

.PHONY: push-tr019-c203
push-tr019-c203:
	bash ./scripts/push-tr019-c203.sh
EOF

set +e
diff -u --label a/Makefile --label b/Makefile "$head_makefile" "$candidate_makefile" > "$makefile_patch"
diff_status=$?
set -e
if [[ "$diff_status" -eq 1 ]]; then
  git apply --cached "$makefile_patch"
elif [[ "$diff_status" -ne 0 ]]; then
  exit "$diff_status"
fi
