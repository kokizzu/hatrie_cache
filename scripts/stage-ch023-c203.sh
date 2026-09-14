#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION_BACKLOG.md
  README.md
  SQL_INDEX_ADVISOR.md
  hat/hatCache/sql_query.go
  hat/hatSql/ch023_primary_prefix_test.go
  hat/hatSql/index_advisor.go
  hat/hatSql/index_advisor_persistence.go
  hat/hatSql/index_advisor_persistence_test.go
  scripts/benchmark-ch023-c203.sh
  scripts/format-ch023-c203.sh
  scripts/test-ch023-c203.sh
  scripts/verify-ch023-c203.sh
  scripts/verify-ch023-focused-c203.sh
  scripts/stage-ch023-c203.sh
  scripts/inspect-staged-ch023-c203.sh
  scripts/commit-ch023-c203.sh
  scripts/push-ch023-c203.sh
)

git add -- "${feature_paths[@]}"

head_makefile=$(mktemp)
feature_makefile=$(mktemp)
makefile_patch=$(mktemp)
trap 'rm -f "$head_makefile" "$feature_makefile" "$makefile_patch"' EXIT

git show HEAD:Makefile > "$head_makefile"
cp "$head_makefile" "$feature_makefile"
cat >> "$feature_makefile" <<'EOF'

.PHONY: test-ch023-c203
test-ch023-c203:
	bash ./scripts/test-ch023-c203.sh

.PHONY: benchmark-ch023-c203
benchmark-ch023-c203:
	bash ./scripts/benchmark-ch023-c203.sh before
	bash ./scripts/benchmark-ch023-c203.sh after

.PHONY: benchmark-ch023-after-c203
benchmark-ch023-after-c203:
	bash ./scripts/benchmark-ch023-c203.sh after

.PHONY: format-ch023-c203
format-ch023-c203:
	bash ./scripts/format-ch023-c203.sh

.PHONY: verify-ch023-c203
verify-ch023-c203:
	bash ./scripts/verify-ch023-c203.sh

.PHONY: verify-ch023-focused-c203
verify-ch023-focused-c203:
	bash ./scripts/verify-ch023-focused-c203.sh

.PHONY: stage-ch023-c203
stage-ch023-c203:
	bash ./scripts/stage-ch023-c203.sh

.PHONY: inspect-staged-ch023-c203
inspect-staged-ch023-c203:
	bash ./scripts/inspect-staged-ch023-c203.sh

.PHONY: commit-ch023-c203
commit-ch023-c203:
	bash ./scripts/commit-ch023-c203.sh

.PHONY: push-ch023-c203
push-ch023-c203:
	bash ./scripts/push-ch023-c203.sh
EOF

set +e
diff -u --label a/Makefile --label b/Makefile "$head_makefile" "$feature_makefile" > "$makefile_patch"
diff_status=$?
set -e
if [[ "$diff_status" -eq 1 ]]; then
  git apply --cached "$makefile_patch"
elif [[ "$diff_status" -ne 0 ]]; then
  exit "$diff_status"
fi
