#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CH012_DELETE_BITMAP.md
  ENGINE_IDEAS.md
  INSPIRATION_BACKLOG.md
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/ch012_delete_bitmap_benchmark_test.go
  hat/hatSql/ch012_delete_bitmap_test.go
  hat/hatSql/typed_table.go
  hat/hatSql/typed_table_delete_bitmap.go
  hat/hatSql/typed_table_patch_parts.go
  scripts/benchmark-ch012-c203.sh
  scripts/benchmark-ch012-mask-c203.sh
  scripts/benchmark-ch012-rows-c203.sh
  scripts/format-ch012-c203.sh
  scripts/inspect-ch012-c203.sh
  scripts/inspect-inspiration-backlog-c203.sh
  scripts/stage-ch012-c203.sh
  scripts/inspect-staged-ch012-c203.sh
  scripts/commit-ch012-c203.sh
  scripts/push-ch012-c203.sh
  scripts/test-ch012-c203.sh
  scripts/verify-ch012-c203.sh
)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to mix an existing index with CH-012' >&2
  exit 1
fi

for path in "${feature_paths[@]}"; do
  if [[ ! -e "$path" ]]; then
    printf 'missing CH-012 path: %s\n' "$path" >&2
    exit 1
  fi
done

makefile_tmp=$(mktemp)
trap 'rm -f "$makefile_tmp"' EXIT
git show HEAD:Makefile >"$makefile_tmp"
if ! rg -q '^\.PHONY: test-ch012-c203$' "$makefile_tmp"; then
  cat >>"$makefile_tmp" <<'EOF'

.PHONY: inspect-ch012-c203
inspect-ch012-c203:
	bash ./scripts/inspect-ch012-c203.sh

.PHONY: inspect-inspiration-backlog-c203
inspect-inspiration-backlog-c203:
	bash ./scripts/inspect-inspiration-backlog-c203.sh

.PHONY: test-ch012-c203
test-ch012-c203:
	bash ./scripts/test-ch012-c203.sh

.PHONY: format-ch012-c203
format-ch012-c203:
	bash ./scripts/format-ch012-c203.sh

.PHONY: benchmark-ch012-c203
benchmark-ch012-c203:
	bash ./scripts/benchmark-ch012-c203.sh

.PHONY: benchmark-ch012-mask-c203
benchmark-ch012-mask-c203:
	bash ./scripts/benchmark-ch012-mask-c203.sh

.PHONY: benchmark-ch012-rows-c203
benchmark-ch012-rows-c203:
	bash ./scripts/benchmark-ch012-rows-c203.sh

.PHONY: verify-ch012-c203
verify-ch012-c203:
	bash ./scripts/verify-ch012-c203.sh

.PHONY: stage-ch012-c203
stage-ch012-c203:
	bash ./scripts/stage-ch012-c203.sh

.PHONY: inspect-staged-ch012-c203
inspect-staged-ch012-c203:
	bash ./scripts/inspect-staged-ch012-c203.sh

.PHONY: commit-ch012-c203
commit-ch012-c203:
	bash ./scripts/commit-ch012-c203.sh

.PHONY: push-ch012-c203
push-ch012-c203:
	bash ./scripts/push-ch012-c203.sh
EOF
fi

makefile_blob=$(git hash-object -w "$makefile_tmp")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- "${feature_paths[@]}"
