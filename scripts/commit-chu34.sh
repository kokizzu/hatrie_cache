#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

index_file="$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-chu34-index.XXXXXX")"
stage_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu34-stage.XXXXXX")"
trap 'rm -f "$index_file"; rm -rf "$stage_root"' EXIT

GIT_INDEX_FILE="$index_file" git read-tree HEAD

paths=(
  "BENCHMARK.md"
  "CHU34_SQL_MUTATION_IDEMPOTENCY.md"
  "Makefile"
  "PRODUCT_IDEA_GAPS.md"
  "README.md"
  "hat/hatCache/chu34_sql_mutation_benchmark_test.go"
  "hat/hatCache/chu34_sql_mutation_http_test.go"
  "hat/hatCache/journal.go"
  "hat/hatCache/monitoring.go"
  "hat/hatCache/sql.go"
  "hat/hatSql/model.go"
  "scripts/benchmark-chu34.sh"
  "scripts/commit-chu34.sh"
  "scripts/format-chu34.sh"
  "scripts/race-chu34.sh"
  "scripts/push-chu34.sh"
  "scripts/test-chu34-package.sh"
  "scripts/test-chu34.sh"
  "scripts/verify-chu34.sh"
  "scripts/vet-chu34.sh"
)

for path in "${paths[@]}"; do
  source_path="$repo_root/$path"
  if [[ ! -f "$source_path" ]]; then
    printf 'missing CH-U34 path: %s\n' "$path" >&2
    exit 1
  fi

  target_path="$stage_root/$path"
  mkdir -p "$(dirname "$target_path")"
  if [[ "$path" == "Makefile" ]]; then
    git show HEAD:Makefile > "$target_path"
    cat >> "$target_path" <<'MAKEFILE_APPEND'

.PHONY: test-chu34
test-chu34:
	bash ./scripts/test-chu34.sh

.PHONY: benchmark-chu34
benchmark-chu34:
	bash ./scripts/benchmark-chu34.sh

.PHONY: format-chu34
format-chu34:
	bash ./scripts/format-chu34.sh

.PHONY: test-chu34-package
test-chu34-package:
	bash ./scripts/test-chu34-package.sh

.PHONY: race-chu34
race-chu34:
	bash ./scripts/race-chu34.sh

.PHONY: vet-chu34
vet-chu34:
	bash ./scripts/vet-chu34.sh

.PHONY: verify-chu34
verify-chu34:
	bash ./scripts/verify-chu34.sh

.PHONY: commit-chu34 push-chu34
commit-chu34:
	bash ./scripts/commit-chu34.sh

push-chu34:
	bash ./scripts/push-chu34.sh
MAKEFILE_APPEND
  else
    cp -- "$source_path" "$target_path"
  fi

  blob="$(git hash-object -w "$target_path")"
  GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo "100644,$blob,$path"
done

printf 'CH-U34 synthetic staged paths (parallel staged changes excluded):\n'
GIT_INDEX_FILE="$index_file" git diff --cached --name-status
GIT_INDEX_FILE="$index_file" git diff --cached --check
GIT_INDEX_FILE="$index_file" git commit -m "feat(sql): add durable HTTP mutation retries"
