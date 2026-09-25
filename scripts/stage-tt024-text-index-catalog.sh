#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
  Makefile
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  TT024_TEXT_INDEX_CATALOG.md
  hat/hatSchema/tt024_text_index_catalog.go
  hat/hatSchema/tt024_text_index_catalog_test.go
  hat/hatSchema/tt024_text_index_catalog_benchmark_test.go
  scripts/test-tt024-text-index-catalog.sh
  scripts/benchmark-tt024-text-index-catalog.sh
  scripts/format-tt024-text-index-catalog.sh
  scripts/race-tt024-text-index-catalog.sh
  scripts/vet-tt024-text-index-catalog.sh
  scripts/stage-tt024-text-index-catalog.sh
  scripts/commit-tt024-text-index-catalog.sh
  scripts/push-tt024-text-index-catalog.sh
)

mapfile -t existing_staged < <(git diff --cached --name-only --)
if (( ${#existing_staged[@]} != 0 )); then
  printf 'refusing to mix with existing staged paths:\n' >&2
  printf '  %s\n' "${existing_staged[@]}" >&2
  exit 1
fi

for path in "${expected_paths[@]}"; do
  [[ -f "$path" ]] || {
    printf 'required file is missing: %s\n' "$path" >&2
    exit 1
  }
done

tmp_makefile=$(mktemp)
cleanup() {
  rm -f -- "$tmp_makefile"
}
trap cleanup EXIT

git show HEAD:Makefile > "$tmp_makefile"
cat >> "$tmp_makefile" <<'EOF'

.PHONY: test-tt024-text-index-catalog benchmark-tt024-text-index-catalog format-tt024-text-index-catalog race-tt024-text-index-catalog vet-tt024-text-index-catalog
test-tt024-text-index-catalog:
	bash ./scripts/test-tt024-text-index-catalog.sh
benchmark-tt024-text-index-catalog:
	bash ./scripts/benchmark-tt024-text-index-catalog.sh
format-tt024-text-index-catalog:
	bash ./scripts/format-tt024-text-index-catalog.sh
race-tt024-text-index-catalog:
	bash ./scripts/race-tt024-text-index-catalog.sh
vet-tt024-text-index-catalog:
	bash ./scripts/vet-tt024-text-index-catalog.sh
stage-tt024-text-index-catalog:
	bash ./scripts/stage-tt024-text-index-catalog.sh
commit-tt024-text-index-catalog:
	bash ./scripts/commit-tt024-text-index-catalog.sh
push-tt024-text-index-catalog:
	bash ./scripts/push-tt024-text-index-catalog.sh
EOF

makefile_blob=$(git hash-object -w "$tmp_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  TT024_TEXT_INDEX_CATALOG.md \
  hat/hatSchema/tt024_text_index_catalog.go \
  hat/hatSchema/tt024_text_index_catalog_test.go \
  hat/hatSchema/tt024_text_index_catalog_benchmark_test.go \
  scripts/test-tt024-text-index-catalog.sh \
  scripts/benchmark-tt024-text-index-catalog.sh \
  scripts/format-tt024-text-index-catalog.sh \
  scripts/race-tt024-text-index-catalog.sh \
  scripts/vet-tt024-text-index-catalog.sh \
  scripts/stage-tt024-text-index-catalog.sh \
  scripts/commit-tt024-text-index-catalog.sh \
  scripts/push-tt024-text-index-catalog.sh
git diff --cached --check
printf 'Staged TT-024 text-index catalog paths:\n'
git diff --cached --name-only --
