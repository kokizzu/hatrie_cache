#!/usr/bin/env bash
set -euo pipefail

files=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CHU17_DENSE_INTEGER_IN.md
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatSql/chu17_in_program_benchmark_test.go
  hat/hatSql/chu17_in_program_test.go
  hat/hatSql/in_program.go
  scripts/benchmark-chu17-c240.sh
  scripts/commit-chu17-c240.sh
  scripts/format-chu17-c240.sh
  scripts/push-chu17-c240.sh
  scripts/race-chu17-c240.sh
  scripts/stage-chu17-c240.sh
  scripts/test-chu17-c240.sh
  scripts/test-chu17-package-c240.sh
  scripts/vet-chu17-c240.sh
)

for file in "${files[@]}"; do
  if [[ ! -f "$file" ]]; then
    printf 'missing feature file: %s\n' "$file" >&2
    exit 1
  fi
done

if [[ -n "$(git diff --cached --name-only)" ]]; then
  printf '%s\n' 'refusing to stage with an existing index; inspect the staged changes first' >&2
  git diff --cached --name-only >&2
  exit 1
fi

git add -- "${files[@]}"

tmp_makefile="$(mktemp /tmp/hatrie-cache-chu17-stage-makefile.XXXXXX)"
patch_file="$(mktemp /tmp/hatrie-cache-chu17-stage-patch.XXXXXX)"
trap 'rm -f "$tmp_makefile" "$patch_file"' EXIT

git show HEAD:Makefile > "$tmp_makefile"
cat >> "$tmp_makefile" <<'EOF'

.PHONY: test-chu17-c240
test-chu17-c240:
	bash ./scripts/test-chu17-c240.sh

.PHONY: benchmark-chu17-c240
benchmark-chu17-c240:
	bash ./scripts/benchmark-chu17-c240.sh

.PHONY: test-chu17-package-c240
test-chu17-package-c240:
	bash ./scripts/test-chu17-package-c240.sh

.PHONY: format-chu17-c240
format-chu17-c240:
	bash ./scripts/format-chu17-c240.sh

.PHONY: race-chu17-c240
race-chu17-c240:
	bash ./scripts/race-chu17-c240.sh

.PHONY: vet-chu17-c240
vet-chu17-c240:
	bash ./scripts/vet-chu17-c240.sh

.PHONY: stage-chu17-c240
stage-chu17-c240:
	bash ./scripts/stage-chu17-c240.sh

.PHONY: commit-chu17-c240
commit-chu17-c240:
	bash ./scripts/commit-chu17-c240.sh

.PHONY: push-chu17-c240
push-chu17-c240:
	bash ./scripts/push-chu17-c240.sh
EOF

if diff -u --label a/Makefile --label b/Makefile <(git show HEAD:Makefile) "$tmp_makefile" > "$patch_file"; then
  printf '%s\n' 'Makefile feature target patch unexpectedly empty' >&2
  exit 1
else
  diff_status=$?
  if [[ "$diff_status" -ne 1 ]]; then
    printf 'failed to create Makefile feature target patch (status %s)\n' "$diff_status" >&2
    exit "$diff_status"
  fi
fi

git apply --cached "$patch_file"
git diff --cached --check
git diff --cached --name-only
