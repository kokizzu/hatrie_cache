#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
{
printf '\n'
printf '%s\n' \
  'stage-rejected-async-batcher-c214:' \
  $'\t@bash ./scripts/stage-rejected-async-batcher-c214.sh' \
  '' \
  'commit-rejected-async-batcher-c214:' \
  $'\t@bash ./scripts/commit-rejected-async-batcher-c214.sh' \
  '' \
  'push-rejected-async-batcher-c214:' \
  $'\t@bash ./scripts/push-rejected-async-batcher-c214.sh' >> "$base_makefile"
}

git add BENCHMARK.md scripts/stage-rejected-async-batcher-c214.sh scripts/commit-rejected-async-batcher-c214.sh scripts/push-rejected-async-batcher-c214.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
