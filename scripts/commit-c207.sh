#!/bin/sh
set -eu

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
  exit 1
fi

git add -- \
  BENCHMARK.md \
  C207_QUERY_CONDITION_CACHE.md \
  INSPIRATION_ROUND2.md \
  scripts/race-c207.sh \
  scripts/vet-c207.sh \
  scripts/commit-c207.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
git show HEAD:Makefile >"$makefile_stage"

printf '%s\n' '' '.PHONY: race-c207' 'race-c207:' >>"$makefile_stage"
printf '\tsh scripts/race-c207.sh\n' >>"$makefile_stage"
printf '%s\n' '' '.PHONY: vet-c207' 'vet-c207:' >>"$makefile_stage"
printf '\tsh scripts/vet-c207.sh\n' >>"$makefile_stage"
printf '%s\n' '' '.PHONY: commit-c207' 'commit-c207:' >>"$makefile_stage"
printf '\tsh scripts/commit-c207.sh\n' >>"$makefile_stage"

makefile_blob=$(git hash-object -w "$makefile_stage")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git commit -m 'docs: verify versioned query condition cache'
