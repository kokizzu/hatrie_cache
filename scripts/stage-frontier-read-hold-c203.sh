#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf 'refusing to stage frontier read hold with pre-existing staged changes\n' >&2
  exit 1
fi

head_makefile="$(mktemp)"
clean_makefile="$(mktemp)"
trap 'rm -f -- "$head_makefile" "$clean_makefile"' EXIT

git show HEAD:Makefile > "$head_makefile"
cp "$head_makefile" "$clean_makefile"
if ! grep -Fq 'test-frontier-read-hold-c203:' "$head_makefile"; then
  printf '\n.PHONY: test-frontier-read-hold-c203 benchmark-frontier-read-hold-c203 race-frontier-read-hold-c203 vet-frontier-read-hold-c203 format-frontier-read-hold-c203\ntest-frontier-read-hold-c203:\n\tbash ./scripts/test-frontier-read-hold-c203.sh\n\nbenchmark-frontier-read-hold-c203:\n\tbash ./scripts/benchmark-frontier-read-hold-c203.sh\n\nrace-frontier-read-hold-c203:\n\tbash ./scripts/race-frontier-read-hold-c203.sh\n\nvet-frontier-read-hold-c203:\n\tbash ./scripts/vet-frontier-read-hold-c203.sh\n\nformat-frontier-read-hold-c203:\n\tbash ./scripts/format-frontier-read-hold-c203.sh\n\n.PHONY: stage-frontier-read-hold-c203 commit-frontier-read-hold-c203 push-frontier-read-hold-c203\nstage-frontier-read-hold-c203:\n\tbash ./scripts/stage-frontier-read-hold-c203.sh\n\ncommit-frontier-read-hold-c203:\n\tbash ./scripts/commit-frontier-read-hold-c203.sh\n\npush-frontier-read-hold-c203:\n\tbash ./scripts/push-frontier-read-hold-c203.sh\n' >> "$clean_makefile"
fi

makefile_blob="$(git hash-object -w "$clean_makefile")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- FRONTIER_READ_HOLDS.md hat/hatDataStructure/frontier_read_hold.go hat/hatDataStructure/frontier_read_hold_test.go hat/hatDataStructure/frontier_read_hold_benchmark_test.go scripts/test-frontier-read-hold-c203.sh scripts/benchmark-frontier-read-hold-c203.sh scripts/race-frontier-read-hold-c203.sh scripts/vet-frontier-read-hold-c203.sh scripts/format-frontier-read-hold-c203.sh scripts/stage-frontier-read-hold-c203.sh scripts/commit-frontier-read-hold-c203.sh scripts/push-frontier-read-hold-c203.sh
git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
