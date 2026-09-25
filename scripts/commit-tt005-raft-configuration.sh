#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
  exit 1
fi

base_file=$(mktemp)
desired_file=$(mktemp)
raw_patch=$(mktemp)
makefile_patch=$(mktemp)
trap 'rm -f "$base_file" "$desired_file" "$raw_patch" "$makefile_patch"' EXIT

git show HEAD:Makefile > "$base_file"
{
  printf '%s\n' "$(<"$base_file")"
  printf '%s\n' \
    '' \
    '.PHONY: test-tt005-raft-configuration' \
    'test-tt005-raft-configuration:' \
    $'\tbash ./scripts/test-tt005-raft-configuration.sh' \
    '' \
    '.PHONY: format-tt005-raft-configuration' \
    'format-tt005-raft-configuration:' \
    $'\tbash ./scripts/format-tt005-raft-configuration.sh' \
    '' \
    '.PHONY: benchmark-tt005-raft-configuration' \
    'benchmark-tt005-raft-configuration:' \
    $'\tbash ./scripts/benchmark-tt005-raft-configuration.sh' \
    '' \
    '.PHONY: test-tt005-package' \
    'test-tt005-package:' \
    $'\tbash ./scripts/test-tt005-package.sh' \
    '' \
    '.PHONY: race-tt005-raft-configuration' \
    'race-tt005-raft-configuration:' \
    $'\tbash ./scripts/race-tt005-raft-configuration.sh' \
    '' \
    '.PHONY: vet-tt005-raft-configuration' \
    'vet-tt005-raft-configuration:' \
    $'\tbash ./scripts/vet-tt005-raft-configuration.sh' \
    '' \
    '.PHONY: compile-tt005-all' \
    'compile-tt005-all:' \
    $'\tbash ./scripts/compile-tt005-all.sh' \
    '' \
    '.PHONY: commit-tt005-raft-configuration' \
    'commit-tt005-raft-configuration:' \
    $'\tbash ./scripts/commit-tt005-raft-configuration.sh' \
    '' \
    '.PHONY: push-tt005-raft-configuration' \
    'push-tt005-raft-configuration:' \
    $'\tbash ./scripts/push-tt005-raft-configuration.sh'
} > "$desired_file"

set +e
git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$base_file" "$desired_file" > "$raw_patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
  printf 'unexpected Makefile diff status: %d\n' "$diff_status" >&2
  exit 1
fi
sed \
  -e "s|a$base_file|a/Makefile|g" \
  -e "s|b$desired_file|b/Makefile|g" \
  -e "s|$base_file|Makefile|g" \
  -e "s|$desired_file|Makefile|g" \
  "$raw_patch" > "$makefile_patch"
git apply --cached "$makefile_patch"

git add -- \
  ENGINE_IDEAS.md \
  TT005_RAFT_CONFIGURATION.md \
  hat/hatTopology/tt005_raft_configuration.go \
  hat/hatTopology/tt005_raft_configuration_test.go \
  hat/hatTopology/tt005_raft_configuration_benchmark_test.go \
  scripts/benchmark-tt005-raft-configuration.sh \
  scripts/commit-tt005-raft-configuration.sh \
  scripts/compile-tt005-all.sh \
  scripts/format-tt005-raft-configuration.sh \
  scripts/push-tt005-raft-configuration.sh \
  scripts/race-tt005-raft-configuration.sh \
  scripts/test-tt005-raft-configuration.sh \
  scripts/test-tt005-package.sh \
  scripts/vet-tt005-raft-configuration.sh

git diff --cached --check
git commit -m 'feat: add raft configuration state machine'
