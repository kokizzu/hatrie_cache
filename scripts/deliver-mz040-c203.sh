#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
makefile_tmp=$(mktemp /tmp/hatrie-cache-mz040-makefile.XXXXXX)
cleanup() {
  rm -f "$makefile_tmp"
}
trap cleanup EXIT

feature_paths=(
  BENCHMARK.md
  INSPIRATION_BACKLOG.md
  MZ040_RECURSIVE_CONVERGENCE_BOUNDS.md
  README.md
  hat/hatSql/mz040_recursive_diagnostics.go
  hat/hatSql/mz040_recursive_diagnostics_test.go
  hat/hatSql/mz040_recursive_diagnostics_benchmark_test.go
  scripts/benchmark-mz040-before-c203.sh
  scripts/benchmark-mz040-c203.sh
  scripts/deliver-mz040-c203.sh
  scripts/format-mz040-c203.sh
  scripts/race-mz040-c203.sh
  scripts/test-mz040-c203.sh
  scripts/test-mz040-red-c203.sh
  scripts/verify-mz040-c203.sh
  scripts/vet-mz040-c203.sh
)

write_feature_makefile() {
  git -C "$repo_root" show HEAD:Makefile > "$makefile_tmp"
  printf '%s\n' \
    '' \
    '.PHONY: format-mz040-c203' \
    'format-mz040-c203:' \
    $'\tbash ./scripts/format-mz040-c203.sh' \
    '' \
    '.PHONY: test-mz040-red-c203' \
    'test-mz040-red-c203:' \
    $'\tbash ./scripts/test-mz040-red-c203.sh' \
    '' \
    '.PHONY: test-mz040-c203' \
    'test-mz040-c203:' \
    $'\tbash ./scripts/test-mz040-c203.sh' \
    '' \
    '.PHONY: race-mz040-c203' \
    'race-mz040-c203:' \
    $'\tbash ./scripts/race-mz040-c203.sh' \
    '' \
    '.PHONY: vet-mz040-c203' \
    'vet-mz040-c203:' \
    $'\tbash ./scripts/vet-mz040-c203.sh' \
    '' \
    '.PHONY: benchmark-mz040-before-c203' \
    'benchmark-mz040-before-c203:' \
    $'\tbash ./scripts/benchmark-mz040-before-c203.sh' \
    '' \
    '.PHONY: benchmark-mz040-c203' \
    'benchmark-mz040-c203:' \
    $'\tbash ./scripts/benchmark-mz040-c203.sh' \
    '' \
    '.PHONY: verify-mz040-c203' \
    'verify-mz040-c203:' \
    $'\tbash ./scripts/verify-mz040-c203.sh' \
    '' \
    '.PHONY: stage-mz040-c203' \
    'stage-mz040-c203:' \
    $'\tbash ./scripts/deliver-mz040-c203.sh stage' \
    '' \
    '.PHONY: commit-mz040-c203' \
    'commit-mz040-c203:' \
    $'\tbash ./scripts/deliver-mz040-c203.sh commit' \
    '' \
    '.PHONY: push-mz040-c203' \
    'push-mz040-c203:' \
    $'\tbash ./scripts/deliver-mz040-c203.sh push' >> "$makefile_tmp"
}

verify_staged_scope() {
  local path allowed
  while IFS= read -r path; do
    allowed=false
    if [[ "$path" == "Makefile" ]]; then
      allowed=true
    else
      for feature_path in "${feature_paths[@]}"; do
        if [[ "$path" == "$feature_path" ]]; then
          allowed=true
          break
        fi
      done
    fi
    if [[ "$allowed" != true ]]; then
      printf 'unexpected staged path: %s\n' "$path" >&2
      exit 1
    fi
  done < <(git -C "$repo_root" diff --cached --name-only)
}

stage_feature() {
  write_feature_makefile
  git -C "$repo_root" add -- "${feature_paths[@]}"
  makefile_blob=$(git -C "$repo_root" hash-object -w "$makefile_tmp")
  git -C "$repo_root" update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
  verify_staged_scope
}

case "${1:-status}" in
status)
  git -C "$repo_root" status --short
  ;;
stage)
  stage_feature
  git -C "$repo_root" diff --cached --stat
  ;;
commit)
  stage_feature
  git -C "$repo_root" commit -m '[skip ci] Add recursive convergence bounds'
  ;;
push)
  git -C "$repo_root" push origin HEAD:master
  ;;
*)
  printf 'unknown delivery mode: %s\n' "$1" >&2
  exit 2
  ;;
esac
