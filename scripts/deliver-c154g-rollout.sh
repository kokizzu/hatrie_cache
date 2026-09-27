#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  C154G_DURABLE_ROLLING_SCHEMA_RUNNER.md
  hat/hatSchema/rolling_schema.go
  hat/hatSchema/c154g_durable_rollout_test.go
  hat/hatSchema/c154g_rollout_benchmark_test.go
  scripts/benchmark-c154g-rollout.sh
  scripts/test-c154g-rollout.sh
  scripts/format-c154g-rollout.sh
  scripts/test-c154g-package.sh
  scripts/race-c154g-rollout.sh
  scripts/vet-c154g-rollout.sh
  scripts/test-all-c154g-isolated.sh
  scripts/deliver-c154g-rollout.sh
)

stage_blob_path() {
  local source="$1"
  local index_path="$2"
  local blob
  blob="$(git hash-object -w -- "$source")"
  git update-index --add --cacheinfo "100644,$blob,$index_path"
}

stage_shared_files() {
  local tmp_dir="$1"
  local makefile_tmp="$tmp_dir/Makefile"
  local inspiration_tmp="$tmp_dir/INSPIRATION.md"

  git show HEAD:Makefile > "$makefile_tmp"
  printf '%s\n' \
    '.PHONY: benchmark-c154g-rollout' \
    'benchmark-c154g-rollout:' \
    $'\t@bash scripts/benchmark-c154g-rollout.sh' \
    '.PHONY: test-c154g-rollout' \
    'test-c154g-rollout:' \
    $'\t@bash scripts/test-c154g-rollout.sh' \
    '.PHONY: format-c154g-rollout' \
    'format-c154g-rollout:' \
    $'\t@bash scripts/format-c154g-rollout.sh' \
    '.PHONY: test-c154g-package' \
    'test-c154g-package:' \
    $'\t@bash scripts/test-c154g-package.sh' \
    '.PHONY: race-c154g-rollout' \
    'race-c154g-rollout:' \
    $'\t@bash scripts/race-c154g-rollout.sh' \
    '.PHONY: vet-c154g-rollout' \
    'vet-c154g-rollout:' \
    $'\t@bash scripts/vet-c154g-rollout.sh' \
    '.PHONY: test-all-c154g-isolated' \
    'test-all-c154g-isolated:' \
    $'\t@bash scripts/test-all-c154g-isolated.sh' \
    '.PHONY: stage-c154g-rollout' \
    'stage-c154g-rollout:' \
    $'\t@bash scripts/deliver-c154g-rollout.sh stage' \
    '.PHONY: commit-c154g-rollout' \
    'commit-c154g-rollout:' \
    $'\t@bash scripts/deliver-c154g-rollout.sh commit' \
    '.PHONY: push-c154g-rollout' \
    'push-c154g-rollout:' \
    $'\t@bash scripts/deliver-c154g-rollout.sh push' \
    '.PHONY: deliver-c154g-rollout' \
    'deliver-c154g-rollout:' \
    $'\t@bash scripts/deliver-c154g-rollout.sh deliver' \
    >> "$makefile_tmp"
  stage_blob_path "$makefile_tmp" Makefile

  git show HEAD:INSPIRATION.md | awk '
    {
      print
      if (!inserted && $0 ~ /C154F_SCHEMA_MIGRATION_BARRIER_SNAPSHOT\.md/) {
        print "- [x] C154g Opt-in checkpoint-aware rolling-schema execution persists every"
        print "  stable prepare/activate phase and resumes from the last durable checkpoint;"
        print "  transport, authentication, and storage remain caller-owned. See"
        print "  [C154G_DURABLE_ROLLING_SCHEMA_RUNNER.md](C154G_DURABLE_ROLLING_SCHEMA_RUNNER.md)."
        inserted = 1
      }
    }
    END {
      if (!inserted) exit 1
    }
  ' > "$inspiration_tmp"
  stage_blob_path "$inspiration_tmp" INSPIRATION.md
}

stage_feature() {
  if ! git diff --cached --quiet; then
    printf '%s\n' 'Refusing to stage: the index already contains changes.' >&2
    return 1
  fi
  git add -- "${feature_paths[@]}"
  local tmp_dir
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154g-delivery.XXXXXX")"
  trap 'rm -rf "$tmp_dir"' RETURN
  stage_shared_files "$tmp_dir"
  git diff --cached --check
  git diff --cached --stat
}

commit_feature() {
  if git diff --cached --quiet; then
    printf '%s\n' 'Refusing to commit: no staged C154g changes.' >&2
    return 1
  fi
  git commit -m 'feat: add durable rolling schema runner [skip ci]'
}

push_feature() {
  git push origin HEAD
}

case "${1:-status}" in
  status)
    git status --short --branch
    git diff --cached --stat
    ;;
  stage)
    stage_feature
    ;;
  commit)
    commit_feature
    ;;
  push)
    push_feature
    ;;
  deliver)
    stage_feature
    commit_feature
    push_feature
    git status --short --branch
    ;;
  *)
    printf 'usage: %s {status|stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac
