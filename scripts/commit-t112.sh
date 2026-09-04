#!/usr/bin/env bash
set -euo pipefail

mode=${1:-stage}
commit_message='ops: add replication queue metrics'
feature_paths=(
  BENCHMARK.md
  INSPIRATION.md
  README.md
  REPLICATION_OPERATIONS.md
  hat/hatCache/monitoring.go
  hat/hatCache/replication.go
  hat/hatCache/replication_test.go
  hat/hatReplication/metrics.go
  hat/hatReplication/metrics_test.go
  hat/hatReplication/model.go
  scripts/benchmark-t112.sh
  scripts/commit-t112.sh
  scripts/format-t112.sh
  scripts/review-t112.sh
  scripts/test-race-t112.sh
  scripts/test-t112.sh
  scripts/vet-t112.sh
)
makefile_block=$'test-t112:\n\tbash ./scripts/test-t112.sh\n\nformat-t112:\n\tbash ./scripts/format-t112.sh\n\nbenchmark-t112:\n\tbash ./scripts/benchmark-t112.sh\n\ntest-race-t112:\n\tbash ./scripts/test-race-t112.sh\n\nvet-t112:\n\tbash ./scripts/vet-t112.sh\n\nreview-t112:\n\tbash ./scripts/review-t112.sh\n\nstage-t112:\n\tbash ./scripts/commit-t112.sh stage\n\ncommit-t112:\n\tbash ./scripts/commit-t112.sh commit\n\npush-t112:\n\tbash ./scripts/commit-t112.sh push'
old_makefile_block=$'test-t112:\n\tbash ./scripts/test-t112.sh\n\nformat-t112:\n\tbash ./scripts/format-t112.sh\n\nbenchmark-t112:\n\tbash ./scripts/benchmark-t112.sh\n\ntest-race-t112:\n\tbash ./scripts/test-race-t112.sh\n\nvet-t112:\n\tbash ./scripts/vet-t112.sh\n\nreview-t112:\n\tbash ./scripts/review-t112.sh'

stage_feature() {
  base=$(mktemp)
  desired=$(mktemp)
  old_desired=$(mktemp)
  raw_patch=$(mktemp)
  patch_file=$(mktemp)
  staged=$(mktemp)
  cleanup() {
    rm -f "$base" "$desired" "$old_desired" "$raw_patch" "$patch_file" "$staged"
  }
  trap cleanup RETURN

  git show HEAD:Makefile > "$base"
  cp "$base" "$desired"
  printf '\n%s\n' "$makefile_block" >> "$desired"
  cp "$base" "$old_desired"
  printf '\n%s\n' "$old_makefile_block" >> "$old_desired"

  makefile_ready=false
  if ! git diff --cached --quiet -- Makefile; then
    git show :Makefile > "$staged"
    if cmp -s "$staged" "$desired"; then
      makefile_ready=true
    elif cmp -s "$staged" "$old_desired"; then
      git restore --staged -- Makefile
    else
      printf '%s\n' 'refusing to rewrite an unrelated staged Makefile' >&2
      exit 1
    fi
  fi
  git add -- "${feature_paths[@]}"
  if [ "$makefile_ready" = true ]; then
    git show :Makefile > "$staged"
  fi
  if [ "$makefile_ready" != true ]; then
    if git diff --no-index "$base" "$desired" > "$raw_patch"; then
      :
    else
      status=$?
      if [ "$status" -ne 1 ]; then
        exit "$status"
      fi
    fi
    base_name=${base#/}
    desired_name=${desired#/}
    sed \
      -e "s|a/$base_name|a/Makefile|g" \
      -e "s|b/$desired_name|b/Makefile|g" \
      -e "s|--- $base|--- a/Makefile|g" \
      -e "s|+++ $desired|+++ b/Makefile|g" \
      "$raw_patch" > "$patch_file"
    git apply --cached --whitespace=nowarn "$patch_file"
  fi
  git show :Makefile > "$staged"
  if ! cmp -s "$staged" "$desired"; then
    printf '%s\n' 'staged Makefile does not contain only the T112 target block' >&2
    exit 1
  fi

  allowed=(Makefile "${feature_paths[@]}")
  while IFS= read -r path; do
    permitted=false
    for candidate in "${allowed[@]}"; do
      if [ "$path" = "$candidate" ]; then
        permitted=true
        break
      fi
    done
    if [ "$permitted" != true ]; then
      printf 'refusing unrelated staged path: %s\n' "$path" >&2
      exit 1
    fi
  done < <(git diff --cached --name-only)
}

case "$mode" in
stage)
  stage_feature
  ;;
commit)
  if git diff --cached --quiet; then
    stage_feature
  fi
  git commit -m "$commit_message"
  ;;
push)
  git push origin HEAD
  ;;
*)
  printf 'usage: %s {stage|commit|push}\n' "$0" >&2
  exit 2
  ;;
esac
