#!/usr/bin/env bash
set -euo pipefail

mode="${1:-stage}"
feature_paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION.md
  README.md
  REPLICATION_OPERATIONS.md
  hat/hatCache/monitoring.go
  hat/hatCache/replication.go
  hat/hatCache/replication_benchmark_test.go
  hat/hatCache/replication_test.go
  hat/hatReplication/model.go
  scripts/benchmark-t063.sh
  scripts/commit-t063.sh
  scripts/format-t063.sh
  scripts/review-t063.sh
  scripts/test-race-t063.sh
  scripts/test-t063.sh
  scripts/vet-t063.sh
)

verify_staged() {
  staged=$(git diff --cached --name-only)
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    allowed=0
    for expected in "${feature_paths[@]}" Makefile; do
      if [[ "$path" == "$expected" ]]; then
        allowed=1
        break
      fi
    done
    if [[ "$allowed" -eq 0 ]]; then
      printf '%s\n' "refusing to commit unrelated staged path: $path" >&2
      exit 1
    fi
  done <<< "$staged"
}

stage_feature() {
  if ! git diff --cached --quiet; then
    verify_staged
  fi

  base=$(mktemp)
  desired=$(mktemp)
  patch=$(mktemp)
  staged_makefile=$(mktemp)
  trap 'rm -f "$base" "$desired" "$patch" "$staged_makefile" "$block"' RETURN
  git show HEAD:Makefile > "$base"
  block=$(mktemp)
  printf '%s\n' \
    '.PHONY: test-t063' \
    'test-t063:' \
    $'\tbash ./scripts/test-t063.sh' \
    '' \
    '.PHONY: format-t063' \
    'format-t063:' \
    $'\tbash ./scripts/format-t063.sh' \
    '' \
    '.PHONY: test-race-t063' \
    'test-race-t063:' \
    $'\tbash ./scripts/test-race-t063.sh' \
    '' \
    '.PHONY: vet-t063' \
    'vet-t063:' \
    $'\tbash ./scripts/vet-t063.sh' \
    '' \
    '.PHONY: benchmark-t063' \
    'benchmark-t063:' \
    $'\tbash ./scripts/benchmark-t063.sh' \
    '' \
    '.PHONY: review-t063' \
    'review-t063:' \
    $'\tbash ./scripts/review-t063.sh' \
    '' \
    '.PHONY: stage-t063' \
    'stage-t063:' \
    $'\tbash ./scripts/commit-t063.sh stage' \
    '' \
    '.PHONY: commit-t063' \
    'commit-t063:' \
    $'\tbash ./scripts/commit-t063.sh commit' \
    '' \
    '.PHONY: push-t063' \
    'push-t063:' \
    $'\tbash ./scripts/commit-t063.sh push' \
    > "$block"
  awk -v block="$block" '
    {
      print
      if ($0 == "\tbash ./scripts/review-t062.sh") {
        while ((getline line < block) > 0) {
          print line
        }
        close(block)
      }
    }
  ' "$base" > "$desired"
  set +e
  git diff --no-index -- "$base" "$desired" > "$patch"
  diff_status=$?
  set -e
  if [[ "$diff_status" -ne 1 ]]; then
    printf '%s\n' "unexpected Makefile patch generation status: $diff_status" >&2
    exit 1
  fi
  sed -i "s|a${base}|a/Makefile|g; s|b${desired}|b/Makefile|g" "$patch"
  git add -- "${feature_paths[@]}"
  if git diff --cached --quiet -- Makefile; then
    git apply --cached "$patch"
  else
    git show :Makefile > "$staged_makefile"
    if ! cmp -s "$staged_makefile" "$desired"; then
      printf '%s\n' 'refusing to refresh T063 over a different staged Makefile' >&2
      exit 1
    fi
  fi
  verify_staged
  git diff --cached --check
  printf '%s\n' '--- staged T063 paths ---'
  git diff --cached --name-status
  printf '%s\n' '--- staged T063 summary ---'
  git diff --cached --stat
}

case "$mode" in
stage)
  stage_feature
  ;;
commit)
  stage_feature
  git commit -m 'ops: add replication pause controls'
  ;;
push)
  git push origin HEAD
  ;;
*)
  printf '%s\n' "usage: $0 {stage|commit|push}" >&2
  exit 2
  ;;
esac
