#!/bin/sh
set -eu

mode=${1:-inspect}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

feature_files='SCHEDULER_MONITORING.md
hat/hatMonitoring/scheduler.go
hat/hatMonitoring/scheduler_test.go
hat/hatCache/monitoring.go
hat/hatCache/monitoring_scheduler_test.go
scripts/test-runtime-introspection.sh
scripts/deliver-runtime-introspection.sh'

case "$mode" in
inspect)
  printf '%s\n' 'Runtime introspection delivery scope:'
  printf '%s\n' "$feature_files"
  printf '%s\n' 'Current status for feature files:'
  git status --short -- SCHEDULER_MONITORING.md hat/hatMonitoring/scheduler.go hat/hatMonitoring/scheduler_test.go hat/hatCache/monitoring.go hat/hatCache/monitoring_scheduler_test.go scripts/test-runtime-introspection.sh scripts/deliver-runtime-introspection.sh
  printf '%s\n' 'Current diff summary for feature files:'
  git diff --stat -- SCHEDULER_MONITORING.md hat/hatMonitoring/scheduler.go hat/hatMonitoring/scheduler_test.go hat/hatCache/monitoring.go hat/hatCache/monitoring_scheduler_test.go scripts/test-runtime-introspection.sh scripts/deliver-runtime-introspection.sh
  ;;
commit)
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-runtime-introspection.XXXXXX")
  index_file=$tmp_dir/index
  inspiration_file=$tmp_dir/INSPIRATION.md
  makefile=$tmp_dir/Makefile
  trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

  GIT_INDEX_FILE=$index_file git read-tree HEAD
  GIT_INDEX_FILE=$index_file git add -- $feature_files

  git show HEAD:INSPIRATION.md | awk '
    $0 == "- [ ] T136 Fiber and scheduler introspection." {
      print "- [x] T136 Fiber and scheduler introspection - Add an authenticated `/api/scheduler` report and Prometheus gauges for goroutine, GOMAXPROCS, CPU, and scheduler metric state; the on-demand report is zero-allocation in the package benchmark (see SCHEDULER_MONITORING.md)."
      found++
      next
    }
    { print }
    END {
      if (found != 1) {
        print "expected exactly one unchecked T136 checklist row" > "/dev/stderr"
        exit 1
      }
    }
  ' > "$inspiration_file"
  inspiration_blob=$(git hash-object -w "$inspiration_file")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

  git show HEAD:Makefile > "$makefile"
  cat >> "$makefile" <<'EOF'

.PHONY: test-runtime-introspection
test-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh test

.PHONY: http-runtime-introspection
http-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh http

.PHONY: race-http-runtime-introspection
race-http-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh race-http

.PHONY: race-runtime-introspection
race-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh race

.PHONY: benchmark-runtime-introspection
benchmark-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh bench

.PHONY: format-runtime-introspection
format-runtime-introspection:
	sh ./scripts/test-runtime-introspection.sh format

.PHONY: inspect-runtime-introspection-delivery
inspect-runtime-introspection-delivery:
	sh ./scripts/deliver-runtime-introspection.sh inspect

.PHONY: commit-runtime-introspection
commit-runtime-introspection:
	sh ./scripts/deliver-runtime-introspection.sh commit

.PHONY: push-runtime-introspection
push-runtime-introspection:
	sh ./scripts/deliver-runtime-introspection.sh push
EOF
  makefile_blob=$(git hash-object -w "$makefile")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

  printf '%s\n' 'Isolated commit contents:'
  GIT_INDEX_FILE=$index_file git diff --cached --name-status
  GIT_INDEX_FILE=$index_file git diff --cached --stat
  GIT_INDEX_FILE=$index_file git commit -m 'feat(monitoring): expose Go scheduler runtime state'
  ;;
push)
  git push origin master
  ;;
*)
  printf '%s\n' "usage: $0 [inspect|commit|push]" >&2
  exit 2
  ;;
esac
