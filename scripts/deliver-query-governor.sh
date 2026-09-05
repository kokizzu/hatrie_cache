#!/bin/sh
set -eu

mode=${1:-inspect}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

feature_files='QUERY_GOVERNANCE.md
hat/hatSql/governance.go
hat/hatSql/governance_queue_test.go
scripts/test-query-governor.sh
scripts/deliver-query-governor.sh'

case "$mode" in
inspect)
  printf '%s\n' 'Query-governor delivery scope:'
  printf '%s\n' "$feature_files"
  git status --short -- QUERY_GOVERNANCE.md hat/hatSql/governance.go hat/hatSql/governance_queue_test.go scripts/test-query-governor.sh scripts/deliver-query-governor.sh
  git diff --stat -- hat/hatSql/governance.go
  ;;
commit)
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-query-governor.XXXXXX")
  index_file=$tmp_dir/index
  inspiration_file=$tmp_dir/INSPIRATION.md
  makefile=$tmp_dir/Makefile
  trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

  GIT_INDEX_FILE=$index_file git read-tree HEAD
  GIT_INDEX_FILE=$index_file git add -- $feature_files

  git show HEAD:INSPIRATION.md | awk '
    $0 == "- [ ] C107 Admission control before expensive scans." {
      print "- [ ] C107 Admission control before expensive scans."
      print "- [x] C107a Bounded query-admission queues - Cap namespace waiters before allocation while preserving the default unlimited behavior (see QUERY_GOVERNANCE.md)."
      found++
      next
    }
    { print }
    END {
      if (found != 1) {
        print "expected exactly one unchecked C107 checklist row" > "/dev/stderr"
        exit 1
      }
    }
  ' > "$inspiration_file"
  inspiration_blob=$(git hash-object -w "$inspiration_file")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

  git show HEAD:Makefile > "$makefile"
  cat >> "$makefile" <<'EOF'

.PHONY: test-query-governor
test-query-governor:
	sh ./scripts/test-query-governor.sh test

.PHONY: race-query-governor
race-query-governor:
	sh ./scripts/test-query-governor.sh race

.PHONY: format-query-governor
format-query-governor:
	sh ./scripts/test-query-governor.sh format

.PHONY: benchmark-query-governor
benchmark-query-governor:
	sh ./scripts/test-query-governor.sh bench

.PHONY: inspect-query-governor-delivery
inspect-query-governor-delivery:
	sh ./scripts/deliver-query-governor.sh inspect

.PHONY: commit-query-governor
commit-query-governor:
	sh ./scripts/deliver-query-governor.sh commit

.PHONY: push-query-governor
push-query-governor:
	sh ./scripts/deliver-query-governor.sh push
EOF
  makefile_blob=$(git hash-object -w "$makefile")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

  printf '%s\n' 'Isolated commit contents:'
  GIT_INDEX_FILE=$index_file git diff --cached --name-status
  GIT_INDEX_FILE=$index_file git diff --cached --stat
  GIT_INDEX_FILE=$index_file git commit -m 'feat(sql): bound namespace query waiters'
  ;;
push)
  git push origin master
  ;;
*)
  printf '%s\n' "usage: $0 [inspect|commit|push]" >&2
  exit 2
  ;;
esac
