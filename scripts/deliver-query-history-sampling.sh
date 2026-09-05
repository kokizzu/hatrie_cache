#!/bin/sh
set -eu

mode=${1:-inspect}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

feature_files='QUERY_HISTORY.md
hat/hatSql/query_manager.go
hat/hatSql/query_manager_history_sampling_test.go
scripts/test-query-history-sampling.sh
scripts/deliver-query-history-sampling.sh'

case "$mode" in
inspect)
  printf '%s\n' 'Query-history sampling delivery scope:'
  printf '%s\n' "$feature_files"
  git status --short -- QUERY_HISTORY.md hat/hatSql/query_manager.go hat/hatSql/query_manager_history_sampling_test.go scripts/test-query-history-sampling.sh scripts/deliver-query-history-sampling.sh
  printf '%s\n' 'Worktree versus HEAD:'
  git diff HEAD --stat -- QUERY_HISTORY.md hat/hatSql/query_manager.go hat/hatSql/query_manager_history_sampling_test.go scripts/test-query-history-sampling.sh scripts/deliver-query-history-sampling.sh
  git diff HEAD -- hat/hatSql/query_manager.go
  ;;
commit)
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-query-history.XXXXXX")
  index_file=$tmp_dir/index
  inspiration_file=$tmp_dir/INSPIRATION.md
  makefile=$tmp_dir/Makefile
  trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

  GIT_INDEX_FILE=$index_file git read-tree HEAD
  GIT_INDEX_FILE=$index_file git add -- $feature_files

  git show HEAD:INSPIRATION.md | awk '
    $0 == "- [ ] C133 Query log retention and sampling policy." {
      print "- [x] C133 Query log retention and sampling policy - SQLQueryManager provides bounded privacy-safe history with deterministic configurable completion sampling and no SQL-text retention (see QUERY_HISTORY.md)."
      found++
      next
    }
    { print }
    END {
      if (found != 1) {
        print "expected exactly one unchecked C133 checklist row" > "/dev/stderr"
        exit 1
      }
    }
  ' > "$inspiration_file"
  inspiration_blob=$(git hash-object -w "$inspiration_file")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

  git show HEAD:Makefile > "$makefile"
  cat >> "$makefile" <<'EOF'

.PHONY: test-query-history-sampling
test-query-history-sampling:
	sh ./scripts/test-query-history-sampling.sh test

.PHONY: race-query-history-sampling
race-query-history-sampling:
	sh ./scripts/test-query-history-sampling.sh race

.PHONY: format-query-history-sampling
format-query-history-sampling:
	sh ./scripts/test-query-history-sampling.sh format

.PHONY: inspect-query-history-sampling-delivery
inspect-query-history-sampling-delivery:
	sh ./scripts/deliver-query-history-sampling.sh inspect

.PHONY: commit-query-history-sampling
commit-query-history-sampling:
	sh ./scripts/deliver-query-history-sampling.sh commit

.PHONY: push-query-history-sampling
push-query-history-sampling:
	sh ./scripts/deliver-query-history-sampling.sh push
EOF
  makefile_blob=$(git hash-object -w "$makefile")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

  printf '%s\n' 'Isolated commit contents:'
  GIT_INDEX_FILE=$index_file git diff --cached --name-status
  GIT_INDEX_FILE=$index_file git diff --cached --stat
  GIT_INDEX_FILE=$index_file git commit -m 'feat(sql): add deterministic query history sampling'
  ;;
push)
  git push origin master
  ;;
*)
  printf '%s\n' "usage: $0 [inspect|commit|push]" >&2
  exit 2
  ;;
esac
