#!/bin/sh
set -eu

mode=${1:-inspect}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

feature_files='QUERY_QUOTA.md
hat/hatSql/governance.go
hat/hatSql/governance_quota_test.go
scripts/test-query-quota.sh
scripts/deliver-query-quota.sh'

case "$mode" in
inspect)
  printf '%s\n' 'Query-quota delivery scope:'
  printf '%s\n' "$feature_files"
  git status --short -- QUERY_QUOTA.md hat/hatSql/governance.go hat/hatSql/governance_quota_test.go scripts/test-query-quota.sh scripts/deliver-query-quota.sh
  printf '%s\n' 'Worktree versus HEAD:'
  git diff HEAD --stat -- QUERY_QUOTA.md hat/hatSql/governance.go hat/hatSql/governance_quota_test.go scripts/test-query-quota.sh scripts/deliver-query-quota.sh
  git diff HEAD -- hat/hatSql/governance.go
  ;;
commit)
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-query-quota.XXXXXX")
  index_file=$tmp_dir/index
  inspiration_file=$tmp_dir/INSPIRATION.md
  makefile=$tmp_dir/Makefile
  trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

  GIT_INDEX_FILE=$index_file git read-tree HEAD
  GIT_INDEX_FILE=$index_file git add -- $feature_files

  git show HEAD:INSPIRATION.md | awk '
    $0 == "- [ ] C100 Query quotas by user, tenant, or source." {
      print "- [x] C100 Query quotas by user, tenant, or source - NamespaceQueryGovernor supports per-namespace fixed-window request quotas with default-off behavior and per-namespace tightening (see QUERY_GOVERNANCE.md)."
      found++
      next
    }
    { print }
    END {
      if (found != 1) {
        print "expected exactly one unchecked C100 checklist row" > "/dev/stderr"
        exit 1
      }
    }
  ' > "$inspiration_file"
  inspiration_blob=$(git hash-object -w "$inspiration_file")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

  git show HEAD:Makefile > "$makefile"
  cat >> "$makefile" <<'EOF'

.PHONY: test-query-quota
test-query-quota:
	sh ./scripts/test-query-quota.sh test

.PHONY: race-query-quota
race-query-quota:
	sh ./scripts/test-query-quota.sh race

.PHONY: benchmark-query-quota
benchmark-query-quota:
	sh ./scripts/test-query-quota.sh bench

.PHONY: format-query-quota
format-query-quota:
	sh ./scripts/test-query-quota.sh format

.PHONY: inspect-query-quota-delivery
inspect-query-quota-delivery:
	sh ./scripts/deliver-query-quota.sh inspect

.PHONY: commit-query-quota
commit-query-quota:
	sh ./scripts/deliver-query-quota.sh commit

.PHONY: push-query-quota
push-query-quota:
	sh ./scripts/deliver-query-quota.sh push
EOF
  makefile_blob=$(git hash-object -w "$makefile")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

  printf '%s\n' 'Isolated commit contents:'
  GIT_INDEX_FILE=$index_file git diff --cached --name-status
  GIT_INDEX_FILE=$index_file git diff --cached --stat
  GIT_INDEX_FILE=$index_file git commit -m 'feat(sql): add namespace query quotas'
  ;;
push)
  git push origin master
  ;;
*)
  printf '%s\n' "usage: $0 [inspect|commit|push]" >&2
  exit 2
  ;;
esac
