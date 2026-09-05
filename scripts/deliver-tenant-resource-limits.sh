#!/bin/sh
set -eu

mode=${1:-inspect}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

feature_files='TENANT_RESOURCE_LIMITS.md
hat/hatSql/governance_tenant_test.go
scripts/test-tenant-resource-limits.sh
scripts/deliver-tenant-resource-limits.sh'

case "$mode" in
inspect)
  printf '%s\n' 'Tenant resource-limit delivery scope:'
  printf '%s\n' "$feature_files"
  git status --short -- TENANT_RESOURCE_LIMITS.md hat/hatSql/governance_tenant_test.go scripts/test-tenant-resource-limits.sh scripts/deliver-tenant-resource-limits.sh
  ;;
commit)
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tenant-limits.XXXXXX")
  index_file=$tmp_dir/index
  inspiration_file=$tmp_dir/INSPIRATION.md
  makefile=$tmp_dir/Makefile
  trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

  GIT_INDEX_FILE=$index_file git read-tree HEAD
  GIT_INDEX_FILE=$index_file git add -- $feature_files

  git show HEAD:INSPIRATION.md | awk '
    $0 == "- [ ] C136 Per-tenant resource quotas." {
      print "- [x] C136 Per-tenant resource quotas - NamespaceQueryGovernor applies immutable, tightening per-namespace resource policies suitable for tenant, user, or source isolation (see TENANT_RESOURCE_LIMITS.md)."
      found++
      next
    }
    { print }
    END {
      if (found != 1) {
        print "expected exactly one unchecked C136 checklist row" > "/dev/stderr"
        exit 1
      }
    }
  ' > "$inspiration_file"
  inspiration_blob=$(git hash-object -w "$inspiration_file")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

  git show HEAD:Makefile > "$makefile"
  cat >> "$makefile" <<'EOF'

.PHONY: test-tenant-resource-limits
test-tenant-resource-limits:
	sh ./scripts/test-tenant-resource-limits.sh test

.PHONY: race-tenant-resource-limits
race-tenant-resource-limits:
	sh ./scripts/test-tenant-resource-limits.sh race

.PHONY: format-tenant-resource-limits
format-tenant-resource-limits:
	sh ./scripts/test-tenant-resource-limits.sh format

.PHONY: inspect-tenant-resource-limits-delivery
inspect-tenant-resource-limits-delivery:
	sh ./scripts/deliver-tenant-resource-limits.sh inspect

.PHONY: commit-tenant-resource-limits
commit-tenant-resource-limits:
	sh ./scripts/deliver-tenant-resource-limits.sh commit

.PHONY: push-tenant-resource-limits
push-tenant-resource-limits:
	sh ./scripts/deliver-tenant-resource-limits.sh push
EOF
  makefile_blob=$(git hash-object -w "$makefile")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

  printf '%s\n' 'Isolated commit contents:'
  GIT_INDEX_FILE=$index_file git diff --cached --name-status
  GIT_INDEX_FILE=$index_file git diff --cached --stat
  GIT_INDEX_FILE=$index_file git commit -m 'test(sql): verify tenant resource policies'
  ;;
push)
  git push origin master
  ;;
*)
  printf '%s\n' "usage: $0 [inspect|commit|push]" >&2
  exit 2
  ;;
esac
