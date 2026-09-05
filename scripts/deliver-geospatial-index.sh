#!/bin/sh
set -eu

mode=${1:-inspect}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

feature_files='SPATIAL_INDEX.md
hat/hatSql/geospatial.go
hat/hatSql/geospatial_optimization_test.go
scripts/test-geospatial-index.sh
scripts/deliver-geospatial-index.sh'

case "$mode" in
inspect)
  printf '%s\n' 'Geospatial delivery scope:'
  printf '%s\n' "$feature_files"
  git status --short -- SPATIAL_INDEX.md hat/hatSql/geospatial.go hat/hatSql/geospatial_optimization_test.go scripts/test-geospatial-index.sh scripts/deliver-geospatial-index.sh
  git diff --stat -- hat/hatSql/geospatial.go
  ;;
commit)
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-geospatial.XXXXXX")
  index_file=$tmp_dir/index
  inspiration_file=$tmp_dir/INSPIRATION.md
  makefile=$tmp_dir/Makefile
  trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

  GIT_INDEX_FILE=$index_file git read-tree HEAD
  GIT_INDEX_FILE=$index_file git add -- $feature_files

  git show HEAD:INSPIRATION.md | awk '
    $0 == "- [ ] T008 RTREE spatial index." {
      print "- [ ] T008 RTREE spatial index."
      print "- [x] T008a Adaptive sparse-grid spatial candidate enumeration - Keep grid-cell memory overhead while avoiding empty-cell scans and per-query candidate deduplication (see SPATIAL_INDEX.md)."
      found++
      next
    }
    { print }
    END {
      if (found != 1) {
        print "expected exactly one unchecked T008 checklist row" > "/dev/stderr"
        exit 1
      }
    }
  ' > "$inspiration_file"
  inspiration_blob=$(git hash-object -w "$inspiration_file")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

  git show HEAD:Makefile > "$makefile"
  cat >> "$makefile" <<'EOF'

.PHONY: test-geospatial-index
test-geospatial-index:
	sh ./scripts/test-geospatial-index.sh test

.PHONY: test-geospatial-all
test-geospatial-all:
	sh ./scripts/test-geospatial-index.sh all

.PHONY: race-geospatial-index
race-geospatial-index:
	sh ./scripts/test-geospatial-index.sh race

.PHONY: benchmark-geospatial-index
benchmark-geospatial-index:
	sh ./scripts/test-geospatial-index.sh bench

.PHONY: format-geospatial-index
format-geospatial-index:
	sh ./scripts/test-geospatial-index.sh format

.PHONY: inspect-geospatial-delivery
inspect-geospatial-delivery:
	sh ./scripts/deliver-geospatial-index.sh inspect

.PHONY: commit-geospatial-index
commit-geospatial-index:
	sh ./scripts/deliver-geospatial-index.sh commit

.PHONY: push-geospatial-index
push-geospatial-index:
	sh ./scripts/deliver-geospatial-index.sh push
EOF
  makefile_blob=$(git hash-object -w "$makefile")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

  printf '%s\n' 'Isolated commit contents:'
  GIT_INDEX_FILE=$index_file git diff --cached --name-status
  GIT_INDEX_FILE=$index_file git diff --cached --stat
  GIT_INDEX_FILE=$index_file git commit -m 'perf(sql): optimize sparse geospatial queries'
  ;;
push)
  git push origin master
  ;;
*)
  printf '%s\n' "usage: $0 [inspect|commit|push]" >&2
  exit 2
  ;;
esac
