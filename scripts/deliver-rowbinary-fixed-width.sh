#!/bin/sh
set -eu

mode=${1:-inspect}
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

feature_files='ROW_BINARY.md
hat/hatSql/row_binary_fixed_width_test.go
scripts/test-rowbinary-fixed-width.sh
scripts/deliver-rowbinary-fixed-width.sh'

case "$mode" in
inspect)
  printf '%s\n' 'Row-binary fixed-width delivery scope:'
  printf '%s\n' "$feature_files"
  git status --short -- ROW_BINARY.md hat/hatSql/row_binary_fixed_width_test.go scripts/test-rowbinary-fixed-width.sh scripts/deliver-rowbinary-fixed-width.sh
  ;;
commit)
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-rowbinary-fixed-width.XXXXXX")
  index_file=$tmp_dir/index
  inspiration_file=$tmp_dir/INSPIRATION.md
  makefile=$tmp_dir/Makefile
  trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

  GIT_INDEX_FILE=$index_file git read-tree HEAD
  GIT_INDEX_FILE=$index_file git add -- $feature_files

  git show HEAD:INSPIRATION.md | awk '
    $0 == "- [ ] C054 Fixed-width date and datetime encodings." {
      print "- [x] C054 Fixed-width date and datetime encodings - RowBinary stores dates as 4-byte epoch days and datetimes as 8-byte Unix nanoseconds, with explicit round-trip and payload-size verification (see ROW_BINARY.md)."
      found++
      next
    }
    { print }
    END {
      if (found != 1) {
        print "expected exactly one unchecked C054 checklist row" > "/dev/stderr"
        exit 1
      }
    }
  ' > "$inspiration_file"
  inspiration_blob=$(git hash-object -w "$inspiration_file")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

  git show HEAD:Makefile > "$makefile"
  cat >> "$makefile" <<'EOF'

.PHONY: test-rowbinary-fixed-width
test-rowbinary-fixed-width:
	sh ./scripts/test-rowbinary-fixed-width.sh test

.PHONY: race-rowbinary-fixed-width
race-rowbinary-fixed-width:
	sh ./scripts/test-rowbinary-fixed-width.sh race

.PHONY: format-rowbinary-fixed-width
format-rowbinary-fixed-width:
	sh ./scripts/test-rowbinary-fixed-width.sh format

.PHONY: inspect-rowbinary-fixed-width-delivery
inspect-rowbinary-fixed-width-delivery:
	sh ./scripts/deliver-rowbinary-fixed-width.sh inspect

.PHONY: commit-rowbinary-fixed-width
commit-rowbinary-fixed-width:
	sh ./scripts/deliver-rowbinary-fixed-width.sh commit

.PHONY: push-rowbinary-fixed-width
push-rowbinary-fixed-width:
	sh ./scripts/deliver-rowbinary-fixed-width.sh push
EOF
  makefile_blob=$(git hash-object -w "$makefile")
  GIT_INDEX_FILE=$index_file git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

  printf '%s\n' 'Isolated commit contents:'
  GIT_INDEX_FILE=$index_file git diff --cached --name-status
  GIT_INDEX_FILE=$index_file git diff --cached --stat
  GIT_INDEX_FILE=$index_file git commit -m 'test(sql): verify fixed-width row-binary time values'
  ;;
push)
  git push origin master
  ;;
*)
  printf '%s\n' "usage: $0 [inspect|commit|push]" >&2
  exit 2
  ;;
esac
