#!/usr/bin/env bash
set -eu

index_file=$(mktemp)
head_makefile=$(mktemp)
next_makefile=$(mktemp)
cleanup() {
	rm -f "$index_file" "$head_makefile" "$next_makefile"
}
trap cleanup EXIT
rm -f "$index_file"

GIT_INDEX_FILE="$index_file" git read-tree HEAD
git show HEAD:Makefile >"$head_makefile"
awk '
{
	print
}
END {
	print ".PHONY: commit-c024-audit"
	print "commit-c024-audit:"
	print "\tbash ./scripts/commit-c024-audit.sh"
}' "$head_makefile" >"$next_makefile"

GIT_INDEX_FILE="$index_file" git add -- INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md scripts/commit-c024-audit.sh
makefile_blob=$(git hash-object -w "$next_makefile")
GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo 100644,"$makefile_blob",Makefile

expected='ADOPTED_QUERY_ENGINE_IDEAS.md
INSPIRATION.md
Makefile
scripts/commit-c024-audit.sh'
actual=$(GIT_INDEX_FILE="$index_file" git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
	printf '%s\n' 'unexpected C024 audit staging:' >&2
	printf '%s\n' "$actual" >&2
	exit 1
fi

GIT_INDEX_FILE="$index_file" git diff --cached --check
GIT_INDEX_FILE="$index_file" git commit -m 'docs: record existing refreshable materialized views'
git push
