#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

mode=${1:-preview}
commit_message='feat(predicate): add reusable batch predicate masks'
child='- [x] C015a Allocation-free batch predicate masks for numeric and string filters.'

if [ "$mode" = push ]; then
	git push
	git rev-parse HEAD
	exit 0
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-simd-predicates.XXXXXX")
index="$work/index"
trap 'rm -rf -- "$work"' EXIT

base=$(git rev-parse HEAD)
git show "$base:Makefile" > "$work/Makefile"
git show "$base:INSPIRATION.md" > "$work/INSPIRATION.md"

checklist_existing=0
if awk '/^- \[x\] C015a / { found=1 } END { exit !found }' "$work/INSPIRATION.md"; then
	checklist_existing=1
else
	awk -v child="$child" '
	BEGIN { added=0 }
	{
		print
		if (!added && $0 ~ /^- \[ \] C015 /) {
			print child
			added=1
		}
	}
	END {
		if (!added) {
			print "C015 parent checklist row was not found" > "/dev/stderr"
			exit 1
		}
	}' "$work/INSPIRATION.md" > "$work/INSPIRATION.next"
	mv "$work/INSPIRATION.next" "$work/INSPIRATION.md"
fi

if [ "$checklist_existing" -eq 1 ]; then
	commit_message='fix(predicate): make SIMD delivery idempotent'
fi

if ! awk '/^# simd-predicate-targets$/ { found=1 } END { exit found }' "$work/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "# simd-predicate-targets"
		print "format-simd-predicates:"
		print "\tbash ./scripts/format-simd-predicates.sh"
		print ""
		print "test-simd-predicates:"
		print "\tbash ./scripts/test-simd-predicates.sh"
		print ""
		print "test-race-simd-predicates:"
		print "\tbash ./scripts/test-race-simd-predicates.sh"
		print ""
		print "benchmark-simd-predicates:"
		print "\tbash ./scripts/benchmark-simd-predicates.sh"
		print ""
		print "deliver-simd-predicates:"
		print "\tbash ./scripts/deliver-simd-predicates.sh preview"
		print ""
		print "commit-simd-predicates:"
		print "\tbash ./scripts/deliver-simd-predicates.sh commit"
		print ""
		print "push-simd-predicates:"
		print "\tbash ./scripts/deliver-simd-predicates.sh push"
	}
	' "$work/Makefile" > "$work/Makefile.next"
	mv "$work/Makefile.next" "$work/Makefile"
fi

rm -f "$index"
GIT_INDEX_FILE="$index" git read-tree "$base"

make_blob=$(git hash-object -w "$work/Makefile")
inspiration_blob=$(git hash-object -w "$work/INSPIRATION.md")
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$make_blob,Makefile"
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

for path in \
	SIMD_PREDICATES.md \
	hat/hatPredicate/mask.go \
	hat/hatPredicate/mask_test.go \
	scripts/benchmark-simd-predicates.sh \
	scripts/deliver-simd-predicates.sh \
	scripts/format-simd-predicates.sh \
	scripts/test-race-simd-predicates.sh \
	scripts/test-simd-predicates.sh; do
	if [ ! -f "$path" ]; then
		printf 'missing delivery file: %s\n' "$path" >&2
		exit 1
	fi
	GIT_INDEX_FILE="$index" git add -- "$path"
done

case "$mode" in
preview)
	printf '%s\n' "base: $base"
	GIT_INDEX_FILE="$index" git diff --cached --name-status
	GIT_INDEX_FILE="$index" git diff --cached --stat
	;;
commit)
	GIT_INDEX_FILE="$index" git diff --cached --check
	GIT_INDEX_FILE="$index" git commit -m "$commit_message"
	git rev-parse HEAD
	;;
*)
	printf 'usage: %s [preview|commit|push]\n' "$0" >&2
	exit 2
	;;
esac
