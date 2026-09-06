#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

mode=${1:-preview}
commit_message='feat(sql): add aggregate state combinators'
child='- [x] C077a Reusable aggregate state, merge, and finalize combinator registry.'

if [ "$mode" = push ]; then
	git push
	git rev-parse HEAD
	exit 0
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-aggregate-combinator.XXXXXX")
index="$work/index"
trap 'rm -rf -- "$work"' EXIT

base=$(git rev-parse HEAD)
git show "$base:Makefile" > "$work/Makefile"
git show "$base:INSPIRATION.md" > "$work/INSPIRATION.md"

checklist_existing=0
if awk '/^- \[x\] C077a / { found=1 } END { exit !found }' "$work/INSPIRATION.md"; then
	checklist_existing=1
else
	awk -v child="$child" '
	BEGIN { added=0 }
	{
		print
		if (!added && $0 ~ /^- \[ \] C077 /) {
			print child
			added=1
		}
	}
	END {
		if (!added) {
			print "C077 parent checklist row was not found" > "/dev/stderr"
			exit 1
		}
	}' "$work/INSPIRATION.md" > "$work/INSPIRATION.next"
	mv "$work/INSPIRATION.next" "$work/INSPIRATION.md"
fi

if [ "$checklist_existing" -eq 1 ]; then
	commit_message='fix(sql): make aggregate delivery idempotent'
fi

if ! awk '/^# aggregate-combinator-targets$/ { found=1 } END { exit found }' "$work/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "# aggregate-combinator-targets"
		print "format-aggregate-combinator:"
		print "\tbash ./scripts/format-aggregate-combinator.sh"
		print ""
		print "test-aggregate-combinator:"
		print "\tbash ./scripts/test-aggregate-combinator.sh"
		print ""
		print "test-race-aggregate-combinator:"
		print "\tbash ./scripts/test-race-aggregate-combinator.sh"
		print ""
		print "benchmark-aggregate-combinator:"
		print "\tbash ./scripts/benchmark-aggregate-combinator.sh"
		print ""
		print "deliver-aggregate-combinator:"
		print "\tbash ./scripts/deliver-aggregate-combinator.sh preview"
		print ""
		print "commit-aggregate-combinator:"
		print "\tbash ./scripts/deliver-aggregate-combinator.sh commit"
		print ""
		print "push-aggregate-combinator:"
		print "\tbash ./scripts/deliver-aggregate-combinator.sh push"
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
	AGGREGATE_COMBINATORS.md \
	hat/hatSql/aggregate_combinator.go \
	hat/hatSql/aggregate_combinator_test.go \
	scripts/benchmark-aggregate-combinator.sh \
	scripts/deliver-aggregate-combinator.sh \
	scripts/format-aggregate-combinator.sh \
	scripts/test-aggregate-combinator.sh \
	scripts/test-race-aggregate-combinator.sh; do
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
