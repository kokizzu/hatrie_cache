#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

mode=${1:-preview}
commit_message='feat(sql): add ordered time-series gap filling'
child='- [x] C081a Ordered time-series gap filling with explicit half-open bounds.'

if [ "$mode" = push ]; then
	git push
	git rev-parse HEAD
	exit 0
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-with-fill.XXXXXX")
index="$work/index"
trap 'rm -rf -- "$work"' EXIT

base=$(git rev-parse HEAD)
git show "$base:Makefile" > "$work/Makefile"
git show "$base:INSPIRATION.md" > "$work/INSPIRATION.md"

checklist_existing=0
if awk '/^- \[x\] C081a / { found=1 } END { exit !found }' "$work/INSPIRATION.md"; then
	checklist_existing=1
else
	awk -v child="$child" '
	BEGIN { added=0 }
	{
		print
		if (!added && $0 ~ /^- \[ \] C081 /) {
			print child
			added=1
		}
	}
	END {
		if (!added) {
			print "C081 parent checklist row was not found" > "/dev/stderr"
			exit 1
		}
	}' "$work/INSPIRATION.md" > "$work/INSPIRATION.next"
	mv "$work/INSPIRATION.next" "$work/INSPIRATION.md"
fi

if [ "$checklist_existing" -eq 1 ]; then
	commit_message='fix(sql): make WITH FILL delivery idempotent'
fi

if ! awk '/^# with-fill-targets$/ { found=1 } END { exit found }' "$work/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "# with-fill-targets"
		print "format-with-fill:"
		print "\tbash ./scripts/format-with-fill.sh"
		print ""
		print "test-with-fill:"
		print "\tbash ./scripts/test-with-fill.sh"
		print ""
		print "test-race-with-fill:"
		print "\tbash ./scripts/test-race-with-fill.sh"
		print ""
		print "benchmark-with-fill:"
		print "\tbash ./scripts/benchmark-with-fill.sh"
		print ""
		print "deliver-with-fill:"
		print "\tbash ./scripts/deliver-with-fill.sh preview"
		print ""
		print "commit-with-fill:"
		print "\tbash ./scripts/deliver-with-fill.sh commit"
		print ""
		print "push-with-fill:"
		print "\tbash ./scripts/deliver-with-fill.sh push"
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
	WITH_FILL.md \
	hat/hatSql/with_fill.go \
	hat/hatSql/with_fill_test.go \
	scripts/benchmark-with-fill.sh \
	scripts/deliver-with-fill.sh \
	scripts/format-with-fill.sh \
	scripts/test-race-with-fill.sh \
	scripts/test-with-fill.sh; do
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
