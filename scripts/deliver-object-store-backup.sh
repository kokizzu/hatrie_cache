#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

mode=${1:-preview}
commit_message='feat(backup): add object-store backup target'
child='- [x] C128a Streaming object-store backup targets with verified manifests and atomic restore.'

if [ "$mode" = push ]; then
	git push
	git rev-parse HEAD
	exit 0
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-object-store-backup.XXXXXX")
index="$work/index"
trap 'rm -rf -- "$work"' EXIT

base=$(git rev-parse HEAD)
git show "$base:Makefile" > "$work/Makefile"
git show "$base:INSPIRATION.md" > "$work/INSPIRATION.md"

checklist_existing=0
if awk '/^- \[x\] C128a / { found=1 } END { exit !found }' "$work/INSPIRATION.md"; then
	checklist_existing=1
else
	awk -v child="$child" '
	BEGIN { added=0 }
	{
		print
		if (!added && $0 ~ /^- \[ \] C128 /) {
			print child
			added=1
		}
	}
	END {
		if (!added) {
			print "C128 parent checklist row was not found" > "/dev/stderr"
			exit 1
		}
	}' "$work/INSPIRATION.md" > "$work/INSPIRATION.next"
	mv "$work/INSPIRATION.next" "$work/INSPIRATION.md"
fi

if [ "$checklist_existing" -eq 1 ]; then
	commit_message='fix(backup): make object-store delivery idempotent'
fi

if ! awk '/^# object-store-backup-targets$/ { found=1 } END { exit found }' "$work/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "# object-store-backup-targets"
		print "format-object-store-backup:"
		print "\tbash ./scripts/format-object-store-backup.sh"
		print ""
		print "test-object-store-backup:"
		print "\tbash ./scripts/test-object-store-backup.sh"
		print ""
		print "test-race-object-store-backup:"
		print "\tbash ./scripts/test-race-object-store-backup.sh"
		print ""
		print "benchmark-object-store-backup:"
		print "\tbash ./scripts/benchmark-object-store-backup.sh"
		print ""
		print "deliver-object-store-backup:"
		print "\tbash ./scripts/deliver-object-store-backup.sh preview"
		print ""
		print "commit-object-store-backup:"
		print "\tbash ./scripts/deliver-object-store-backup.sh commit"
		print ""
		print "push-object-store-backup:"
		print "\tbash ./scripts/deliver-object-store-backup.sh push"
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
	OBJECT_STORE_BACKUP.md \
	hat/hatBackup/object_store.go \
	hat/hatBackup/object_store_test.go \
	scripts/benchmark-object-store-backup.sh \
	scripts/deliver-object-store-backup.sh \
	scripts/format-object-store-backup.sh \
	scripts/test-object-store-backup.sh \
	scripts/test-race-object-store-backup.sh; do
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
