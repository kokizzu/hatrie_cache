#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

case "${1:-inspect}" in
inspect)
	git status --short
	git diff -- hat/hatCache/journal.go hat/hatCache/journal_replay_fastpath.go hat/hatCache/journal_replay_fastpath_test.go scripts/test-journal-replay-fastpath.sh JOURNAL_REPLAY.md
	;;
commit)
	index=$(mktemp)
	inspiration=$(mktemp)
	inspiration_next=$(mktemp)
	makefile=$(mktemp)
	makefile_next=$(mktemp)
	trap 'rm -f "$index" "$inspiration" "$inspiration_next" "$makefile" "$makefile_next"' EXIT

	GIT_INDEX_FILE="$index" git read-tree HEAD
	GIT_INDEX_FILE="$index" git add -- \
		hat/hatCache/journal.go \
		hat/hatCache/journal_replay_fastpath.go \
		hat/hatCache/journal_replay_fastpath_test.go \
		scripts/test-journal-replay-fastpath.sh \
		scripts/deliver-journal-replay-fastpath.sh \
		JOURNAL_REPLAY.md

	git show HEAD:INSPIRATION.md > "$inspiration"
	awk '
		{ print }
		$0 == "- [ ] T042 Recovery-time parallel replay." {
			print "- [x] T042a Recovery replay mutation fast path - scalar durable mutations avoid constructing public command responses; unsupported commands keep the existing dispatcher (see [JOURNAL_REPLAY.md](JOURNAL_REPLAY.md))."
		}
	' "$inspiration" > "$inspiration_next"
	inspiration_blob=$(git hash-object -w "$inspiration_next")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md

	git show HEAD:Makefile > "$makefile"
	awk '
		{ print }
		$0 == "\tbash scripts/deliver-typed-table-aggregate-key.sh push" {
			print ""
			print ".PHONY: test-journal-replay-fastpath"
			print "test-journal-replay-fastpath:"
			print "\tsh ./scripts/test-journal-replay-fastpath.sh test"
			print ""
			print ".PHONY: format-journal-replay-fastpath"
			print "format-journal-replay-fastpath:"
			print "\tsh ./scripts/test-journal-replay-fastpath.sh format"
			print ""
			print ".PHONY: race-journal-replay-fastpath"
			print "race-journal-replay-fastpath:"
			print "\tsh ./scripts/test-journal-replay-fastpath.sh race"
			print ""
			print ".PHONY: benchmark-journal-replay-fastpath"
			print "benchmark-journal-replay-fastpath:"
			print "\tsh ./scripts/test-journal-replay-fastpath.sh bench"
		}
	' "$makefile" > "$makefile_next"
	makefile_blob=$(git hash-object -w "$makefile_next")
	GIT_INDEX_FILE="$index" git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

	printf '%s\n' '== isolated commit diff =='
	GIT_INDEX_FILE="$index" git diff --cached --name-status
	GIT_INDEX_FILE="$index" git diff --cached --stat
	GIT_INDEX_FILE="$index" git commit -m 'perf(cache): reduce journal replay mutation allocations'
	;;
push)
	git push origin master
	;;
*)
	printf '%s\n' 'usage: deliver-journal-replay-fastpath.sh [inspect|commit|push]' >&2
	exit 2
	;;
esac
