#!/usr/bin/env bash
set -euo pipefail

branch="codex/chg11-write-quorum"
commit_message="feat(replication): add journal write quorum [skip ci]"
paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	PRODUCT_IDEA_GAPS.md
	README.md
	TU10_JOURNAL_WRITE_QUORUM.md
	Makefile
	hat/hatReplication/tu010_journal_write_quorum.go
	hat/hatReplication/tr011_journal_write_quorum_test.go
	scripts/deliver-chg11-quorum.sh
	scripts/test-chg11-quorum.sh
)

check() {
	git diff --check HEAD
}

status() {
	git status --short
}

stage() {
	git add -- "${paths[@]}"
	git diff --cached --check
	git diff --cached --name-status
}

commit() {
	stage
	git commit -m "$commit_message"
}

push() {
	current_branch="$(git branch --show-current)"
	if [ "$current_branch" != "$branch" ]; then
		echo "expected branch $branch, got $current_branch" >&2
		exit 1
	fi
	git push -u origin "$branch"
}

case "${1:-check}" in
check)
	check
	;;
status)
	status
	;;
stage)
	stage
	;;
commit)
	commit
	;;
push)
	push
	;;
*)
	echo "usage: $0 {check|status|stage|commit|push}" >&2
	exit 2
	;;
esac
