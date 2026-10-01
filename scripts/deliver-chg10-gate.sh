#!/usr/bin/env bash
set -euo pipefail

branch="codex/chg10-replica-readonly"
commit_message="feat(replication): add replica read-only gate [skip ci]"
paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	PRODUCT_IDEA_GAPS.md
	README.md
	TU06_REPLICA_READ_ONLY_GATE.md
	Makefile
	hat/hatReplication/tr010_replica_read_only_gate.go
	hat/hatReplication/tr010_replica_read_only_gate_test.go
	scripts/deliver-chg10-gate.sh
	scripts/test-chg10-gate.sh
	scripts/verify-chg10-gate.sh
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
