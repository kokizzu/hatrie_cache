#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
commit_message='feat(data-structure): add durable tuple update journal [skip ci]'
paths=(
	Makefile
	BENCHMARK.md
	DATA_STRUCTURE.md
	PRODUCT_IDEA_GAPS.md
	README.md
	TU19_DURABLE_TUPLE_UPDATE_JOURNAL.md
	hat/hatDataStructure/tuple_update_journal.go
	hat/hatDataStructure/tuple_update_journal_test.go
	hat/hatDataStructure/tuple_update_journal_benchmark_test.go
	scripts/test-chg23-tuple-journal.sh
	scripts/deliver-chg23-tuple-journal.sh
)

case "$mode" in
status)
	git status --short
	git diff --check
	git diff --stat
	git diff -- README.md DATA_STRUCTURE.md PRODUCT_IDEA_GAPS.md BENCHMARK.md Makefile
	;;
stage)
	git add "${paths[@]}"
	git diff --cached --check
	git diff --cached --stat
	;;
commit)
	git commit -m "$commit_message"
	;;
push)
	git push -u origin HEAD
	;;
*)
	printf 'unsupported mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
