#!/usr/bin/env bash
set -euo pipefail

mode="${1:?expected status, verify, stage, commit, or push}"
branch="$(git branch --show-current)"
expected_branch="codex/chg20-clickhouse-next"

if [[ "$branch" != "$expected_branch" ]]; then
	printf 'unexpected branch: %s (want %s)\n' "$branch" "$expected_branch" >&2
	exit 1
fi

case "$mode" in
status)
	git status --short --branch
	;;
verify)
	bash scripts/test-chg20-tuple-journal.sh format
	bash scripts/test-chg20-tuple-journal.sh unit
	bash scripts/test-chg20-tuple-journal.sh race
	bash scripts/test-chg20-tuple-journal.sh vet
	bash scripts/test-chg20-tuple-journal.sh package
	bash scripts/test-chg20-tuple-journal.sh benchmark
	git diff --check
	;;
stage)
	git add Makefile README.md PRODUCT_IDEA_GAPS.md TU19_DURABLE_TUPLE_FIELD_JOURNAL.md \
		hat/hatDataStructure/tuple_field_update_journal.go \
		hat/hatDataStructure/tuple_field_update_journal_test.go \
		hat/hatDataStructure/tuple_field_update_journal_benchmark_test.go \
		scripts/test-chg20-tuple-journal.sh scripts/deliver-chg20-tuple-journal.sh
	git diff --cached --check
	git status --short
	;;
commit)
	git diff --cached --check
	git commit -m 'feat(tuple): add durable field-update journal [skip ci]'
	;;
push)
	git push origin "$branch"
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
