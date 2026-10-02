#!/usr/bin/env bash
set -euo pipefail

mode="${1:?expected status, verify, stage, commit, or push}"
branch="$(git branch --show-current)"
expected_branch="codex/chg21-clickhouse-next"

if [[ "$branch" != "$expected_branch" ]]; then
	printf 'unexpected branch: %s (want %s)\n' "$branch" "$expected_branch" >&2
	exit 1
fi

case "$mode" in
status)
	git status --short --branch
	;;
verify)
	bash scripts/test-chg21-conflict.sh format
	bash scripts/test-chg21-conflict.sh unit
	bash scripts/test-chg21-conflict.sh race
	bash scripts/test-chg21-conflict.sh vet
	bash scripts/test-chg21-conflict.sh package
	bash scripts/test-chg21-conflict.sh benchmark
	git diff --check
	;;
stage)
	git add Makefile README.md PRODUCT_IDEA_GAPS.md TU38_CONFLICT_INTROSPECTION.md \
		hat/hatReplication/conflict_introspection.go \
		hat/hatReplication/conflict_introspection_test.go \
		hat/hatReplication/conflict_introspection_benchmark_test.go \
		scripts/test-chg21-conflict.sh scripts/deliver-chg21-conflict.sh
	git diff --cached --check
	git status --short
	;;
commit)
	git diff --cached --check
	git commit -m 'feat(replication): add redacted conflict introspection [skip ci]'
	;;
push)
	git push origin "$branch"
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
