#!/usr/bin/env bash
set -euo pipefail

mode="${1:-status}"
case "$mode" in
status)
	git status --short
	;;
stage)
	git add \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		BENCHMARK.md \
		PRODUCT_IDEA_GAPS.md \
		README.md \
		TU11_CONFLICT_POLICY_SNAPSHOTS.md \
		Makefile \
		hat/hatReplication/conflict_policy.go \
		hat/hatReplication/conflict_policy_registry_benchmark_test.go \
		hat/hatReplication/conflict_policy_registry_test.go \
		scripts/benchmark-chg12.sh \
		scripts/check-chg12.sh \
		scripts/deliver-chg12.sh \
		scripts/format-chg12.sh \
		scripts/test-chg12.sh
	;;
commit)
	git commit -m 'feat(replication): add conflict policy snapshots [skip ci]'
	;;
push)
	git push -u origin codex/chg12-conflict-policy
	;;
*)
	printf 'unknown chg12 delivery mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
