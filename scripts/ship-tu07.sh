#!/usr/bin/env bash
set -euo pipefail

feature_files=(
	"ADOPTED_QUERY_ENGINE_IDEAS.md"
	"BENCHMARK.md"
	"PRODUCT_IDEA_GAPS.md"
	"TU07_REPLICA_RPO_METRICS.md"
	"Makefile"
	"hat/hatCache/monitoring.go"
	"hat/hatCache/replication.go"
	"hat/hatCache/replication_region.go"
	"hat/hatCache/tu07_replica_rpo_metrics_test.go"
	"scripts/format-tu07-replica-rpo.sh"
	"scripts/run-tu07-replica-rpo.sh"
	"scripts/ship-tu07.sh"
)

mode="${1:-review}"
case "$mode" in
review)
	git diff --check -- "${feature_files[@]}"
	git diff --cached --check -- "${feature_files[@]}"
	bash -n scripts/format-tu07-replica-rpo.sh scripts/run-tu07-replica-rpo.sh scripts/ship-tu07.sh
	if compgen -G '.tu07-go-cache-*' > /dev/null; then
		printf 'round-local Go cache was not cleaned\n' >&2
		exit 1
	fi
	if test -e scripts/inspect-round31-replica-lag.sh || test -e scripts/audit-round31-processes.sh; then
		printf 'round-only diagnostic script remains\n' >&2
		exit 1
	fi
	git status --short -- "${feature_files[@]}"
	git diff --stat -- "${feature_files[@]}"
	;;
stage)
	git add -- "${feature_files[@]}"
	;;
commit)
	git add -- "${feature_files[@]}"
	git commit -m "feat: expose regional RPO monitoring metrics [skip ci]"
	;;
push)
	git push -u origin "$(git branch --show-current)"
	;;
verify)
	git status --short
	git log -1 --oneline
	;;
*)
	printf 'usage: %s {review|stage|commit|push|verify}\n' "$0" >&2
	exit 2
	;;
esac
