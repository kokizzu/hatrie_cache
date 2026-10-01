#!/usr/bin/env bash
set -euo pipefail

mode=${1:-review}
feature_files=(
	Makefile
	hat/hatCache/maintenance_read_only.go
	hat/hatCache/main.go
	hat/hatCache/command.go
	hat/hatCache/monitoring.go
	hat/hatCache/grpc.go
	hat/hatCache/tu06_replica_read_only_test.go
	hat/hatCache/tu06_replica_read_only_baseline_benchmark_test.go
	hat/hatCache/tu06_replica_read_only_benchmark_test.go
	scripts/run-tu06-replica-read-only.sh
	scripts/ship-tu06.sh
	TU06_REPLICA_READ_ONLY.md
	BENCHMARK.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	PRODUCT_IDEA_GAPS.md
)

case "$mode" in
review)
	git diff --check -- "${feature_files[@]}"
	git diff --stat -- "${feature_files[@]}"
	git diff -- "${feature_files[@]}"
	git status --short -- "${feature_files[@]}"
	;;
verify)
	for file in hat/hatSql/tu06_round_compat.go hat/hatCache/tu06_replica_read_only_compat.go; do
		if [[ -e "$file" ]]; then
			printf 'temporary compatibility file remains: %s\n' "$file" >&2
			exit 1
		fi
	done
	git diff --check -- "${feature_files[@]}"
	git status --short -- "${feature_files[@]}"
	;;
commit)
	git add -- "${feature_files[@]}"
	git diff --cached --check
	git commit -m "feat: add trie replica read-only admission [skip ci]"
	;;
push)
	git push -u origin HEAD
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
