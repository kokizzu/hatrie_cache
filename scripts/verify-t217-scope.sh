#!/usr/bin/env bash
set -euo pipefail

unexpected=0
while IFS= read -r line; do
	path=${line:3}
	case "$path" in
		Makefile|README.md|BENCHMARK.md|INSPIRATION_ROUND2.md|T217_COLUMNAR_BATCH_INGEST.md)
			printf 'T217-SCOPE %s\n' "$path"
			;;
		hat/hatSql/typed_table_columnar_batch.go|hat/hatSql/t217_columnar_batch_benchmark_test.go|hat/hatSql/t217_columnar_batch_test.go)
			printf 'T217-SCOPE %s\n' "$path"
			;;
		scripts/format-t217.sh|scripts/test-t217.sh|scripts/benchmark-t217-before.sh|scripts/benchmark-t217.sh)
			printf 'T217-SCOPE %s\n' "$path"
			;;
		scripts/test-t217-package.sh|scripts/race-t217.sh|scripts/vet-t217.sh|scripts/verify-t217-scope.sh)
			printf 'T217-SCOPE %s\n' "$path"
			;;
		scripts/stage-t217.sh|scripts/commit-t217.sh|scripts/push-t217.sh)
			printf 'T217-SCOPE %s\n' "$path"
			;;
		scripts/commit-c237-projection.sh|scripts/inspect-c237-row-mapping.sh|scripts/push-c237-projection.sh|scripts/stage-c237-projection.sh)
			printf 'PROTECTED-UNRELATED %s\n' "$path"
			;;
		*)
			printf 'UNEXPECTED %s\n' "$path" >&2
			unexpected=1
			;;
	esac
done < <(git status --short --untracked-files=all)

if [[ "$unexpected" -ne 0 ]]; then
	exit 1
fi
printf 'T217 scope verified.\n'
