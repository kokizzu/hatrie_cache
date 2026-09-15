#!/usr/bin/env bash
set -euo pipefail

paths=(
	Makefile
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	ENGINE_IDEAS.md
	INSPIRATION_BACKLOG.md
	MZ002_TYPED_TABLE_READ_HOLDS.md
	hat/hatSql/mz002_typed_table_compact_benchmark_test.go
	hat/hatSql/mz002_typed_table_read_hold_benchmark_test.go
	hat/hatSql/mz002_typed_table_read_hold_test.go
	hat/hatSql/typed_table.go
	hat/hatSql/typed_table_change_read_hold.go
	scripts/format-mz002-c226.sh
	scripts/inspect-mz002-c226.sh
	scripts/locate-mz002-c226.sh
	scripts/run-mz002-c226.sh
	scripts/stage-mz002-c226.sh
	scripts/commit-mz002-c226.sh
	scripts/push-mz002-c226.sh
)

if ! git diff --cached --quiet; then
	while IFS= read -r staged_path; do
		case " ${paths[*]} " in
			*" $staged_path "*) ;;
			*) printf 'refusing to reuse unexpected staged path: %s\n' "$staged_path" >&2; exit 1 ;;
		esac
	done < <(git diff --cached --name-only)
	git add -- scripts/stage-mz002-c226.sh
	git diff --cached --check
	exit 0
fi

git add -- "${paths[@]}"

makefile_tmp="$(mktemp /tmp/hatrie-cache-mz002-makefile.XXXXXX)"
cleanup() {
	rm -f "$makefile_tmp"
}
trap cleanup EXIT

git show HEAD:Makefile > "$makefile_tmp"
printf '%s\n' \
	'' \
	'.PHONY: inspect-mz002-c226 test-mz002-c226 test-mz002-all-c226 benchmark-mz002-c226 benchmark-mz002-before-c226 race-mz002-c226 vet-mz002-c226 locate-mz002-c226 format-mz002-c226 stage-mz002-c226 commit-mz002-c226 push-mz002-c226' \
	'inspect-mz002-c226:' \
	$'\tbash ./scripts/inspect-mz002-c226.sh' \
	'test-mz002-c226:' \
	$'\tbash ./scripts/run-mz002-c226.sh test' \
	'test-mz002-all-c226:' \
	$'\tbash ./scripts/run-mz002-c226.sh all' \
	'benchmark-mz002-c226:' \
	$'\tbash ./scripts/run-mz002-c226.sh benchmark' \
	'benchmark-mz002-before-c226:' \
	$'\tbash ./scripts/run-mz002-c226.sh baseline' \
	'race-mz002-c226:' \
	$'\tbash ./scripts/run-mz002-c226.sh race' \
	'vet-mz002-c226:' \
	$'\tbash ./scripts/run-mz002-c226.sh vet' \
	'locate-mz002-c226:' \
	$'\tbash ./scripts/locate-mz002-c226.sh' \
	'format-mz002-c226:' \
	$'\tbash ./scripts/format-mz002-c226.sh' \
	'stage-mz002-c226:' \
	$'\tbash ./scripts/stage-mz002-c226.sh' \
	'commit-mz002-c226:' \
	$'\tbash ./scripts/commit-mz002-c226.sh' \
	'push-mz002-c226:' \
	$'\tbash ./scripts/push-mz002-c226.sh' >> "$makefile_tmp"

makefile_hash="$(git hash-object -w "$makefile_tmp")"
git update-index --add --cacheinfo "100644,$makefile_hash,Makefile"
git diff --cached --check
