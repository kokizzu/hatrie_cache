#!/usr/bin/env bash
set -euo pipefail

paths=(
	Makefile
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CH007_ROW_TTL.md
	ENGINE_IDEAS.md
	hat/hatSql/ch007_row_ttl_benchmark_test.go
	hat/hatSql/ch225_ttl_expiry_index_test.go
	hat/hatSql/ch225_ttl_memory_benchmark_test.go
	hat/hatSql/typed_table.go
	hat/hatSql/typed_table_patch_parts.go
	hat/hatSql/typed_table_ttl.go
	hat/hatSql/typed_table_ttl_expiry_index.go
	scripts/format-ch007-ttl-c225.sh
	scripts/inspect-benchmark-ch007-c225.sh
	scripts/inspect-ttl-c225.sh
	scripts/inspect-ttl-mutation-c225.sh
	scripts/locate-ch007-c225.sh
	scripts/push-ch007-ttl-c225.sh
	scripts/run-ch007-ttl-c225.sh
	scripts/stage-ch007-ttl-c225.sh
	scripts/commit-ch007-ttl-c225.sh
)

if ! git diff --cached --quiet; then
	while IFS= read -r staged_path; do
		case " ${paths[*]} " in
			*" $staged_path "*) ;;
			*) printf 'refusing to reuse unexpected staged path: %s\n' "$staged_path" >&2; exit 1 ;;
		esac
	done < <(git diff --cached --name-only)
	git add -- scripts/stage-ch007-ttl-c225.sh
	git diff --cached --check
	exit 0
fi

git add -- "${paths[@]}"

makefile_tmp="$(mktemp /tmp/hatrie-cache-ch225-makefile.XXXXXX)"
cleanup() {
	rm -f "$makefile_tmp"
}
trap cleanup EXIT

git show HEAD:Makefile > "$makefile_tmp"
printf '%s\n' \
	'' \
	'.PHONY: inspect-ttl-c225 test-ch007-ttl-c225 test-ch007-ttl-all-c225 benchmark-ch007-ttl-c225 benchmark-ch007-ttl-before-c225 race-ch007-ttl-c225 vet-ch007-ttl-c225 inspect-ttl-mutation-c225 locate-ch007-c225 inspect-benchmark-ch007-c225 format-ch007-ttl-c225 stage-ch007-ttl-c225 commit-ch007-ttl-c225 push-ch007-ttl-c225' \
	'inspect-ttl-c225:' \
	$'\tbash ./scripts/inspect-ttl-c225.sh' \
	'test-ch007-ttl-c225:' \
	$'\tbash ./scripts/run-ch007-ttl-c225.sh test' \
	'test-ch007-ttl-all-c225:' \
	$'\tbash ./scripts/run-ch007-ttl-c225.sh all' \
	'benchmark-ch007-ttl-c225:' \
	$'\tbash ./scripts/run-ch007-ttl-c225.sh benchmark' \
	'benchmark-ch007-ttl-before-c225:' \
	$'\tbash ./scripts/run-ch007-ttl-c225.sh baseline' \
	'race-ch007-ttl-c225:' \
	$'\tbash ./scripts/run-ch007-ttl-c225.sh race' \
	'vet-ch007-ttl-c225:' \
	$'\tbash ./scripts/run-ch007-ttl-c225.sh vet' \
	'inspect-ttl-mutation-c225:' \
	$'\tbash ./scripts/inspect-ttl-mutation-c225.sh' \
	'locate-ch007-c225:' \
	$'\tbash ./scripts/locate-ch007-c225.sh' \
	'inspect-benchmark-ch007-c225:' \
	$'\tbash ./scripts/inspect-benchmark-ch007-c225.sh' \
	'format-ch007-ttl-c225:' \
	$'\tbash ./scripts/format-ch007-ttl-c225.sh' \
	'stage-ch007-ttl-c225:' \
	$'\tbash ./scripts/stage-ch007-ttl-c225.sh' \
	'commit-ch007-ttl-c225:' \
	$'\tbash ./scripts/commit-ch007-ttl-c225.sh' \
	'push-ch007-ttl-c225:' \
	$'\tbash ./scripts/push-ch007-ttl-c225.sh' >> "$makefile_tmp"

makefile_hash="$(git hash-object -w "$makefile_tmp")"
git update-index --add --cacheinfo "100644,$makefile_hash,Makefile"
git diff --cached --check
