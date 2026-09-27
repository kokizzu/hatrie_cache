#!/usr/bin/env bash
set -euo pipefail

action="${1:-stage}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

tmp_dir="$(mktemp -d /tmp/hatrie-m052ah-delivery.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

write_block() {
	local name="$1"
	case "$name" in
	makefile)
		cat >"$tmp_dir/$name.block" <<'EOF'
.PHONY: test-m052ah-auto-native-count-distinct
test-m052ah-auto-native-count-distinct:
	bash scripts/test-m052ah-auto-native-count-distinct.sh

.PHONY: format-m052ah-auto-native-count-distinct
format-m052ah-auto-native-count-distinct:
	bash scripts/format-m052ah-auto-native-count-distinct.sh

.PHONY: baseline-m052ah-auto-native-count-distinct
baseline-m052ah-auto-native-count-distinct:
	bash scripts/baseline-m052ah-auto-native-count-distinct.sh

.PHONY: verify-m052ah-auto-native-count-distinct
verify-m052ah-auto-native-count-distinct:
	bash scripts/verify-m052ah-auto-native-count-distinct.sh

.PHONY: stage-m052ah-auto-native-count-distinct
stage-m052ah-auto-native-count-distinct:
	bash scripts/deliver-m052ah-auto-native-count-distinct.sh stage

.PHONY: commit-m052ah-auto-native-count-distinct
commit-m052ah-auto-native-count-distinct:
	bash scripts/deliver-m052ah-auto-native-count-distinct.sh commit

.PHONY: push-m052ah-auto-native-count-distinct
push-m052ah-auto-native-count-distinct:
	bash scripts/deliver-m052ah-auto-native-count-distinct.sh push

.PHONY: deliver-m052ah-auto-native-count-distinct
deliver-m052ah-auto-native-count-distinct:
	bash scripts/deliver-m052ah-auto-native-count-distinct.sh deliver
EOF
		;;
	readme)
		cat >"$tmp_dir/$name.block" <<'EOF'

## Automatic Native `COUNT(DISTINCT)`

Ordinary row-resolver SQL queries now automatically use typed native dataflow
state for global and grouped `COUNT(DISTINCT scalar)` aggregates. Integer and
string keys are exact, NULL is ignored, and unsupported automatic runtime keys
fall back to the general executor. Set `SQLQueryOptions.DisableNativeDataflow`
to force the compatibility path. See
[M052AH_AUTO_NATIVE_COUNT_DISTINCT.md](M052AH_AUTO_NATIVE_COUNT_DISTINCT.md)
for semantics, limits, raw benchmark samples, and verification commands.
EOF
		;;
	benchmark)
		cat >"$tmp_dir/$name.block" <<'EOF'

<a id="m052ah-automatic-native-count-distinct"></a>
## M052ah Automatic Native `COUNT(DISTINCT)`

Command: `make baseline-m052ah-auto-native-count-distinct`.

Linux/amd64, AMD Ryzen 9 5950X; 8,192 rows, 128 groups, 64 repeated integer
values; five samples with `-benchtime=100ms -benchmem`.

| Path | Median ns/op | B/op | Allocs/op | Improvement vs fallback |
| --- | ---: | ---: | ---: | ---: |
| Automatic native | 1,898,469 | 1,371,958 | 8,770 | 6.31x CPU, 8.69x bytes, 11.39x allocs |
| Explicit fallback | 11,984,532 | 11,928,431 | 99,893 | 1.00x |

The pre-change automatic path was the same fallback at 11,961,020 ns/op,
11,928,290 B/op, and 99,892 allocs/op. The native path is fail-closed for
unsupported runtime key types; automatic execution retries through the general
executor, while direct native callers receive `ErrSQLNativeDataflowUnsupported`.
Full raw samples and the semantics matrix are in
[M052AH_AUTO_NATIVE_COUNT_DISTINCT.md](M052AH_AUTO_NATIVE_COUNT_DISTINCT.md).
EOF
		;;
	inspiration)
		cat >"$tmp_dir/$name.block" <<'EOF'
- [x] M052ah Automatic native `COUNT(DISTINCT)` for ordinary row resolvers.
  Global and grouped scalar counts reuse the existing typed integer/string
  distinct-key state, ignore SQL NULL, and preserve exact fallback behavior for
  unsupported runtime keys. The paired benchmark is 6.31x faster with 8.69x
  fewer bytes and 11.39x fewer allocations; see
  [M052AH_AUTO_NATIVE_COUNT_DISTINCT.md](M052AH_AUTO_NATIVE_COUNT_DISTINCT.md)
  and [BENCHMARK.md](BENCHMARK.md#m052ah-automatic-native-count-distinct).
EOF
		;;
	*)
		printf 'unknown block %s\n' "$name" >&2
		exit 1
		;;
	esac
}

stage_shared_file() {
	local path="$1"
	local block_name="$2"
	local marker="$3"
	local base="$tmp_dir/${block_name}.base"
	local updated="$tmp_dir/${block_name}.updated"
	local raw_patch="$tmp_dir/${block_name}.raw.patch"
	local patch="$tmp_dir/${block_name}.patch"

	git show "HEAD:$path" >"$base"
	awk -v marker="$marker" -v block="$tmp_dir/$block_name.block" '
		{ print }
		index($0, marker) {
			while ((getline line < block) > 0) print line
			close(block)
			found++
		}
		END { if (found != 1) exit 2 }
	' "$base" >"$updated"

	set +e
	diff -u "$base" "$updated" >"$raw_patch"
	status=$?
	set -e
	if [[ "$status" != 1 ]]; then
		printf 'unexpected diff status for %s: %s\n' "$path" "$status" >&2
		exit 1
	fi
	sed -e "1s|^--- .*|--- a/$path|" -e "2s|^+++ .*|+++ b/$path|" "$raw_patch" >"$patch"
	git apply --cached "$patch"
}

stage_feature() {
	if ! git diff --cached --quiet; then
		printf '%s\n' 'refusing to stage: the index already contains changes' >&2
		git diff --cached --name-only >&2
		exit 1
	fi
	write_block makefile
	write_block readme
	write_block benchmark
	write_block inspiration

	git add \
		M052AH_AUTO_NATIVE_COUNT_DISTINCT.md \
		hat/hatSql/m052ah_auto_native_count_distinct_benchmark_test.go \
		hat/hatSql/m052ah_auto_native_count_distinct_test.go \
		hat/hatSql/m052c_native_dataflow.go \
		hat/hatSql/m052p_auto_native_dataflow.go \
		hat/hatSql/query.go \
		scripts/baseline-m052ah-auto-native-count-distinct.sh \
		scripts/deliver-m052ah-auto-native-count-distinct.sh \
		scripts/format-m052ah-auto-native-count-distinct.sh \
		scripts/test-m052ah-auto-native-count-distinct.sh \
		scripts/verify-m052ah-auto-native-count-distinct.sh
	stage_shared_file Makefile makefile 'bash scripts/deliver-m039-incremental-sql-group-count-distinct.sh deliver'
	stage_shared_file README.md readme 'make benchmark-tu22` for focused verification.'
	stage_shared_file BENCHMARK.md benchmark '[M052AG_NATIVE_QUAD_GROUPED_ORDERED.md](M052AG_NATIVE_QUAD_GROUPED_ORDERED.md).'
	stage_shared_file INSPIRATION.md inspiration 'and [BENCHMARK.md](BENCHMARK.md#m052ag-native-four-field-grouped-ordered-top-n).'

	git diff --cached --check
	git diff --cached --name-status
}

commit_feature() {
	git diff --cached --quiet && {
		printf '%s\n' 'refusing to commit: no staged feature changes' >&2
		exit 1
	}
	git commit -m 'feat(sql): auto-native count distinct [skip ci]'
}

validate_staged_feature() {
	while IFS= read -r path; do
		case "$path" in
		BENCHMARK.md|INSPIRATION.md|M052AH_AUTO_NATIVE_COUNT_DISTINCT.md|Makefile|README.md|hat/hatSql/m052ah_auto_native_count_distinct_benchmark_test.go|hat/hatSql/m052ah_auto_native_count_distinct_test.go|hat/hatSql/m052c_native_dataflow.go|hat/hatSql/m052p_auto_native_dataflow.go|hat/hatSql/query.go|scripts/baseline-m052ah-auto-native-count-distinct.sh|scripts/deliver-m052ah-auto-native-count-distinct.sh|scripts/format-m052ah-auto-native-count-distinct.sh|scripts/test-m052ah-auto-native-count-distinct.sh|scripts/verify-m052ah-auto-native-count-distinct.sh)
			;;
		*)
			printf 'refusing to commit unexpected staged path: %s\n' "$path" >&2
			exit 1
			;;
		esac
	done < <(git diff --cached --name-only)
}

stage_or_validate_feature() {
	if git diff --cached --quiet; then
		stage_feature
		return
	fi
	validate_staged_feature
}

case "$action" in
stage)
	stage_feature
	;;
commit)
	stage_or_validate_feature
	commit_feature
	;;
push)
	git push origin HEAD
	;;
deliver)
	stage_or_validate_feature
	commit_feature
	git push origin HEAD
	;;
*)
	printf 'usage: %s {stage|commit|push|deliver}\n' "$0" >&2
	exit 2
	;;
esac
