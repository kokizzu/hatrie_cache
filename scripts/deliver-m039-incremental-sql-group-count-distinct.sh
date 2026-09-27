#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
case "$mode" in
stage|commit|push|deliver) ;;
*)
	printf '%s\n' "usage: $0 {stage|commit|push|deliver}" >&2
	exit 2
	;;
esac

stage_dir=$(mktemp -d /tmp/hatrie-m039-stage.XXXXXX)
trap 'rm -rf "$stage_dir"' EXIT

append_diff() {
	local old_label=$1
	local new_label=$2
	local old_file=$3
	local new_file=$4
	local status
	local raw_file="$stage_dir/$(basename "$old_file").raw.patch"
	local old_base=$(basename "$old_file")
	local new_base=$(basename "$new_file")
	set +e
	git diff --no-index --no-prefix "$old_file" "$new_file" > "$raw_file"
	status=$?
	set -e
	if [ "$status" -ne 1 ]; then
		exit "$status"
	fi
	sed -e "s|[^[:space:]]*/$new_base|$new_label|g" -e "s|[^[:space:]]*/$old_base|$old_label|g" "$raw_file" >> "$stage_dir/feature.patch"
}

build_index_patch() {
	git show HEAD:Makefile > "$stage_dir/Makefile.base"
	cp "$stage_dir/Makefile.base" "$stage_dir/Makefile.feature"
	cat >> "$stage_dir/Makefile.feature" <<'EOF'

.PHONY: baseline-m039-incremental-sql-group-count-distinct
baseline-m039-incremental-sql-group-count-distinct:
	bash scripts/baseline-m039-incremental-sql-group-count-distinct.sh

.PHONY: verify-m039-incremental-sql-group-count-distinct
verify-m039-incremental-sql-group-count-distinct:
	bash scripts/verify-m039-incremental-sql-group-count-distinct.sh

.PHONY: test-m039-incremental-sql-group-count-distinct
test-m039-incremental-sql-group-count-distinct:
	bash scripts/test-m039-incremental-sql-group-count-distinct.sh

.PHONY: stage-m039-incremental-sql-group-count-distinct
stage-m039-incremental-sql-group-count-distinct:
	bash scripts/deliver-m039-incremental-sql-group-count-distinct.sh stage

.PHONY: commit-m039-incremental-sql-group-count-distinct
commit-m039-incremental-sql-group-count-distinct:
	bash scripts/deliver-m039-incremental-sql-group-count-distinct.sh commit

.PHONY: push-m039-incremental-sql-group-count-distinct
push-m039-incremental-sql-group-count-distinct:
	bash scripts/deliver-m039-incremental-sql-group-count-distinct.sh push

.PHONY: deliver-m039-incremental-sql-group-count-distinct
deliver-m039-incremental-sql-group-count-distinct:
	bash scripts/deliver-m039-incremental-sql-group-count-distinct.sh deliver
EOF

	git show HEAD:BENCHMARK.md > "$stage_dir/BENCHMARK.base"
	cp "$stage_dir/BENCHMARK.base" "$stage_dir/BENCHMARK.feature"
	cat >> "$stage_dir/BENCHMARK.feature" <<'EOF'

<a id="m039-incremental-sql-count-distinct"></a>
## M039 Incremental SQL `COUNT(DISTINCT)`

Command: `make baseline-m039-incremental-sql-group-count-distinct`.

The rebuild path reconstructs a 10,000-row exact grouped distinct state for
each operation. The incremental path seeds that state once and applies one
duplicate `int64` update per operation. Five `-benchmem` samples were run on
Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Rebuild 10,000 rows | 5,576,928 | 7,965,703 | 40,919 | 1.00x |
| Incremental one-row update | 434.6 | 80 | 3 | 12,832x faster |

The incremental path is 99,571x lower in transient allocated bytes and 13,640x
lower in allocation count. These are per-operation allocation measurements;
the incremental operator intentionally retains its exact multiplicity state.
Raw samples and supported SQL shape are in
[M039_INCREMENTAL_SQL_GROUP_COUNT_DISTINCT.md](M039_INCREMENTAL_SQL_GROUP_COUNT_DISTINCT.md).
EOF

	: > "$stage_dir/feature.patch"
	append_diff a/Makefile b/Makefile "$stage_dir/Makefile.base" "$stage_dir/Makefile.feature"
	append_diff a/BENCHMARK.md b/BENCHMARK.md "$stage_dir/BENCHMARK.base" "$stage_dir/BENCHMARK.feature"
	git apply --cached "$stage_dir/feature.patch"
}

stage_feature() {
	build_index_patch
	git add -- \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		ENGINE_IDEAS.md \
		MZ034_GENERIC_NEGATIVE_DIFF.md \
		README.md \
		hat/hatSql/m052c_native_dataflow.go \
		hat/hatSql/query.go \
		M039_INCREMENTAL_SQL_GROUP_COUNT_DISTINCT.md \
		hat/hatSql/m039_sql_incremental_group_count_distinct.go \
		hat/hatSql/m039_sql_incremental_group_count_distinct_baseline_test.go \
		hat/hatSql/m039_sql_incremental_group_count_distinct_test.go \
		scripts/baseline-m039-incremental-sql-group-count-distinct.sh \
		scripts/deliver-m039-incremental-sql-group-count-distinct.sh \
		scripts/test-m039-incremental-sql-group-count-distinct.sh \
		scripts/verify-m039-incremental-sql-group-count-distinct.sh
	git diff --cached --check
	git diff --cached --name-status
}

commit_feature() {
	git commit -m 'feat(sql): add incremental grouped count distinct [skip ci]'
}

push_feature() {
	git push origin HEAD
}

case "$mode" in
stage)
	stage_feature
	;;
commit)
	commit_feature
	;;
push)
	push_feature
	;;
deliver)
	stage_feature
	commit_feature
	push_feature
	;;
esac
