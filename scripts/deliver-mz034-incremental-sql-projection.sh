#!/usr/bin/env bash
set -euo pipefail

mode=${1:-deliver}
commit_message='feat(sql): add incremental SQL projection lowering [skip ci]'
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz034-projection.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

feature_files=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	ENGINE_IDEAS.md
	MZ034_GENERIC_NEGATIVE_DIFF.md
	MZ034_INCREMENTAL_SQL_PROJECTION.md
	README.md
	hat/hatSql/mz034_sql_incremental_projection.go
	hat/hatSql/mz034_sql_incremental_projection_test.go
	hat/hatSql/mz034_sql_incremental_projection_baseline_test.go
	scripts/deliver-mz034-incremental-sql-projection.sh
	scripts/test-mz034-incremental-sql-projection.sh
)

makefile_target_block='baseline-mz034-incremental-sql-projection:
	bash ./scripts/test-mz034-incremental-sql-projection.sh baseline

format-mz034-incremental-sql-projection:
	bash ./scripts/test-mz034-incremental-sql-projection.sh format

test-mz034-incremental-sql-projection:
	bash ./scripts/test-mz034-incremental-sql-projection.sh test

benchmark-mz034-incremental-sql-projection:
	bash ./scripts/test-mz034-incremental-sql-projection.sh benchmark

test-mz034-incremental-sql-projection-package:
	bash ./scripts/test-mz034-incremental-sql-projection.sh package

race-mz034-incremental-sql-projection:
	bash ./scripts/test-mz034-incremental-sql-projection.sh race

vet-mz034-incremental-sql-projection:
	bash ./scripts/test-mz034-incremental-sql-projection.sh vet

stage-mz034-incremental-sql-projection:
	bash ./scripts/deliver-mz034-incremental-sql-projection.sh stage

commit-mz034-incremental-sql-projection:
	bash ./scripts/deliver-mz034-incremental-sql-projection.sh commit

push-mz034-incremental-sql-projection:
	bash ./scripts/deliver-mz034-incremental-sql-projection.sh push

deliver-mz034-incremental-sql-projection:
	bash ./scripts/deliver-mz034-incremental-sql-projection.sh deliver'

stage_feature() {
	if [[ -n "$(git diff --cached --name-only)" ]]; then
		echo "refusing to stage with pre-existing staged changes" >&2
		git diff --cached --name-only >&2
		exit 1
	fi
	git diff --check
	for path in "${feature_files[@]}"; do
		git add -- "$path"
	done
	printf '%s\n' "$makefile_target_block" > "$tmp_dir/makefile-targets"
	git show HEAD:Makefile > "$tmp_dir/Makefile"
	cat "$tmp_dir/makefile-targets" >> "$tmp_dir/Makefile"
	makefile_hash=$(git hash-object -w "$tmp_dir/Makefile")
	git update-index --cacheinfo "100644,$makefile_hash,Makefile"
	git diff --cached --check
}

case "$mode" in
stage)
	stage_feature
	;;
commit)
	git commit -m "$commit_message"
	;;
push)
	git push
	;;
deliver)
	stage_feature
	git commit -m "$commit_message"
	git push
	;;
*)
	echo "usage: $0 {stage|commit|push|deliver}" >&2
	exit 2
	;;
esac
