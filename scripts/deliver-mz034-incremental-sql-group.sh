#!/usr/bin/env bash
set -euo pipefail

mode="${1:-deliver}"
root="$(git rev-parse --show-toplevel)"
cd "$root"

files=(
  "MZ034_INCREMENTAL_SQL_GROUP_AGGREGATE.md"
  "MZ034_GENERIC_NEGATIVE_DIFF.md"
  "README.md"
  "ENGINE_IDEAS.md"
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "hat/hatSql/mz034_sql_incremental_group.go"
  "hat/hatSql/mz034_sql_incremental_group_test.go"
  "hat/hatSql/mz034_sql_incremental_group_baseline_test.go"
  "scripts/baseline-mz034-incremental-sql-group.sh"
  "scripts/test-mz034-incremental-sql-group.sh"
  "scripts/format-mz034-incremental-sql-group.sh"
  "scripts/benchmark-mz034-incremental-sql-group.sh"
  "scripts/test-mz034-incremental-sql-group-package.sh"
  "scripts/race-mz034-incremental-sql-group.sh"
  "scripts/vet-mz034-incremental-sql-group.sh"
  "scripts/deliver-mz034-incremental-sql-group.sh"
  "Makefile"
)

if [[ "$mode" == "stage" || "$mode" == "deliver" ]] && ! git diff --cached --quiet; then
  printf '%s\n' "refusing to stage with pre-existing staged changes" >&2
  exit 1
fi

makefile_block=$(cat <<'EOF'

baseline-mz034-incremental-sql-group:
	@bash ./scripts/baseline-mz034-incremental-sql-group.sh

test-mz034-incremental-sql-group:
	@bash ./scripts/test-mz034-incremental-sql-group.sh

format-mz034-incremental-sql-group:
	@bash ./scripts/format-mz034-incremental-sql-group.sh

benchmark-mz034-incremental-sql-group:
	@bash ./scripts/benchmark-mz034-incremental-sql-group.sh

test-mz034-incremental-sql-group-package:
	@bash ./scripts/test-mz034-incremental-sql-group-package.sh

race-mz034-incremental-sql-group:
	@bash ./scripts/race-mz034-incremental-sql-group.sh

vet-mz034-incremental-sql-group:
	@bash ./scripts/vet-mz034-incremental-sql-group.sh

stage-mz034-incremental-sql-group:
	@bash ./scripts/deliver-mz034-incremental-sql-group.sh stage

commit-mz034-incremental-sql-group:
	@bash ./scripts/deliver-mz034-incremental-sql-group.sh commit

push-mz034-incremental-sql-group:
	@bash ./scripts/deliver-mz034-incremental-sql-group.sh push

deliver-mz034-incremental-sql-group:
	@bash ./scripts/deliver-mz034-incremental-sql-group.sh deliver
EOF
)

stage_feature() {
  git add -- "${files[@]}"
  temporary_makefile="$(mktemp)"
  trap 'rm -f "$temporary_makefile"' RETURN
  git show HEAD:Makefile > "$temporary_makefile"
  printf '%s\n' "$makefile_block" >> "$temporary_makefile"
  blob="$(git hash-object -w "$temporary_makefile")"
  git update-index --add --cacheinfo "100644,$blob,Makefile"
  git diff --cached --check
}

case "$mode" in
stage)
  stage_feature
  ;;
commit)
  if git diff --cached --quiet; then
    printf '%s\n' "no staged feature changes; run the stage target first" >&2
    exit 1
  fi
  git diff --cached --check
  git commit -m "feat(sql): add incremental SQL group aggregate lowering [skip ci]"
  ;;
push)
  git push origin HEAD
  ;;
deliver)
  stage_feature
  git commit -m "feat(sql): add incremental SQL group aggregate lowering [skip ci]"
  git push origin HEAD
  ;;
*)
  printf 'usage: %s {stage|commit|push|deliver}\n' "$0" >&2
  exit 2
  ;;
esac
