#!/bin/sh
set -eu

feature_paths='
Makefile
README.md
BENCHMARK.md
INCREMENTAL_PROJECTIONS.md
hat/hatSql/incremental_projection.go
hat/hatSql/incremental_projection_test.go
hat/hatSql/incremental_projection_benchmark_test.go
hat/hatSql/projection_checkpoint_file.go
hat/hatCache/sql_incremental_projection.go
hat/hatCache/sql_incremental_projection_test.go
scripts/format-sql-incremental-projection.sh
scripts/test-sql-incremental-projection.sh
scripts/test-race-sql-incremental-projection.sh
scripts/benchmark-sql-incremental-projection.sh
scripts/verify-incremental-projection-docs.sh
scripts/audit-sql-incremental-projection-security.sh
scripts/deliver-sql-incremental-projection.sh
'

is_feature_path() {
	case "$1" in
		Makefile|README.md|BENCHMARK.md|INCREMENTAL_PROJECTIONS.md|hat/hatSql/incremental_projection.go|hat/hatSql/incremental_projection_test.go|hat/hatSql/incremental_projection_benchmark_test.go|hat/hatSql/projection_checkpoint_file.go|hat/hatCache/sql_incremental_projection.go|hat/hatCache/sql_incremental_projection_test.go|scripts/format-sql-incremental-projection.sh|scripts/test-sql-incremental-projection.sh|scripts/test-race-sql-incremental-projection.sh|scripts/benchmark-sql-incremental-projection.sh|scripts/verify-incremental-projection-docs.sh|scripts/audit-sql-incremental-projection-security.sh|scripts/deliver-sql-incremental-projection.sh)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

staged_paths=$(mktemp)
trap 'rm -f "$staged_paths"' EXIT
git diff --cached --name-only > "$staged_paths"
while IFS= read -r path; do
	if [ -n "$path" ] && ! is_feature_path "$path"; then
		printf '%s\n' "refusing to commit pre-staged unrelated path: $path" >&2
		exit 1
	fi
done < "$staged_paths"

git diff --check -- $feature_paths
git add -- $feature_paths
git diff --cached --check
if git diff --cached --quiet; then
	printf '%s\n' 'no incremental projection changes staged' >&2
	exit 1
fi
git commit -m 'feat(sql): add journal-driven incremental projections'
git push
