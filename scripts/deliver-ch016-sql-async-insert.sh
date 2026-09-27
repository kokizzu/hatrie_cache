#!/usr/bin/env bash
set -euo pipefail

expected_branch="codex/inspiration-next-10"
current_branch="$(git branch --show-current)"
if [[ "$current_branch" != "$expected_branch" ]]; then
	printf 'unexpected branch: %s (want %s)\n' "$current_branch" "$expected_branch" >&2
	exit 1
fi

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage while the index already contains changes' >&2
	exit 1
fi

required=(
	CH016_SQL_ASYNC_INSERT.md
	ENGINE_IDEAS.md
	README.md
	hat/hatCache/sql.go
	hat/hatCache/ch016_async_insert_sql.go
	hat/hatCache/ch016_async_insert_sql_test.go
	hat/hatCache/ch016_async_insert_sql_benchmark_test.go
	scripts/format-ch016.sh
	scripts/test-ch016-async-insert-sql.sh
	scripts/benchmark-ch016-async-insert.sh
	scripts/test-ch016-package.sh
	scripts/race-ch016.sh
	scripts/vet-ch016.sh
	scripts/deliver-ch016-sql-async-insert.sh
)
for path in "${required[@]}"; do
	if [[ ! -f "$path" ]]; then
		printf 'missing required path: %s\n' "$path" >&2
		exit 1
	fi
done

git add "${required[@]}"
git diff --cached --check
git commit -m 'feat: add CH-016 SQL async insert adapter [skip ci]'
git push origin HEAD
