#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage C226 with pre-existing staged changes' >&2
	exit 1
fi

original_makefile=$(mktemp)
staged_makefile=$(mktemp)
cleanup() {
	cp "$original_makefile" Makefile
	rm -f "$original_makefile" "$staged_makefile"
}
trap cleanup EXIT

cp Makefile "$original_makefile"
git show HEAD:Makefile > "$staged_makefile"
printf '%s\n' \
    '' \
    '# C226 partition-scoped journal tail restore' \
    '' \
    '.PHONY: stage-journal-tail-c226 commit-journal-tail-c226 push-journal-tail-c226' \
    '' \
    'stage-journal-tail-c226:' \
    $'\tbash ./scripts/stage-journal-tail-c226.sh' \
    '' \
    'commit-journal-tail-c226:' \
    $'\tbash ./scripts/commit-journal-tail-c226.sh' \
    '' \
    'push-journal-tail-c226:' \
    $'\tbash ./scripts/push-journal-tail-c226.sh' >> "$staged_makefile"
cp "$staged_makefile" Makefile

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	hat/hatCache/backup_partition_restore.go \
	hat/hatCache/backup_partition_restore_benchmark_test.go \
	hat/hatCache/backup_partition_restore_test.go \
	hat/hatCache/backup_restore.go \
	scripts/commit-journal-tail-c226.sh \
	scripts/push-journal-tail-c226.sh \
	scripts/stage-journal-tail-c226.sh

git diff --cached --check
git diff --cached --name-only
