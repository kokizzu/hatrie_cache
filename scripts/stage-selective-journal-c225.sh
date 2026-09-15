#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage C225 with pre-existing staged changes' >&2
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
    '# C225 checkpoint-only journal selective restore' \
    '' \
    '.PHONY: stage-selective-journal-c225 commit-selective-journal-c225 push-selective-journal-c225' \
    '' \
    'stage-selective-journal-c225:' \
    $'\tbash ./scripts/stage-selective-journal-c225.sh' \
    '' \
    'commit-selective-journal-c225:' \
    $'\tbash ./scripts/commit-selective-journal-c225.sh' \
    '' \
    'push-selective-journal-c225:' \
    $'\tbash ./scripts/push-selective-journal-c225.sh' >> "$staged_makefile"
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
	scripts/commit-selective-journal-c225.sh \
	scripts/push-selective-journal-c225.sh \
	scripts/stage-selective-journal-c225.sh

git diff --cached --check
git diff --cached --name-only
