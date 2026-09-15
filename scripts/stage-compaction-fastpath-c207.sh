#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
	BENCHMARK.md
	INSPIRATION_BACKLOG.md
	README.md
	TR054_COMPACTION_SCHEDULER_SINGLE_TASK.md
	hat/hatStorage/compaction_scheduler.go
	hat/hatStorage/compaction_scheduler_fastpath_benchmark_test.go
	hat/hatStorage/compaction_scheduler_fastpath_test.go
	scripts/benchmark-compaction-fastpath-c207.sh
	scripts/commit-compaction-fastpath-c207.sh
	scripts/format-compaction-fastpath-c207.sh
	scripts/inspect-compaction-fastpath-diff-c207.sh
	scripts/stage-compaction-fastpath-c207.sh
	scripts/test-compaction-fastpath-c207.sh
	scripts/verify-compaction-fastpath-c207.sh
	scripts/push-compaction-fastpath-c207.sh
)

makefile_backup=$(mktemp)
makefile_staged=$(mktemp)
cleanup() {
	if [[ -f "$makefile_backup" ]]; then
		cp -- "$makefile_backup" Makefile
	fi
	rm -f -- "$makefile_backup" "$makefile_staged"
}
trap cleanup EXIT

cp -- Makefile "$makefile_backup"
git show HEAD:Makefile >"$makefile_staged"
printf '\n.PHONY: test-compaction-fastpath-c207 benchmark-compaction-fastpath-c207 format-compaction-fastpath-c207 verify-compaction-fastpath-c207 inspect-compaction-fastpath-diff-c207 stage-compaction-fastpath-c207 commit-compaction-fastpath-c207 push-compaction-fastpath-c207\n' >>"$makefile_staged"
printf 'test-compaction-fastpath-c207:\n\tbash ./scripts/test-compaction-fastpath-c207.sh\n\n' >>"$makefile_staged"
printf 'benchmark-compaction-fastpath-c207:\n\tbash ./scripts/benchmark-compaction-fastpath-c207.sh\n\n' >>"$makefile_staged"
printf 'format-compaction-fastpath-c207:\n\tbash ./scripts/format-compaction-fastpath-c207.sh\n\n' >>"$makefile_staged"
printf 'verify-compaction-fastpath-c207:\n\tbash ./scripts/verify-compaction-fastpath-c207.sh\n\n' >>"$makefile_staged"
printf 'inspect-compaction-fastpath-diff-c207:\n\tbash ./scripts/inspect-compaction-fastpath-diff-c207.sh\n\n' >>"$makefile_staged"
printf 'stage-compaction-fastpath-c207:\n\tbash ./scripts/stage-compaction-fastpath-c207.sh\n\n' >>"$makefile_staged"
printf 'commit-compaction-fastpath-c207:\n\tbash ./scripts/commit-compaction-fastpath-c207.sh\n\n' >>"$makefile_staged"
printf 'push-compaction-fastpath-c207:\n\tbash ./scripts/push-compaction-fastpath-c207.sh\n' >>"$makefile_staged"

cp -- "$makefile_staged" Makefile
git add -- Makefile "${feature_paths[@]}"
