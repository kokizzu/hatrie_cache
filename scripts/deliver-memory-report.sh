#!/bin/sh
set -eu

feature_files='ADOPTED_QUERY_ENGINE_IDEAS.md
BENCHMARK.md
MEMORY_REPORT.md
README.md
hat/hatCache/monitoring.go
hat/hatCache/monitoring_memory_test.go
hat/hatMonitoring/memory.go
hat/hatMonitoring/memory_test.go
scripts/benchmark-memory-report.sh
scripts/deliver-memory-report.sh
scripts/format-memory-report.sh
scripts/test-memory-report.sh'

is_allowed() {
	case "$1" in
		Makefile|api.go|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|MEMORY_REPORT.md|README.md|hat/hatCache/monitoring.go|hat/hatCache/monitoring_memory_test.go|hat/hatMonitoring/memory.go|hat/hatMonitoring/memory_test.go|scripts/benchmark-memory-report.sh|scripts/deliver-memory-report.sh|scripts/format-memory-report.sh|scripts/test-memory-report.sh)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

check_staged() {
	staged=$(git diff --cached --name-only)
	for path in $staged; do
		if ! is_allowed "$path"; then
			printf 'refusing memory report delivery with unrelated staged path: %s\n' "$path" >&2
			exit 1
		fi
	done
}

stage_append() {
	file=$1
	text=$2
	base=$(mktemp)
	desired=$(mktemp)
	staged=$(mktemp)
	patch=$(mktemp)
	git show "HEAD:$file" > "$base"
	cp "$base" "$desired"
	printf '%b' "$text" >> "$desired"
	if git show ":$file" > "$staged" 2>/dev/null; then
		if cmp -s "$staged" "$desired"; then
			rm -f "$base" "$desired" "$staged" "$patch"
			return 0
		fi
		if ! cmp -s "$staged" "$base"; then
			printf 'refusing to replace unexpected staged content in %s\n' "$file" >&2
			rm -f "$base" "$desired" "$staged" "$patch"
			exit 1
		fi
	fi
	diff_status=0
	diff -u --label "a/$file" --label "b/$file" "$base" "$desired" > "$patch" || diff_status=$?
	if [ "$diff_status" -ne 0 ] && [ "$diff_status" -ne 1 ]; then
		rm -f "$base" "$desired" "$staged" "$patch"
		exit "$diff_status"
	fi
	if [ "$diff_status" -eq 1 ]; then
		git apply --cached --recount "$patch"
	fi
	rm -f "$base" "$desired" "$staged" "$patch"
}

stage_feature() {
	git diff --check
	check_staged
	for path in $feature_files; do
		git add -- "$path"
	done
	stage_append Makefile '\n\nformat-memory-report:\n\tbash scripts/format-memory-report.sh\n\ntest-memory-report:\n\tbash scripts/test-memory-report.sh\n\nbenchmark-memory-report:\n\tbash scripts/benchmark-memory-report.sh\n\ndeliver-memory-report:\n\tbash scripts/deliver-memory-report.sh apply\n\ncheck-memory-report-stage:\n\tbash scripts/deliver-memory-report.sh check\n\ncommit-memory-report:\n\tbash scripts/deliver-memory-report.sh commit\n\npush-memory-report:\n\tbash scripts/deliver-memory-report.sh push\n'
	stage_append api.go '\n\ntype MonitoringMemoryReport = core.MonitoringMemoryReport\n\nvar ReadMonitoringMemoryReport = core.ReadMonitoringMemoryReport\n'
	git diff --cached --check
	check_staged
}

case "${1:-check}" in
	apply)
		stage_feature
		;;
	check)
		git diff --check
		check_staged
		git diff --cached --check
		;;
	commit)
		check_staged
		git diff --cached --check
		git commit -m 'feat(ops): add on-demand memory report'
		;;
	push)
		check_staged
		git push
		;;
	*)
		printf 'usage: %s {apply|check|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
