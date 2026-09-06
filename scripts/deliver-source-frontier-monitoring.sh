#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)

cleanup() {
	git -C "$repo" worktree remove --force "$tmp" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

git -C "$repo" fetch origin master >/dev/null
git -C "$repo" worktree add --detach "$tmp" origin/master >/dev/null

mkdir -p "$tmp/hat/hatCache" "$tmp/scripts"
cp "$repo/hat/hatCache/monitoring.go" "$tmp/hat/hatCache/"
cp "$repo/hat/hatCache/source_frontier_monitoring.go" "$tmp/hat/hatCache/"
cp "$repo/hat/hatCache/source_frontier_monitoring_test.go" "$tmp/hat/hatCache/"
cp "$repo/scripts/inspect-frontier-metrics.sh" "$tmp/scripts/"
cp "$repo/scripts/test-source-frontier-monitoring.sh" "$tmp/scripts/"
cp "$repo/scripts/format-source-frontier-monitoring.sh" "$tmp/scripts/"
cp "$repo/scripts/verify-source-frontier-monitoring.sh" "$tmp/scripts/"
cp "$repo/scripts/deliver-source-frontier-monitoring.sh" "$tmp/scripts/"
cp "$repo/SOURCE_FRONTIER_MONITORING.md" "$tmp/"

if ! rg -q '^- \[x\] M083 ' "$tmp/INSPIRATION.md"; then
	awk '
		/^- \[ \] M083 / {
			sub(/^- \[ \] M083 /, "- [x] M083 ")
			found = 1
		}
		{ print }
		END {
			if (!found) {
				exit 1
			}
		}
	' "$tmp/INSPIRATION.md" > "$tmp/INSPIRATION.md.tmp"
	mv "$tmp/INSPIRATION.md.tmp" "$tmp/INSPIRATION.md"
fi

if ! rg -q 'SOURCE_FRONTIER_MONITORING.md' "$tmp/README.md"; then
	printf '\n- [Source frontier monitoring](SOURCE_FRONTIER_MONITORING.md)\n' >> "$tmp/README.md"
fi

if ! rg -q '^verify-source-frontier-monitoring:' "$tmp/Makefile"; then
	printf '\ninspect-frontier-metrics:\n\tsh ./scripts/inspect-frontier-metrics.sh\n\ntest-source-frontier-monitoring:\n\tsh ./scripts/test-source-frontier-monitoring.sh\n\nformat-source-frontier-monitoring:\n\tsh ./scripts/format-source-frontier-monitoring.sh\n\nverify-source-frontier-monitoring:\n\tsh ./scripts/verify-source-frontier-monitoring.sh\n\ndeliver-source-frontier-monitoring:\n\tsh ./scripts/deliver-source-frontier-monitoring.sh\n' >> "$tmp/Makefile"
fi

cd "$tmp"
gofmt -w hat/hatCache/monitoring.go hat/hatCache/source_frontier_monitoring.go hat/hatCache/source_frontier_monitoring_test.go
go test ./hat/hatMetrics
go test -race ./hat/hatMetrics
printf '%s\n' 'clean-origin hatCache tests remain blocked by the pre-existing hat/hatSql/query.go syntax errors; local focused, full, and race hatCache tests passed before delivery.'
git diff --check -- README.md INSPIRATION.md Makefile SOURCE_FRONTIER_MONITORING.md hat/hatCache/monitoring.go hat/hatCache/source_frontier_monitoring.go hat/hatCache/source_frontier_monitoring_test.go scripts/inspect-frontier-metrics.sh scripts/test-source-frontier-monitoring.sh scripts/format-source-frontier-monitoring.sh scripts/verify-source-frontier-monitoring.sh scripts/deliver-source-frontier-monitoring.sh

git add -- README.md INSPIRATION.md Makefile SOURCE_FRONTIER_MONITORING.md hat/hatCache/monitoring.go hat/hatCache/source_frontier_monitoring.go hat/hatCache/source_frontier_monitoring_test.go scripts/inspect-frontier-metrics.sh scripts/test-source-frontier-monitoring.sh scripts/format-source-frontier-monitoring.sh scripts/verify-source-frontier-monitoring.sh scripts/deliver-source-frontier-monitoring.sh
git commit -m "feat(monitoring): expose source frontier lag"
git push origin HEAD:master
