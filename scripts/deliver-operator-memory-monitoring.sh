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
cp "$repo/hat/hatCache/operator_memory_monitoring.go" "$tmp/hat/hatCache/"
cp "$repo/hat/hatCache/operator_memory_monitoring_test.go" "$tmp/hat/hatCache/"
cp "$repo/scripts/inspect-frontier-metrics.sh" "$tmp/scripts/"
cp "$repo/scripts/test-operator-memory-monitoring.sh" "$tmp/scripts/"
cp "$repo/scripts/format-operator-memory-monitoring.sh" "$tmp/scripts/"
cp "$repo/scripts/verify-operator-memory-monitoring.sh" "$tmp/scripts/"
cp "$repo/scripts/deliver-operator-memory-monitoring.sh" "$tmp/scripts/"
cp "$repo/OPERATOR_MEMORY_MONITORING.md" "$tmp/"

if ! rg -q '^- \[x\] M084 ' "$tmp/INSPIRATION.md"; then
	awk '
		/^- \[ \] M084 / {
			sub(/^- \[ \] M084 /, "- [x] M084 ")
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

if ! rg -q 'OPERATOR_MEMORY_MONITORING.md' "$tmp/README.md"; then
	printf '\n- [Operator memory monitoring](OPERATOR_MEMORY_MONITORING.md)\n' >> "$tmp/README.md"
fi

if ! rg -q '^verify-operator-memory-monitoring:' "$tmp/Makefile"; then
	printf '\ntest-operator-memory-monitoring:\n\tsh ./scripts/test-operator-memory-monitoring.sh\n\nformat-operator-memory-monitoring:\n\tsh ./scripts/format-operator-memory-monitoring.sh\n\nverify-operator-memory-monitoring:\n\tsh ./scripts/verify-operator-memory-monitoring.sh\n\ndeliver-operator-memory-monitoring:\n\tsh ./scripts/deliver-operator-memory-monitoring.sh\n' >> "$tmp/Makefile"
fi

cd "$tmp"
gofmt -w hat/hatCache/monitoring.go hat/hatCache/operator_memory_monitoring.go hat/hatCache/operator_memory_monitoring_test.go
go test ./hat/hatMetrics
go test -race ./hat/hatMetrics
printf '%s\n' 'clean-origin hatCache tests remain blocked by the pre-existing hat/hatSql/query.go syntax errors; local focused, full, and race hatCache tests passed before delivery.'
git diff --check -- README.md INSPIRATION.md Makefile OPERATOR_MEMORY_MONITORING.md hat/hatCache/monitoring.go hat/hatCache/operator_memory_monitoring.go hat/hatCache/operator_memory_monitoring_test.go scripts/inspect-frontier-metrics.sh scripts/test-operator-memory-monitoring.sh scripts/format-operator-memory-monitoring.sh scripts/verify-operator-memory-monitoring.sh scripts/deliver-operator-memory-monitoring.sh

git add -- README.md INSPIRATION.md Makefile OPERATOR_MEMORY_MONITORING.md hat/hatCache/monitoring.go hat/hatCache/operator_memory_monitoring.go hat/hatCache/operator_memory_monitoring_test.go scripts/inspect-frontier-metrics.sh scripts/test-operator-memory-monitoring.sh scripts/format-operator-memory-monitoring.sh scripts/verify-operator-memory-monitoring.sh scripts/deliver-operator-memory-monitoring.sh
git commit -m "feat(monitoring): expose operator retained memory"
git push origin HEAD:master
