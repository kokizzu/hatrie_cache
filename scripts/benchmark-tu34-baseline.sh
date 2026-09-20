#!/usr/bin/env bash
set -euo pipefail

worktree=/tmp/hatrie-cache-tu34-baseline
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" origin/master
cp hat/hatCache/tu34_space_sync_policy_common_benchmark_test.go "$worktree/hat/hatCache/"
cat > "$worktree/hat/hatSql/tu34_compile_baseline.go" <<'EOF'
package hatSql

const MaxDataflowTextBytes = 1 << 20

const (
	TypedTableDate      TypedTableKind = 5
	TypedTableTimestamp TypedTableKind = 6
)
EOF
(
	cd "$worktree"
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU34CommandJournal$' -benchmem -count=5
)
