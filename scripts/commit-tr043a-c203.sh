#!/usr/bin/env bash
set -euo pipefail

expected=$(cat <<'EOF'
BENCHMARK.md
COMPACT_PEER_SESSION.md
INSPIRATION_BACKLOG.md
Makefile
README.md
TR043A_COMPACT_PEER_PREPARED_CALL.md
hat/hatPeer/compact_session.go
hat/hatPeer/compact_session_benchmark_test.go
hat/hatPeer/compact_session_test.go
scripts/benchmark-tr043a-c203.sh
scripts/commit-tr043a-c203.sh
scripts/format-tr043a-c203.sh
scripts/inspect-staged-tr043a-c203.sh
scripts/push-tr043a-c203.sh
scripts/stage-tr043a-c203.sh
scripts/test-tr043a-c203.sh
scripts/verify-tr043a-c203.sh
EOF
)
actual=$(git diff --cached --name-only)
if [[ "$actual" != "$expected" ]]; then
	echo "unexpected staged paths" >&2
	echo "$actual" >&2
	exit 1
fi
git diff --cached --check
git commit -m "Use prepared templates for compact peer calls"
