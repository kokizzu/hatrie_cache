#!/usr/bin/env bash
set -euo pipefail

expected=$(cat <<'EOF'
BENCHMARK.md
COMPACT_PEER_SESSION.md
INSPIRATION_BACKLOG.md
Makefile
README.md
TR043B_COMPACT_PEER_WRITE_BUFFER.md
hat/hatPeer/compact_protocol.go
hat/hatPeer/compact_protocol_benchmark_test.go
hat/hatPeer/compact_protocol_test.go
hat/hatPeer/compact_session.go
scripts/benchmark-tr043b-c203.sh
scripts/commit-tr043b-c203.sh
scripts/format-tr043b-c203.sh
scripts/inspect-staged-tr043b-c203.sh
scripts/push-tr043b-c203.sh
scripts/stage-tr043b-c203.sh
scripts/test-tr043b-c203.sh
scripts/verify-tr043b-c203.sh
EOF
)
actual=$(git diff --cached --name-only)
if [[ "$actual" != "$expected" ]]; then
	echo "unexpected staged paths" >&2
	echo "$actual" >&2
	exit 1
fi
git diff --cached --check
git commit -m "Reuse compact peer frame write buffers"
