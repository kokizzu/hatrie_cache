#!/usr/bin/env bash
set -euo pipefail

expected=$(cat <<'EOF'
BENCHMARK.md
COMPACT_PEER_LISTENER.md
INSPIRATION_BACKLOG.md
Makefile
README.md
TR044_COMPACT_PEER_COMPRESSION.md
TR051_COMPACT_PEER_CAPABILITY_NEGOTIATION.md
hat/hatPeer/compact_listener.go
hat/hatPeer/compact_listener_benchmark_test.go
hat/hatPeer/compact_listener_test.go
hat/hatPeer/compact_session.go
scripts/benchmark-tr051-c203.sh
scripts/commit-tr051-c203.sh
scripts/format-tr051-c203.sh
scripts/inspect-staged-tr051-c203.sh
scripts/push-tr051-c203.sh
scripts/stage-tr051-c203.sh
scripts/test-tr051-c203.sh
scripts/verify-tr051-c203.sh
EOF
)
actual=$(git diff --cached --name-only)
if [[ "$actual" != "$expected" ]]; then
	echo "unexpected staged paths" >&2
	echo "$actual" >&2
	exit 1
fi
git diff --cached --check
git commit -m "Negotiate compact peer compression capability"
