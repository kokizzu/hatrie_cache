#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^(TestStreamTransactionRecovery|TestCompactPeerStream)' -count=1
go test -race ./hat/hatPeer -run '^(TestStreamTransactionRecovery|TestCompactPeerStream)' -count=1
go vet ./hat/hatPeer
bash scripts/benchmark-tu29.sh
