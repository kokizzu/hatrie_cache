#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-tu28-lifecycle.sh
go test ./hat/hatPeer -run 'TestConnectionPool|TestPeerLifecycle' -count=1 -timeout=30s
go test -race ./hat/hatPeer -run 'TestConnectionPoolEmits.*LifecycleEvent|TestConnectionPoolEmitsPhysicalLifecycleEvents' -count=1
go vet ./hat/hatPeer
git diff --check
