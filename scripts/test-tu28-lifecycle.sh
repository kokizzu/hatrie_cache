#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^(TestConnectionPool|TestPeerLifecycleRegistry|TestCompactPeerSessionEmitsLifecycle|TestTU28)' -count=1
