#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run 'TestConnectionPoolEmits.*LifecycleEvent|TestConnectionPoolEmitsPhysicalLifecycleEvents' -count=1
