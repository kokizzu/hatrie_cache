#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run 'TestConnectionPoolEmits.*LifecycleEvent|TestConnectionPoolEmitsPhysicalLifecycleEvents' -count=1
