#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/connection_pool_lifecycle.go hat/hatPeer/connection_pool_cancellation_test.go
