#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPeer/connection_pool.go hat/hatPeer/connection_pool_test.go
