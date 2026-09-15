#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/connection_pool.go hat/hatReplication/connection_pool_idle_fastpath_test.go
