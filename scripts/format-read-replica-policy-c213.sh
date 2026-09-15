#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/read_replica_policy.go hat/hatReplication/read_consistency.go hat/hatReplication/read_replica_selection_fastpath_test.go
